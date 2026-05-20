"""
Migration listener — detects pump.fun bonding curves that just graduated to PumpSwap AMM.

Different strategy from snipe and patient: instead of buying fresh launches or hoping
for mature survivors, we wait for tokens to FULLY GRADUATE. A token that completes its
bonding curve has:
  - Locked $80K+ of liquidity (PumpSwap migration threshold)
  - A community of buyers who held through the curve
  - Migrated to an AMM with predictable price-impact math
  - Survived the launch dump cycle by definition

This listener:
  1. Subscribes to logs from the migration wrapper program
     (39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg)
  2. For each successful "Migrate" instruction, parses the Program data to extract
     the migrated token's mint + PumpSwap pool address + initial liquidity
  3. Builds a TokenInfo with the PumpSwap pool details in additional_data so the
     trader can route through Jupiter (or a future PumpSwap-specific trader)
  4. Fires the token_callback so the trader picks it up via the normal pipeline

Pump.fun migration wrapper program emits one event per Migrate instruction with
the fields parsed in MIGRATION_EVENT_LAYOUT below. The on-chain structure is
documented in learning-examples/listen-migrations/listen_logsubscribe.py.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import struct
from collections.abc import Awaitable, Callable
from time import monotonic
from typing import Any

import base58
import websockets
from solders.pubkey import Pubkey

from interfaces.core import Platform, TokenInfo
from monitoring.base_listener import BaseTokenListener
from utils.logger import get_logger

logger = get_logger(__name__)

# Pump.fun's migration wrapper program. Emits a MigrationEvent per Migrate
# instruction. Distinct from the main pump.fun program (which only emits
# CompleteEvent when the curve fills — the migration itself runs separately).
MIGRATION_PROGRAM_ID = "39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg"

# Anchor discriminator prefix at byte 0..8 — we just skip past it.
EVENT_DISCRIMINATOR_BYTES = 8

# Event layout (after the 8-byte discriminator). Each tuple is (field_name, type).
# Sourced from learning-examples/listen-migrations/listen_logsubscribe.py — verified
# parsing works against on-chain data.
MIGRATION_EVENT_LAYOUT: list[tuple[str, str]] = [
    ("timestamp", "i64"),
    ("index", "u16"),
    ("creator", "publicKey"),
    ("baseMint", "publicKey"),
    ("quoteMint", "publicKey"),
    ("baseMintDecimals", "u8"),
    ("quoteMintDecimals", "u8"),
    ("baseAmountIn", "u64"),
    ("quoteAmountIn", "u64"),
    ("poolBaseAmount", "u64"),
    ("poolQuoteAmount", "u64"),
    ("minimumLiquidity", "u64"),
    ("initialLiquidity", "u64"),
    ("lpTokenAmountOut", "u64"),
    ("poolBump", "u8"),
    ("pool", "publicKey"),
    ("lpMint", "publicKey"),
    ("userBaseTokenAccount", "publicKey"),
    ("userQuoteTokenAccount", "publicKey"),
]


def _parse_migration_event(data: bytes) -> dict[str, Any] | None:
    """Parse the binary event payload after the 8-byte discriminator.

    Returns None on any parse failure — caller treats None as "skip this tx".
    """
    if len(data) < EVENT_DISCRIMINATOR_BYTES:
        return None

    offset = EVENT_DISCRIMINATOR_BYTES
    out: dict[str, Any] = {}

    try:
        for name, kind in MIGRATION_EVENT_LAYOUT:
            if kind == "publicKey":
                out[name] = base58.b58encode(data[offset : offset + 32]).decode()
                offset += 32
            elif kind == "u64":
                out[name] = struct.unpack("<Q", data[offset : offset + 8])[0]
                offset += 8
            elif kind == "i64":
                out[name] = struct.unpack("<q", data[offset : offset + 8])[0]
                offset += 8
            elif kind == "u16":
                out[name] = struct.unpack("<H", data[offset : offset + 2])[0]
                offset += 2
            elif kind == "u8":
                out[name] = data[offset]
                offset += 1
            else:
                logger.warning("Unknown field type %s", kind)
                return None
        return out
    except (struct.error, IndexError):
        logger.exception("Failed to parse migration event payload (offset=%d)", offset)
        return None


def _is_successful_migration(logs: list[str]) -> bool:
    """Inspect the log array to decide if this is a successful, non-redundant migration."""
    if not logs:
        return False
    for log in logs:
        if "AnchorError thrown" in log or "Error" in log:
            return False
    # Must include a Migrate instruction
    if not any("Program log: Instruction: Migrate" in log for log in logs):
        return False
    # Skip already-migrated cases (the program logs this when it's a no-op)
    if any("Program log: Bonding curve already migrated" in log for log in logs):
        return False
    return True


class UniversalMigrationListener(BaseTokenListener):
    """Listens for pump.fun → PumpSwap migrations and dispatches the new pool."""

    def __init__(
        self,
        wss_endpoint: str,
        platforms: list[Platform] | None = None,
    ):
        super().__init__()
        self.wss_endpoint = wss_endpoint
        self.platforms = platforms or [Platform.PUMP_FUN]
        # We track recently-dispatched mints to dedupe — sometimes the same
        # migration event appears under multiple sigs (logs/blocks overlap).
        self._recent_mints: dict[str, float] = {}
        self._dedup_window_seconds = 60.0

    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        """Open the WSS connection and dispatch matured graduations as TokenInfo."""
        while True:
            try:
                logger.info("Connecting to WSS for migration listener…")
                async with websockets.connect(
                    self.wss_endpoint, ping_interval=20
                ) as ws:
                    sub = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [MIGRATION_PROGRAM_ID]},
                            {"commitment": "processed"},
                        ],
                    }
                    await ws.send(json.dumps(sub))
                    ack = await ws.recv()
                    logger.info("Migration subscription confirmed: %s", ack[:120])

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=90)
                        except asyncio.TimeoutError:
                            logger.info("Migration WSS quiet for 90s — keeping connection")
                            continue

                        msg = json.loads(raw)
                        if msg.get("method") != "logsNotification":
                            continue

                        log_data = msg["params"]["result"]["value"]
                        logs = log_data.get("logs", [])
                        if not _is_successful_migration(logs):
                            continue

                        # Find the first Program data line — that's the migration event
                        parsed = None
                        for log in logs:
                            if log.startswith("Program data:"):
                                try:
                                    raw_b64 = log.split(": ", 1)[1]
                                    data = base64.b64decode(raw_b64)
                                    parsed = _parse_migration_event(data)
                                    if parsed:
                                        break
                                except Exception:  # noqa: BLE001
                                    # base64/struct parsing can fail on truncated payloads
                                    continue

                        if not parsed:
                            logger.debug("Migration log had no parsable Program data")
                            continue

                        await self._dispatch(parsed, token_callback, log_data)

            except Exception as e:  # noqa: BLE001
                logger.exception("Migration listener crashed; reconnecting in 5s: %s", e)
                await asyncio.sleep(5)

    async def _dispatch(
        self,
        parsed: dict[str, Any],
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        log_data: dict[str, Any],
    ) -> None:
        """Build a TokenInfo and hand it to the trader."""
        base_mint_str = parsed["baseMint"]
        # Dedup
        now = monotonic()
        # Evict old dedup entries
        self._recent_mints = {
            k: v for k, v in self._recent_mints.items() if now - v < self._dedup_window_seconds
        }
        if base_mint_str in self._recent_mints:
            logger.debug("Migration dedup: skipping %s (seen %.1fs ago)",
                         base_mint_str[:12], now - self._recent_mints[base_mint_str])
            return
        self._recent_mints[base_mint_str] = now

        try:
            base_mint = Pubkey.from_string(base_mint_str)
            pool = Pubkey.from_string(parsed["pool"])
            creator = Pubkey.from_string(parsed["creator"])
        except ValueError:
            logger.exception("Migration event has invalid pubkey strings")
            return

        # We don't have name/symbol/uri from the migration event itself —
        # those came from the original CreateEvent which fired earlier. Use
        # the mint string as the symbol for now (trader doesn't depend on
        # symbol for trading logic, only for logging).
        token_info = TokenInfo(
            name=f"migrated-{base_mint_str[:8]}",
            symbol=base_mint_str[:8],
            uri="",
            mint=base_mint,
            platform=Platform.PUMP_FUN,
            creator=creator,
            creation_timestamp=float(parsed["timestamp"]),  # actual on-chain timestamp
            # Stash PumpSwap pool details in additional_data — the trader
            # checks this to route via PumpSwap/Jupiter instead of bonding curve
            additional_data={
                "migration": True,
                "pumpswap_pool": str(pool),
                "lp_mint": parsed["lpMint"],
                "quote_mint": parsed["quoteMint"],
                "initial_pool_base": parsed["poolBaseAmount"],
                "initial_pool_quote": parsed["poolQuoteAmount"],
                "migration_signature": log_data.get("signature"),
            },
        )
        logger.info(
            "Migration detected: mint=%s pool=%s base=%d quote=%d initial_liq_lamports=%d",
            base_mint_str[:12],
            parsed["pool"][:12],
            parsed["poolBaseAmount"],
            parsed["poolQuoteAmount"],
            parsed["initialLiquidity"],
        )

        try:
            await token_callback(token_info)
        except Exception:  # noqa: BLE001
            logger.exception("Migration token_callback raised for %s", base_mint_str[:12])
