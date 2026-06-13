"""
PumpSwap new-pool listener — catches tokens at the moment of graduation,
before they appear on Dexscreener trending.

This is the strategic pivot from "Dexscreener trending follower" (lagging
indicator: tokens that already pumped) to "PumpSwap fresh-pool sniper"
(leading indicator: tokens entering an AMM for the first time).

The pool account creation is the canonical "I just graduated and have
~$80K of locked liquidity available" signal. By subscribing to the
PumpSwap program's account stream filtered for newly-created 245-byte
Pool accounts, we see new pools the same slot they're created — usually
30-60 seconds before Dexscreener ingests them.

We deliberately wait `min_post_creation_seconds` after detection before
emitting, so the bot doesn't snipe at t=0 (which is where MEV bots and
graduation-jitter dominate). Sweet spot is usually 30s-3min after
creation — initial volatility settles but real trader flow hasn't dried up.
"""

from __future__ import annotations

import asyncio
import base64
import json
import struct
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aiohttp
import base58
import websockets
from solders.pubkey import Pubkey

from interfaces.core import Platform, TokenInfo
from monitoring.base_listener import BaseTokenListener
from utils.logger import get_logger

logger = get_logger(__name__)

# Constants copied from learning-examples/listen-migrations/listen_programsubscribe.py
# verified against on-chain Pool account layout in pump_swap_idl.json
PUMP_AMM_PROGRAM_ID = Pubkey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
WSOL_MINT = Pubkey.from_string("So11111111111111111111111111111111111111112")
MARKET_ACCOUNT_LENGTH = 245
# Pool account discriminator from IDL: [241, 154, 109, 4, 17, 177, 109, 188]
MARKET_DISCRIMINATOR_B58 = base58.b58encode(b"\xf1\x9am\x04\x11\xb1m\xbc").decode()
QUOTE_MINT_SOL_B58 = base58.b58encode(bytes(WSOL_MINT)).decode()

# Pool offsets (after 8-byte discriminator + 1 pool_bump + 2 index = 11 bytes)
OFFSET_CREATOR = 11
OFFSET_BASE_MINT = 43
OFFSET_QUOTE_MINT = 75
OFFSET_LP_MINT = 107
OFFSET_POOL_BASE_TA = 139
OFFSET_POOL_QUOTE_TA = 171
OFFSET_LP_SUPPLY = 203
OFFSET_COIN_CREATOR = 211


@dataclass
class NewPoolFilters:
    """All thresholds are inclusive."""

    # WARMUP: ignore all WSS events for the first N seconds after startup —
    # we use this to populate the "known pubkeys" set without emitting. After
    # warmup any new pubkey is genuinely fresh.
    warmup_seconds: int = 90

    # Wait N seconds after detection before emitting — gives MEV/jitter
    # time to settle while still being well before Dexscreener ingestion.
    min_post_creation_seconds: int = 30
    max_post_creation_seconds: int = 600  # 10 min: skip if we got behind

    # Reject user-created pools (creator on-curve = wallet, not PDA).
    # PumpSwap graduations are PDA-created, which is what we want.
    skip_user_created_pools: bool = True

    # Minimum quote (SOL) reserve at evaluation time. Pump.fun graduations
    # land with ~80 SOL in the quote vault. Anything below this means liq
    # was drained immediately after migration (= rug warning).
    min_quote_reserve_sol: float = 30.0


def _parse_pool_account(data: bytes) -> dict | None:
    """Parse the 245-byte Pool account into named fields. None on failure."""
    if len(data) < MARKET_ACCOUNT_LENGTH:
        return None
    try:
        return {
            "pool_bump": data[8],
            "index": struct.unpack("<H", data[9:11])[0],
            "creator": base58.b58encode(data[OFFSET_CREATOR:OFFSET_CREATOR + 32]).decode(),
            "base_mint": base58.b58encode(data[OFFSET_BASE_MINT:OFFSET_BASE_MINT + 32]).decode(),
            "quote_mint": base58.b58encode(data[OFFSET_QUOTE_MINT:OFFSET_QUOTE_MINT + 32]).decode(),
            "lp_mint": base58.b58encode(data[OFFSET_LP_MINT:OFFSET_LP_MINT + 32]).decode(),
            "pool_base_token_account": base58.b58encode(data[OFFSET_POOL_BASE_TA:OFFSET_POOL_BASE_TA + 32]).decode(),
            "pool_quote_token_account": base58.b58encode(data[OFFSET_POOL_QUOTE_TA:OFFSET_POOL_QUOTE_TA + 32]).decode(),
            "lp_supply": struct.unpack("<Q", data[OFFSET_LP_SUPPLY:OFFSET_LP_SUPPLY + 8])[0],
            "coin_creator": base58.b58encode(data[OFFSET_COIN_CREATOR:OFFSET_COIN_CREATOR + 32]).decode(),
            "is_mayhem_mode": bool(data[243]) if len(data) > 243 else False,
            "is_cashback_coin": bool(data[244]) if len(data) > 244 else False,
        }
    except Exception:  # noqa: BLE001
        logger.exception("Failed to parse pool account")
        return None


class PumpSwapNewPoolListener(BaseTokenListener):
    """Detects newly-created PumpSwap pools (= fresh pump.fun graduations)
    and emits them as TokenInfo for the trader.

    Lifecycle:
      1. Fetch all existing pool pubkeys via getProgramAccounts (cold start)
      2. Open WSS programSubscribe with dataSize + discriminator + WSOL-quote filters
      3. For each notification, check if pubkey is new; if so, queue with
         creation timestamp.
      4. After `min_post_creation_seconds`, emit TokenInfo to callback.
    """

    def __init__(
        self,
        wss_endpoint: str,
        rpc_endpoint: str,
        platforms: list[Platform] | None = None,
        filters: NewPoolFilters | None = None,
    ):
        super().__init__()
        self.wss_endpoint = wss_endpoint
        self.rpc_endpoint = rpc_endpoint
        self.platforms = platforms or [Platform.PUMP_FUN]
        self.filters = filters or NewPoolFilters()
        self._known_pubkeys: set[str] = set()
        self._warmup_until: float = 0.0  # set at startup
        # Pubkey of pending pool -> (created_ts, parsed_data)
        self._pending: dict[str, tuple[float, dict]] = {}

    # NOTE: getProgramAccounts isn't usable for PumpSwap cold-start dedup —
    # 5M+ existing pools, Helius hard-caps the response. Instead we use a
    # WARMUP PERIOD: for the first N seconds of WSS streaming, we add every
    # pubkey we see to the known set without emitting. After warmup, anything
    # new is genuinely a freshly-created pool.

    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        self._warmup_until = time.monotonic() + self.filters.warmup_seconds
        logger.info(
            "PumpSwap new-pool listener starting (warmup=%ds, post_create wait=%ds, "
            "min_quote_sol=%.1f)",
            self.filters.warmup_seconds,
            self.filters.min_post_creation_seconds,
            self.filters.min_quote_reserve_sol,
        )

        # Launch the pending-emitter task in parallel
        emit_task = asyncio.create_task(
            self._pending_emitter(token_callback, match_string, creator_address)
        )

        # Main WSS subscription loop with reconnect
        while True:
            try:
                logger.info("Connecting to PumpSwap programSubscribe…")
                async with websockets.connect(
                    self.wss_endpoint, ping_interval=20
                ) as ws:
                    sub = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "programSubscribe",
                        "params": [
                            str(PUMP_AMM_PROGRAM_ID),
                            {
                                "commitment": "processed",
                                "encoding": "base64",
                                "filters": [
                                    {"dataSize": MARKET_ACCOUNT_LENGTH},
                                    {"memcmp": {"offset": 0, "bytes": MARKET_DISCRIMINATOR_B58}},
                                    {"memcmp": {"offset": 75, "bytes": QUOTE_MINT_SOL_B58}},
                                ],
                            },
                        ],
                    }
                    await ws.send(json.dumps(sub))
                    ack = await ws.recv()
                    logger.info("Subscribed to PumpSwap programSubscribe: %s", ack[:120])

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=90)
                        except asyncio.TimeoutError:
                            logger.debug("WSS quiet for 90s — keeping connection")
                            continue
                        msg = json.loads(raw)
                        if msg.get("method") != "programNotification":
                            continue
                        await self._handle_notification(msg)
            except Exception:  # noqa: BLE001
                logger.exception("PumpSwap new-pool listener crashed; reconnecting in 5s")
                await asyncio.sleep(5)

    async def _handle_notification(self, msg: dict) -> None:
        """Process one programNotification — queue if new, ignore if known."""
        try:
            value = msg["params"]["result"]["value"]
            pubkey = value["pubkey"]
            raw_b64 = value["account"].get("data", [None])[0]
        except (KeyError, IndexError, TypeError):
            return

        # Already seen — skip silently (this also covers all warmup-period adds)
        if pubkey in self._known_pubkeys:
            return

        now = time.monotonic()
        in_warmup = now < self._warmup_until

        # During warmup, just add to known and don't emit
        if in_warmup:
            self._known_pubkeys.add(pubkey)
            return

        if not raw_b64:
            return

        try:
            account_data = base64.b64decode(raw_b64)
        except Exception:  # noqa: BLE001
            return

        parsed = _parse_pool_account(account_data)
        if not parsed:
            return

        # Skip user-created pools (regular wallet creator = not a PDA)
        if self.filters.skip_user_created_pools:
            try:
                creator_pk = Pubkey.from_string(parsed["creator"])
                if creator_pk.is_on_curve():
                    self._known_pubkeys.add(pubkey)
                    return
            except Exception:  # noqa: BLE001
                self._known_pubkeys.add(pubkey)
                return

        # Queue with current timestamp
        self._known_pubkeys.add(pubkey)
        self._pending[pubkey] = (now, parsed)
        logger.info(
            "Queued new PumpSwap pool: %s (mint=%s coin_creator=%s) — "
            "will emit in %ds",
            pubkey[:12], parsed["base_mint"][:12], parsed["coin_creator"][:12],
            self.filters.min_post_creation_seconds,
        )

    async def _pending_emitter(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None,
        creator_address: str | None,
    ) -> None:
        """Background task: drain pending pools after their wait period."""
        while True:
            now = time.monotonic()
            ready = [
                (pk, p) for pk, (ts, p) in list(self._pending.items())
                if now - ts >= self.filters.min_post_creation_seconds
            ]
            for pubkey, parsed in ready:
                ts, _ = self._pending.pop(pubkey, (0, None))
                age = now - ts
                if age > self.filters.max_post_creation_seconds:
                    logger.debug("Skipping stale pool %s (age=%.0fs)", pubkey[:12], age)
                    continue
                await self._emit(pubkey, parsed, token_callback, match_string, creator_address)
            await asyncio.sleep(2)

    async def _emit(
        self,
        pool_pubkey: str,
        parsed: dict,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None,
        creator_address: str | None,
    ) -> None:
        """Build a TokenInfo and hand to the trader."""
        try:
            base_mint = Pubkey.from_string(parsed["base_mint"])
            coin_creator = Pubkey.from_string(parsed["coin_creator"])
        except ValueError:
            logger.warning("Invalid pubkey in parsed pool %s", pool_pubkey[:12])
            return

        # Check current quote reserve — reject if too low (rug indicator)
        try:
            async with aiohttp.ClientSession() as sess:
                body = {
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getTokenAccountBalance",
                    "params": [parsed["pool_quote_token_account"]],
                }
                async with sess.post(
                    self.rpc_endpoint, json=body,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    bal_data = await resp.json()
            ui = bal_data.get("result", {}).get("value", {}).get("uiAmount") or 0
        except Exception:  # noqa: BLE001
            logger.warning("Failed to fetch quote balance for %s", pool_pubkey[:12])
            return

        if ui < self.filters.min_quote_reserve_sol:
            logger.info(
                "Skip %s: quote_reserve=%.2f SOL < min %.1f (drained/rug)",
                pool_pubkey[:12], ui, self.filters.min_quote_reserve_sol,
            )
            return

        if creator_address and parsed["coin_creator"] != creator_address:
            return

        # Approximate USD liquidity: 2× quote_reserve_SOL × SOL price.
        # Used by safety_filters.filter_liquidity_floor which gates AMM tokens
        # on `additional_data["liquidity_usd"]`. Using a conservative $150/SOL.
        approx_liq_usd = ui * 2 * 150

        token_info = TokenInfo(
            name=f"newpool-{parsed['base_mint'][:8]}",
            symbol=parsed["base_mint"][:8],
            uri="",
            mint=base_mint,
            platform=Platform.PUMP_FUN,
            creator=coin_creator,
            creation_timestamp=time.time(),  # approximate — actual is some slot ago
            additional_data={
                "source": "pumpswap_new_pool",
                "dex_id": "pumpswap",
                "pumpswap_pool": pool_pubkey,
                "pair_address": pool_pubkey,
                "lp_mint": parsed["lp_mint"],
                "coin_creator": parsed["coin_creator"],
                "is_mayhem_mode": parsed["is_mayhem_mode"],
                "is_cashback_coin": parsed["is_cashback_coin"],
                "quote_reserve_sol_at_eval": ui,
                # Bridge to safety_filters' AMM-token checks
                "liquidity_usd": approx_liq_usd,
                "market_cap": 0,  # unknown at hour 0; safety filter accepts 0
            },
        )

        if match_string and match_string.lower() not in token_info.symbol.lower():
            return

        logger.info(
            "NEW POOL EMIT: %s pool=%s quote_reserve=%.2f SOL",
            token_info.symbol, pool_pubkey[:12], ui,
        )
        try:
            await token_callback(token_info)
        except Exception:  # noqa: BLE001
            logger.exception("token_callback raised for %s", pool_pubkey[:12])
