"""
Direct PumpSwap AMM trader — builds buy/sell instructions from the IDL
without going through Jupiter.

Why bypass Jupiter? Jupiter's lite-api PumpSwap adapter has gaps with
freshly-migrated tokens (ConstraintOwner 2004 on uninitialized per-coin
volume accumulators). Phantom and Photon don't use Jupiter for PumpSwap
pools — they talk to the pump-amm program directly. We do the same.

Account layout sourced from the pump_swap_idl.json `buy` and `sell`
instructions (23 / 21 accounts) plus 2 remaining_accounts observed on every
real PumpSwap buy/sell tx on chain (creator volume accumulator + a fee
program account). Total: 25 accounts for buy, 23 for sell.

This trader supports both wrap-on-the-fly (SOL → WSOL ATA temp account) and
existing WSOL accounts. For pump.fun graduates the trade pair is always
{base_mint: meme_token, quote_mint: WSOL}.
"""

from __future__ import annotations

import asyncio
import base64
import json
import struct
from dataclasses import dataclass
from pathlib import Path

import base58
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.system_program import ID as SYSTEM_PROGRAM_ID
from solders.transaction import VersionedTransaction
from spl.token.constants import (
    ASSOCIATED_TOKEN_PROGRAM_ID,
    TOKEN_2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
)

from core.client import SolanaClient
from core.wallet import Wallet
from interfaces.core import Platform, TokenInfo
from trading.base import TradeResult
from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# PumpSwap on-chain constants (sourced from real successful PumpSwap txs)
# ---------------------------------------------------------------------------
PUMPSWAP_PROGRAM_ID = Pubkey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
GLOBAL_CONFIG = Pubkey.from_string("ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw")
PROTOCOL_FEE_RECIPIENT = Pubkey.from_string(
    "JCRGumoE9Qi5BBgULTgdgTLjSgkCMSbF62ZZfGs84JeU"
)
FEE_CONFIG = Pubkey.from_string("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx")
FEE_PROGRAM = Pubkey.from_string("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
# Note: PumpSwap's buy/sell also include "remaining_accounts" past the IDL's
# 23 standard accounts. These are pool-specific fee_recipient ATAs managed by
# fee_program (pfeeUxB6...). Number varies (typically 2 or 3, sometimes more
# for Token-2022 base mints). They're NOT derivable from public constants —
# we discover them at runtime via discover_fee_remaining_accounts() by scanning
# recent successful buys for the target pool.

WSOL_MINT = Pubkey.from_string("So11111111111111111111111111111111111111112")

# Instruction discriminators (Anchor 8-byte prefix from the IDL).
# We prefer `buy_exact_quote_in` over `buy`: it lets us spend an exact lamport
# amount (matching what we wrapped into WSOL) instead of asking for an exact
# token amount and bidding up to a max SOL cap. `buy` was causing PumpSwap's
# post-swap accounting to overflow (Custom 6023) under our usage.
BUY_DISC = bytes([102, 6, 61, 18, 1, 218, 235, 234])  # legacy `buy`
BUY_EXACT_QUOTE_IN_DISC = bytes([198, 46, 21, 82, 180, 217, 232, 112])
SELL_DISC = bytes([51, 230, 133, 164, 1, 127, 131, 173])

# Pool layout offsets (after 8-byte account discriminator)
POOL_ACCOUNT_SIZE = 245
POOL_BASE_TA_OFFSET = 139  # 8 + 1 + 2 + 32*4
POOL_QUOTE_TA_OFFSET = 171
POOL_COIN_CREATOR_OFFSET = 211


# ---------------------------------------------------------------------------
# PDA helpers
# ---------------------------------------------------------------------------
def derive_pool(base_mint: Pubkey, quote_mint: Pubkey, index: int = 0) -> Pubkey:
    """Derive the canonical pool address.

    Pump.fun graduates default to index=0. We prefer using the pool address
    from Dexscreener (already known) — this is the fallback when we don't
    have one.
    """
    pda, _ = Pubkey.find_program_address(
        [
            b"pool",
            index.to_bytes(2, "little"),
            bytes(base_mint),
            bytes(quote_mint),
        ],
        PUMPSWAP_PROGRAM_ID,
    )
    return pda


def derive_event_authority() -> Pubkey:
    pda, _ = Pubkey.find_program_address([b"__event_authority"], PUMPSWAP_PROGRAM_ID)
    return pda


def derive_user_volume_accumulator(user: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"user_volume_accumulator", bytes(user)], PUMPSWAP_PROGRAM_ID
    )
    return pda


def derive_global_volume_accumulator() -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"global_volume_accumulator"], PUMPSWAP_PROGRAM_ID
    )
    return pda


def derive_creator_vault_authority(coin_creator: Pubkey) -> Pubkey:
    """The per-coin-creator vault authority PDA."""
    pda, _ = Pubkey.find_program_address(
        [b"creator_vault", bytes(coin_creator)], PUMPSWAP_PROGRAM_ID
    )
    return pda


def derive_ata(
    owner: Pubkey, mint: Pubkey, token_program: Pubkey = TOKEN_PROGRAM_ID
) -> Pubkey:
    """Standard SPL associated token account derivation.

    For Token-2022 mints, pass TOKEN_2022_PROGRAM_ID — the seeds use the token
    program ID, so a regular-Token ATA is at a different address than a
    Token-2022 ATA for the same (owner, mint).
    """
    pda, _ = Pubkey.find_program_address(
        [bytes(owner), bytes(token_program), bytes(mint)],
        ASSOCIATED_TOKEN_PROGRAM_ID,
    )
    return pda


def build_create_idempotent_ata_ix(
    payer: Pubkey,
    owner: Pubkey,
    mint: Pubkey,
    token_program: Pubkey = TOKEN_PROGRAM_ID,
) -> Instruction:
    """ATA program 'createIdempotent' (instruction discriminator = 1).

    The ATA program internally calls into `token_program` to initialize the
    account — for Token-2022 mints this MUST be TOKEN_2022_PROGRAM_ID or the
    on-chain call fails with IncorrectProgramId.
    """
    ata = derive_ata(owner, mint, token_program)
    return Instruction(
        program_id=ASSOCIATED_TOKEN_PROGRAM_ID,
        accounts=[
            AccountMeta(payer, is_signer=True, is_writable=True),
            AccountMeta(ata, is_signer=False, is_writable=True),
            AccountMeta(owner, is_signer=False, is_writable=False),
            AccountMeta(mint, is_signer=False, is_writable=False),
            AccountMeta(SYSTEM_PROGRAM_ID, is_signer=False, is_writable=False),
            AccountMeta(token_program, is_signer=False, is_writable=False),
        ],
        data=bytes([1]),  # 1 = createIdempotent
    )


def build_close_account_ix(
    account: Pubkey,
    dest: Pubkey,
    owner: Pubkey,
    token_program: Pubkey = TOKEN_PROGRAM_ID,
) -> Instruction:
    """SPL Token closeAccount (instruction tag = 9). Used to unwrap WSOL."""
    return Instruction(
        program_id=token_program,
        accounts=[
            AccountMeta(account, is_signer=False, is_writable=True),
            AccountMeta(dest, is_signer=False, is_writable=True),
            AccountMeta(owner, is_signer=True, is_writable=False),
        ],
        data=bytes([9]),
    )


def build_sync_native_ix(
    account: Pubkey, token_program: Pubkey = TOKEN_PROGRAM_ID
) -> Instruction:
    """SPL Token syncNative (instruction tag = 17). Updates WSOL amount after deposit."""
    return Instruction(
        program_id=token_program,
        accounts=[AccountMeta(account, is_signer=False, is_writable=True)],
        data=bytes([17]),
    )


async def discover_fee_remaining_accounts(
    client: SolanaClient,
    pool: Pubkey,
    instruction: str = "buy",
    max_lookback: int = 80,
) -> list[Pubkey]:
    """Scan recent successful txs for this pool and extract the
    `remaining_accounts` tail past index 22.

    `instruction` selects which discriminator to match:
        "buy"  — match any of buy / buy_exact_quote_in
        "sell" — match SELL_DISC only

    Buy and sell use DIFFERENT fee_recipient sets — picking a sell's recipients
    for a buy tx (or vice versa) causes PumpSwap's internal accounting to
    overflow (Custom 6023). Always discover per-direction.

    Cached per-(pool, instruction) per-process.
    """
    import base58
    cache = getattr(discover_fee_remaining_accounts, "_cache", None)
    if cache is None:
        cache = {}
        discover_fee_remaining_accounts._cache = cache  # type: ignore[attr-defined]
    cache_key = (str(pool), instruction)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    if instruction == "buy":
        match_discs = {BUY_DISC, BUY_EXACT_QUOTE_IN_DISC}
    elif instruction == "sell":
        match_discs = {SELL_DISC}
    else:
        raise ValueError(f"unknown instruction {instruction!r}")

    try:
        underlying = await client.get_client()
        sigs_resp = await underlying.get_signatures_for_address(
            pool, limit=max_lookback
        )
    except Exception:  # noqa: BLE001
        logger.warning("Could not fetch signatures for pool %s", pool)
        cache[cache_key] = []
        return []

    for s in sigs_resp.value:
        if s.err is not None:
            continue
        try:
            tx = await underlying.get_transaction(
                s.signature,
                max_supported_transaction_version=0,
                encoding="json",
            )
        except Exception:  # noqa: BLE001
            continue
        if not tx.value:
            continue
        try:
            keys = list(tx.value.transaction.transaction.message.account_keys)
            loaded = tx.value.transaction.meta.loaded_addresses
            if loaded:
                keys += list(loaded.writable) + list(loaded.readonly)
            for ix in tx.value.transaction.transaction.message.instructions:
                if str(keys[ix.program_id_index]) != str(PUMPSWAP_PROGRAM_ID):
                    continue
                raw = (
                    base58.b58decode(ix.data)
                    if isinstance(ix.data, str)
                    else bytes(ix.data)
                )
                if raw[:8] not in match_discs:
                    continue
                # Different IDLs for buy (23 accounts) vs sell (21 accounts).
                # Both have their respective remaining_accounts past their
                # last IDL slot. Hardcoded splits: buy at 23, sell at 21.
                ix_idl_count = 23 if instruction == "buy" else 21
                extras_idx = list(ix.accounts)[ix_idl_count:]
                extras = [keys[i] for i in extras_idx]
                logger.info(
                    "Discovered %d %s fee remaining_accounts for pool %s: %s",
                    len(extras), instruction, str(pool)[:12],
                    [str(p)[:12] for p in extras],
                )
                cache[cache_key] = extras
                return extras
        except Exception:  # noqa: BLE001
            continue

    logger.warning(
        "No successful %s txs found for pool %s — using no extras",
        instruction, pool,
    )
    cache[cache_key] = []
    return []


async def detect_token_program(client: SolanaClient, mint: Pubkey) -> Pubkey:
    """Return TOKEN_PROGRAM_ID or TOKEN_2022_PROGRAM_ID based on who owns the mint.

    Pump.fun graduates can be either classic SPL Token or Token-2022 mints —
    we have to read the mint account to find out which.
    """
    try:
        info = await client.get_account_info(mint)
        owner = info.owner
    except Exception:  # noqa: BLE001
        logger.warning("Failed to read mint %s owner; defaulting to TOKEN_PROGRAM_ID", mint)
        return TOKEN_PROGRAM_ID
    if owner == TOKEN_2022_PROGRAM_ID:
        return TOKEN_2022_PROGRAM_ID
    return TOKEN_PROGRAM_ID


def build_transfer_lamports_ix(
    from_: Pubkey, to: Pubkey, lamports: int
) -> Instruction:
    """System program transfer instruction (tag = 2).

    Data layout: 4-byte u32 tag (=2) + 8-byte u64 lamports = 12 bytes total.
    """
    return Instruction(
        program_id=SYSTEM_PROGRAM_ID,
        accounts=[
            AccountMeta(from_, is_signer=True, is_writable=True),
            AccountMeta(to, is_signer=False, is_writable=True),
        ],
        data=struct.pack("<IQ", 2, lamports),
    )


# ---------------------------------------------------------------------------
# Pool data fetch
# ---------------------------------------------------------------------------
@dataclass
class PoolState:
    pool: Pubkey
    base_mint: Pubkey
    quote_mint: Pubkey
    pool_base_ta: Pubkey
    pool_quote_ta: Pubkey
    coin_creator: Pubkey
    base_reserve: int  # raw token amount in pool
    quote_reserve: int  # lamports
    base_token_program: Pubkey = TOKEN_PROGRAM_ID
    quote_token_program: Pubkey = TOKEN_PROGRAM_ID  # WSOL is always regular
    # remaining_accounts the fee_program reads — pool-specific, dynamically
    # discovered by inspecting on-chain successful buys for this pool
    # or simulating. Empty list = no extras (works for some pool configs).
    fee_remaining_accounts: list[Pubkey] | None = None

    @property
    def price_quote_per_base(self) -> float:
        if self.base_reserve == 0:
            return 0.0
        return self.quote_reserve / self.base_reserve


async def fetch_pool_state(
    client: SolanaClient, pool: Pubkey, base_mint: Pubkey, quote_mint: Pubkey
) -> PoolState | None:
    """Read the Pool account and the two vault token accounts."""
    try:
        pool_info = await client.get_account_info(pool)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to read pool %s", pool)
        return None

    data = pool_info.data
    if not data or len(data) < POOL_ACCOUNT_SIZE:
        logger.warning("Pool %s data too small: %d bytes", pool, len(data) if data else 0)
        return None

    pool_base_ta = Pubkey.from_bytes(data[POOL_BASE_TA_OFFSET : POOL_BASE_TA_OFFSET + 32])
    pool_quote_ta = Pubkey.from_bytes(data[POOL_QUOTE_TA_OFFSET : POOL_QUOTE_TA_OFFSET + 32])
    coin_creator = Pubkey.from_bytes(
        data[POOL_COIN_CREATOR_OFFSET : POOL_COIN_CREATOR_OFFSET + 32]
    )

    # Read vault balances via getTokenAccountBalance — fast path
    try:
        base_reserve = await client.get_token_account_balance(pool_base_ta)
        quote_reserve = await client.get_token_account_balance(pool_quote_ta)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to read pool vaults")
        return None

    # Detect base mint's token program (Token-2022 vs classic SPL Token)
    base_token_program = await detect_token_program(client, base_mint)

    # NOTE: fee_remaining_accounts is intentionally left None here. Buy and
    # sell need DIFFERENT recipient sets; the caller (PumpSwapTrader.buy/sell)
    # calls discover_fee_remaining_accounts with the right `instruction` arg
    # right before building the ix.
    return PoolState(
        pool=pool,
        base_mint=base_mint,
        quote_mint=quote_mint,
        pool_base_ta=pool_base_ta,
        pool_quote_ta=pool_quote_ta,
        coin_creator=coin_creator,
        base_reserve=int(base_reserve),
        quote_reserve=int(quote_reserve),
        base_token_program=base_token_program,
        quote_token_program=TOKEN_PROGRAM_ID,  # WSOL
        fee_remaining_accounts=None,
    )


# ---------------------------------------------------------------------------
# Swap math (constant product)
# ---------------------------------------------------------------------------
def quote_in_for_base_out(
    base_out: int, base_reserve: int, quote_reserve: int, fee_bps: int = 100
) -> int:
    """Given desired base_out, return required quote_in (with fee).

    Pump-AMM fee is ~1% on the input. This is an approximation; the on-chain
    program may apply additional protocol fees, which is why we add a safety
    margin on top via slippage in the caller.
    """
    if base_out >= base_reserve:
        # Asking for more than what's in the pool — caller's problem
        return quote_reserve * 2  # arbitrary large
    numerator = quote_reserve * base_out * 10000
    denominator = (base_reserve - base_out) * (10000 - fee_bps)
    return (numerator // denominator) + 1


def base_out_for_quote_in(
    quote_in: int, base_reserve: int, quote_reserve: int, fee_bps: int = 100
) -> int:
    """Given quote_in, return expected base_out (after fee)."""
    fee_adjusted = quote_in * (10000 - fee_bps) // 10000
    numerator = base_reserve * fee_adjusted
    denominator = quote_reserve + fee_adjusted
    return numerator // denominator


def quote_out_for_base_in(
    base_in: int, base_reserve: int, quote_reserve: int, fee_bps: int = 100
) -> int:
    """Given base_in (selling), return expected quote_out (after fee)."""
    fee_adjusted = base_in * (10000 - fee_bps) // 10000
    numerator = quote_reserve * fee_adjusted
    denominator = base_reserve + fee_adjusted
    return numerator // denominator


# ---------------------------------------------------------------------------
# Instruction builders
# ---------------------------------------------------------------------------
def _option_bool(b: bool) -> bytes:
    """OptionBool is a struct wrapping a single bool — serialized as 1 byte.

    Despite the name, this is NOT Anchor's standard Option<bool> (which is 2 bytes).
    Verified by decoding real on-chain PumpSwap buys: ends in `00` for false,
    `01` for true. Documented in CLAUDE.md.
    """
    return b"\x01" if b else b"\x00"


def build_buy_exact_quote_in_ix(
    pool: PoolState,
    user: Pubkey,
    spendable_quote_in: int,
    min_base_amount_out: int,
) -> Instruction:
    """Construct the 25-account PumpSwap `buy_exact_quote_in` instruction.

    Spend exactly `spendable_quote_in` lamports of WSOL; require at least
    `min_base_amount_out` raw tokens out (slippage protection).
    """
    user_base_ta = derive_ata(user, pool.base_mint, pool.base_token_program)
    user_quote_ta = derive_ata(user, pool.quote_mint, pool.quote_token_program)
    creator_vault_authority = derive_creator_vault_authority(pool.coin_creator)
    creator_vault_ata = derive_ata(
        creator_vault_authority, pool.quote_mint, pool.quote_token_program
    )
    protocol_fee_recipient_ta = derive_ata(
        PROTOCOL_FEE_RECIPIENT, pool.quote_mint, pool.quote_token_program
    )
    global_vol = derive_global_volume_accumulator()
    user_vol = derive_user_volume_accumulator(user)
    event_auth = derive_event_authority()

    accounts = [
        AccountMeta(pool.pool, False, True),                            # 0
        AccountMeta(user, True, True),                                  # 1
        AccountMeta(GLOBAL_CONFIG, False, False),                       # 2
        AccountMeta(pool.base_mint, False, False),                      # 3
        AccountMeta(pool.quote_mint, False, False),                     # 4
        AccountMeta(user_base_ta, False, True),                         # 5
        AccountMeta(user_quote_ta, False, True),                        # 6
        AccountMeta(pool.pool_base_ta, False, True),                    # 7
        AccountMeta(pool.pool_quote_ta, False, True),                   # 8
        AccountMeta(PROTOCOL_FEE_RECIPIENT, False, False),              # 9
        AccountMeta(protocol_fee_recipient_ta, False, True),            # 10
        AccountMeta(pool.base_token_program, False, False),             # 11
        AccountMeta(pool.quote_token_program, False, False),            # 12
        AccountMeta(SYSTEM_PROGRAM_ID, False, False),                   # 13
        AccountMeta(ASSOCIATED_TOKEN_PROGRAM_ID, False, False),         # 14
        AccountMeta(event_auth, False, False),                          # 15
        AccountMeta(PUMPSWAP_PROGRAM_ID, False, False),                 # 16
        AccountMeta(creator_vault_ata, False, True),                    # 17
        AccountMeta(creator_vault_authority, False, False),             # 18
        AccountMeta(global_vol, False, True),                           # 19
        AccountMeta(user_vol, False, True),                             # 20
        AccountMeta(FEE_CONFIG, False, False),                          # 21
        AccountMeta(FEE_PROGRAM, False, False),                         # 22
        # remaining_accounts (fee_program-managed per-pool fee recipient ATAs +
        # config) — append at call site via pool.fee_remaining_accounts
    ]
    accounts.extend(
        AccountMeta(pk, False, True) for pk in (pool.fee_remaining_accounts or [])
    )

    data = (
        BUY_EXACT_QUOTE_IN_DISC
        + spendable_quote_in.to_bytes(8, "little")
        + min_base_amount_out.to_bytes(8, "little")
        + _option_bool(False)  # track_volume=false (matches known-good buys)
    )

    return Instruction(
        program_id=PUMPSWAP_PROGRAM_ID, accounts=accounts, data=data
    )


def build_sell_ix(
    pool: PoolState,
    user: Pubkey,
    base_amount_in: int,
    min_quote_amount_out: int,
) -> Instruction:
    """Construct the PumpSwap sell instruction (21 IDL accounts + per-pool fee extras)."""
    user_base_ta = derive_ata(user, pool.base_mint, pool.base_token_program)
    user_quote_ta = derive_ata(user, pool.quote_mint, pool.quote_token_program)
    creator_vault_authority = derive_creator_vault_authority(pool.coin_creator)
    creator_vault_ata = derive_ata(
        creator_vault_authority, pool.quote_mint, pool.quote_token_program
    )
    protocol_fee_recipient_ta = derive_ata(
        PROTOCOL_FEE_RECIPIENT, pool.quote_mint, pool.quote_token_program
    )
    event_auth = derive_event_authority()

    accounts = [
        AccountMeta(pool.pool, False, True),                            # 0
        AccountMeta(user, True, True),                                  # 1
        AccountMeta(GLOBAL_CONFIG, False, False),                       # 2
        AccountMeta(pool.base_mint, False, False),                      # 3
        AccountMeta(pool.quote_mint, False, False),                     # 4
        AccountMeta(user_base_ta, False, True),                         # 5
        AccountMeta(user_quote_ta, False, True),                        # 6
        AccountMeta(pool.pool_base_ta, False, True),                    # 7
        AccountMeta(pool.pool_quote_ta, False, True),                   # 8
        AccountMeta(PROTOCOL_FEE_RECIPIENT, False, False),              # 9
        AccountMeta(protocol_fee_recipient_ta, False, True),            # 10
        AccountMeta(pool.base_token_program, False, False),             # 11
        AccountMeta(pool.quote_token_program, False, False),            # 12
        AccountMeta(SYSTEM_PROGRAM_ID, False, False),                   # 13
        AccountMeta(ASSOCIATED_TOKEN_PROGRAM_ID, False, False),         # 14
        AccountMeta(event_auth, False, False),                          # 15
        AccountMeta(PUMPSWAP_PROGRAM_ID, False, False),                 # 16
        AccountMeta(creator_vault_ata, False, True),                    # 17
        AccountMeta(creator_vault_authority, False, False),             # 18
        AccountMeta(FEE_CONFIG, False, False),                          # 19
        AccountMeta(FEE_PROGRAM, False, False),                         # 20
    ]
    accounts.extend(
        AccountMeta(pk, False, True) for pk in (pool.fee_remaining_accounts or [])
    )

    data = (
        SELL_DISC
        + base_amount_in.to_bytes(8, "little")
        + min_quote_amount_out.to_bytes(8, "little")
    )

    return Instruction(
        program_id=PUMPSWAP_PROGRAM_ID, accounts=accounts, data=data
    )


# ---------------------------------------------------------------------------
# Public trader interface
# ---------------------------------------------------------------------------
class PumpSwapTrader:
    """Direct PumpSwap AMM trader — bypasses Jupiter for PumpSwap pools."""

    def __init__(
        self,
        client: SolanaClient,
        wallet: Wallet,
        slippage_bps: int = 1000,
        prioritization_fee_lamports: int = 500_000,
        compute_unit_limit: int = 250_000,
    ):
        self.client = client
        self.wallet = wallet
        self.slippage_bps = slippage_bps
        self.priority_fee = prioritization_fee_lamports
        self.cu_limit = compute_unit_limit

    # ----- buy ----------------------------------------------------------
    async def buy(
        self, token_info: TokenInfo, sol_amount_lamports: int
    ) -> TradeResult:
        if sol_amount_lamports <= 0:
            return self._fail("amount must be positive")

        pool_address = self._get_pool_address(token_info)
        if pool_address is None:
            return self._fail("no PumpSwap pool address provided")

        pool = await fetch_pool_state(
            self.client, pool_address, token_info.mint, WSOL_MINT
        )
        if pool is None:
            return self._fail("failed to fetch pool state")

        if pool.base_reserve == 0 or pool.quote_reserve == 0:
            return self._fail(
                f"pool has zero reserve (base={pool.base_reserve}, quote={pool.quote_reserve})"
            )

        # Discover buy-side fee accounts for THIS pool. If empty, the pool
        # has no recent successful buys — sending without remaining_accounts
        # is virtually guaranteed to fail with Overflow 6023. Skip instead.
        pool.fee_remaining_accounts = await discover_fee_remaining_accounts(
            self.client, pool.pool, instruction="buy",
        )
        if not pool.fee_remaining_accounts:
            return self._fail(
                "no recent successful buys for pool — skip to avoid wasted fee"
            )

        # Compute expected base_amount_out from constant-product math.
        # Then apply slippage tolerance: accept up to slippage_bps fewer tokens.
        expected_base = base_out_for_quote_in(
            sol_amount_lamports, pool.base_reserve, pool.quote_reserve
        )
        min_base_out = expected_base * (10000 - self.slippage_bps) // 10000
        if min_base_out <= 0:
            return self._fail("computed min_base_out <= 0 (slippage too wide?)")

        # Build the tx
        user = self.wallet.pubkey
        keypair: Keypair = self.wallet.keypair

        # Setup: ensure user_base_ta exists with the right token program
        user_base_ta = derive_ata(user, token_info.mint, pool.base_token_program)
        user_quote_ta = derive_ata(user, WSOL_MINT, pool.quote_token_program)

        instructions: list[Instruction] = [
            set_compute_unit_limit(self.cu_limit),
            set_compute_unit_price(self.priority_fee),
            build_create_idempotent_ata_ix(
                user, user, token_info.mint, pool.base_token_program
            ),
            # Wrap SOL: create WSOL ATA + transfer lamports + syncNative
            build_create_idempotent_ata_ix(
                user, user, WSOL_MINT, pool.quote_token_program
            ),
            build_transfer_lamports_ix(user, user_quote_ta, sol_amount_lamports),
            build_sync_native_ix(user_quote_ta, pool.quote_token_program),
            # The buy itself: spend EXACTLY sol_amount_lamports, get at least
            # min_base_out tokens (slippage-protected).
            build_buy_exact_quote_in_ix(
                pool, user, sol_amount_lamports, min_base_out
            ),
            # Unwrap any leftover WSOL back to native SOL
            build_close_account_ix(
                user_quote_ta, user, user, pool.quote_token_program
            ),
        ]

        sig = await self._sign_send(instructions, keypair, user)
        if sig is None:
            return self._fail("sendTransaction failed")

        confirmed = await self.client.confirm_transaction(sig)
        if not confirmed:
            return TradeResult(
                success=False,
                platform=Platform.PUMP_FUN,
                tx_signature=sig,
                error_message=f"buy failed to confirm: {sig}",
            )

        # Price: SOL per WHOLE TOKEN (not per raw unit). Position monitor
        # compares this to Dexscreener priceNative which is also per-whole-token.
        # pump.fun tokens use 6 decimals.
        sol_in = sol_amount_lamports / 1e9
        expected_whole_tokens = expected_base / 1e6
        price = sol_in / expected_whole_tokens if expected_whole_tokens > 0 else 0
        logger.info(
            "PumpSwap BUY confirmed: sig=%s expected_tokens=%.4f sol_spent=%.6f price=%.10f SOL/token",
            sig[:12] + "...", expected_whole_tokens, sol_in, price,
        )
        return TradeResult(
            success=True,
            platform=Platform.PUMP_FUN,
            tx_signature=sig,
            price=price,
            # amount is in WHOLE tokens — the universal_trader converts back
            # to raw via `* 1e6` when issuing the sell ix.
            amount=float(expected_whole_tokens),
        )

    # ----- sell ---------------------------------------------------------
    async def sell(
        self, token_info: TokenInfo, token_amount_raw: int
    ) -> TradeResult:
        if token_amount_raw <= 0:
            return self._fail("amount must be positive")

        pool_address = self._get_pool_address(token_info)
        if pool_address is None:
            return self._fail("no PumpSwap pool address provided")

        pool = await fetch_pool_state(
            self.client, pool_address, token_info.mint, WSOL_MINT
        )
        if pool is None:
            return self._fail("failed to fetch pool state")

        expected_quote = quote_out_for_base_in(
            token_amount_raw, pool.base_reserve, pool.quote_reserve
        )
        min_quote_out = expected_quote * (10000 - self.slippage_bps) // 10000
        if min_quote_out <= 0:
            return self._fail("computed min_quote_out <= 0")

        # Discover sell-side fee accounts for THIS pool (different from buy).
        # If no recent sells (e.g. token only-pumping, no exits yet), fall back
        # to buy-side accounts — empirically the recipients overlap enough that
        # buys' extras work as sell extras in this edge case.
        pool.fee_remaining_accounts = await discover_fee_remaining_accounts(
            self.client, pool.pool, instruction="sell",
        )
        if not pool.fee_remaining_accounts:
            logger.warning(
                "No recent sells for pool %s — falling back to buy-side fee accounts",
                str(pool.pool)[:12],
            )
            pool.fee_remaining_accounts = await discover_fee_remaining_accounts(
                self.client, pool.pool, instruction="buy",
            )
        if not pool.fee_remaining_accounts:
            return self._fail(
                "no fee discovery (neither buy nor sell txs on pool) — would drop"
            )

        user = self.wallet.pubkey
        keypair: Keypair = self.wallet.keypair
        user_quote_ta = derive_ata(user, WSOL_MINT, pool.quote_token_program)

        instructions: list[Instruction] = [
            set_compute_unit_limit(self.cu_limit),
            set_compute_unit_price(self.priority_fee),
            build_create_idempotent_ata_ix(
                user, user, WSOL_MINT, pool.quote_token_program
            ),
            build_sell_ix(pool, user, token_amount_raw, min_quote_out),
            # Unwrap WSOL to native SOL so PnL is visible in wallet
            build_close_account_ix(
                user_quote_ta, user, user, pool.quote_token_program
            ),
        ]

        sig = await self._sign_send(instructions, keypair, user)
        if sig is None:
            return self._fail("sendTransaction failed")

        confirmed = await self.client.confirm_transaction(sig)
        if not confirmed:
            return TradeResult(
                success=False,
                platform=Platform.PUMP_FUN,
                tx_signature=sig,
                error_message=f"sell failed to confirm: {sig}",
            )

        sol_out = expected_quote / 1e9
        whole_tokens = token_amount_raw / 1e6
        price = sol_out / whole_tokens if whole_tokens > 0 else 0
        logger.info(
            "PumpSwap SELL confirmed: sig=%s tokens_sold=%.4f expected_sol=%.6f price=%.10f SOL/token",
            sig[:12] + "...", whole_tokens, sol_out, price,
        )
        return TradeResult(
            success=True,
            platform=Platform.PUMP_FUN,
            tx_signature=sig,
            price=price,
            amount=sol_out,
        )

    # ----- internals ----------------------------------------------------
    def _get_pool_address(self, token_info: TokenInfo) -> Pubkey | None:
        ad = token_info.additional_data or {}
        candidate = ad.get("pumpswap_pool") or ad.get("pair_address")
        if not candidate:
            return None
        try:
            return Pubkey.from_string(candidate)
        except ValueError:
            return None

    async def _sign_send(
        self, instructions: list[Instruction], signer: Keypair, payer: Pubkey
    ) -> str | None:
        try:
            # Prefer the bot's cached blockhash (background-refreshed every 5s)
            # over fetching a new one — under burst load Helius starts 429ing
            # and the swap fails before we even sign. Cached blockhash is at
            # 'processed' commitment which can sometimes be slightly stale, but
            # with skipPreflight=true the leader will just retry with a fresher
            # one on the next slot.
            try:
                blockhash = await self.client.get_cached_blockhash()
            except Exception:  # noqa: BLE001
                # Cache not warm yet — fall back to direct fetch
                underlying = await self.client.get_client()
                bh_resp = await underlying.get_latest_blockhash(commitment="confirmed")
                blockhash = bh_resp.value.blockhash

            msg = MessageV0.try_compile(
                payer=payer,
                instructions=instructions,
                address_lookup_table_accounts=[],
                recent_blockhash=blockhash,
            )
            tx = VersionedTransaction(msg, [signer])

            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendTransaction",
                "params": [
                    base64.b64encode(bytes(tx)).decode(),
                    # maxRetries=5: Helius re-broadcasts the tx within the blockhash
                    # validity window. Crucial when competing for inclusion against
                    # other bots on hot tokens — a single submission frequently drops.
                    {"encoding": "base64", "skipPreflight": True, "maxRetries": 5},
                ],
            }
            resp = await self.client.post_rpc(body)
            if not resp or "result" not in resp:
                err = resp.get("error") if resp else "no response"
                logger.error("PumpSwap sendTransaction failed: %s", err)
                return None
            sig = resp["result"]
            logger.info(
                "PumpSwap tx submitted: %s (cu=%d priority=%d µL/CU, %d ixs)",
                sig, self.cu_limit, self.priority_fee, len(instructions),
            )
            return sig
        except Exception:  # noqa: BLE001
            logger.exception("PumpSwap sign/send failed")
            return None

    def _fail(self, msg: str) -> TradeResult:
        return TradeResult(
            success=False,
            platform=Platform.PUMP_FUN,
            error_message=f"PumpSwap: {msg}",
        )
