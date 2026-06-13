"""
One-time setup: initialize the user_volume_accumulator account for our wallet
under the PumpSwap AMM program.

Without this, Jupiter swaps that route through PumpSwap will fail with
ConstraintOwner (custom program error 0x7d4) on the ExtendAccount instruction,
because the account is owned by System Program (default for uninitialized
accounts) but PumpSwap expects it owned by pAMMBay6...

Run once per wallet. Costs ~0.002 SOL in rent + fees.

Usage:
    cd "<bot project root>" && source .venv/bin/activate
    python scripts/init_pumpswap_user_account.py
"""

from __future__ import annotations

import asyncio
import base64
import os
import sys
from pathlib import Path

import base58
from dotenv import load_dotenv
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.system_program import ID as SYSTEM_PROGRAM_ID
from solders.transaction import VersionedTransaction

# Add src to path so we can use the bot's SolanaClient
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

load_dotenv()

PUMPSWAP_PROGRAM_ID = Pubkey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
INIT_USER_VOLUME_ACCUMULATOR_DISCRIMINATOR = bytes([94, 6, 202, 115, 255, 96, 232, 183])


def derive_user_volume_accumulator(user: Pubkey) -> Pubkey:
    """PDA seeds: [b'user_volume_accumulator', user]."""
    pda, _bump = Pubkey.find_program_address(
        [b"user_volume_accumulator", bytes(user)],
        PUMPSWAP_PROGRAM_ID,
    )
    return pda


def derive_event_authority() -> Pubkey:
    """PDA seeds: [b'__event_authority']."""
    pda, _bump = Pubkey.find_program_address(
        [b"__event_authority"],
        PUMPSWAP_PROGRAM_ID,
    )
    return pda


def build_init_instruction(user: Pubkey) -> Instruction:
    """Construct the init_user_volume_accumulator instruction.

    Account order from the IDL:
      0. payer (writable, signer)
      1. user (read-only)
      2. user_volume_accumulator (writable)
      3. system_program
      4. event_authority
      5. program (the pump-amm program itself, per Anchor convention)
    """
    user_volume_acc = derive_user_volume_accumulator(user)
    event_authority = derive_event_authority()

    accounts = [
        AccountMeta(pubkey=user, is_signer=True, is_writable=True),       # payer
        AccountMeta(pubkey=user, is_signer=False, is_writable=False),      # user
        AccountMeta(pubkey=user_volume_acc, is_signer=False, is_writable=True),
        AccountMeta(pubkey=SYSTEM_PROGRAM_ID, is_signer=False, is_writable=False),
        AccountMeta(pubkey=event_authority, is_signer=False, is_writable=False),
        AccountMeta(pubkey=PUMPSWAP_PROGRAM_ID, is_signer=False, is_writable=False),
    ]

    return Instruction(
        program_id=PUMPSWAP_PROGRAM_ID,
        data=INIT_USER_VOLUME_ACCUMULATOR_DISCRIMINATOR,  # no args
        accounts=accounts,
    )


async def main() -> int:
    secret = os.environ.get("SOLANA_PRIVATE_KEY")
    if not secret:
        print("❌ SOLANA_PRIVATE_KEY missing from env. Run from the bot project root.")
        return 1

    rpc = os.environ.get("SOLANA_NODE_RPC_ENDPOINT")
    if not rpc:
        print("❌ SOLANA_NODE_RPC_ENDPOINT missing")
        return 1

    keypair = Keypair.from_bytes(base58.b58decode(secret))
    user = keypair.pubkey()
    print(f"Wallet: {user}")

    user_vol_acc = derive_user_volume_accumulator(user)
    event_auth = derive_event_authority()
    print(f"user_volume_accumulator PDA: {user_vol_acc}")
    print(f"event_authority PDA:         {event_auth}")
    print()

    # Use the bot's SolanaClient for sending. Constructor already kicks off
    # the blockhash updater as a background task — do NOT await it (it's a
    # `while True` loop).
    from core.client import SolanaClient
    client = SolanaClient(rpc, max_rps=25)
    print("SolanaClient ready, checking account state…", flush=True)

    try:
        # 0. Check if it's already initialized (idempotency check)
        try:
            existing = await client.get_account_info(user_vol_acc)
            owner = getattr(existing, "owner", None)
            if owner == PUMPSWAP_PROGRAM_ID:
                print("✅ user_volume_accumulator is already initialized and owned by PumpSwap.")
                print("   No action needed.")
                return 0
            print(f"Account exists but owner is {owner}. Will attempt init anyway.", flush=True)
        except ValueError:
            # Account doesn't exist — expected on first run
            print("Account does not exist yet — initializing.", flush=True)
        except Exception as e:
            print(f"get_account_info errored ({e!r}) — proceeding with init.", flush=True)

        # 1. Build the init instruction
        init_ix = build_init_instruction(user)

        # 2. Wrap with compute-budget instructions (200k CU is plenty)
        cu_limit_ix = set_compute_unit_limit(200_000)
        cu_price_ix = set_compute_unit_price(500_000)  # 500k microlamports/CU priority

        # 3. Get a fresh blockhash. Use 'confirmed' (not the bot's default
        # 'processed') so preflight simulation can actually see it on chain.
        underlying = await client.get_client()
        bh_resp = await underlying.get_latest_blockhash(commitment="confirmed")
        blockhash = bh_resp.value.blockhash
        print(f"Using blockhash: {blockhash}", flush=True)

        # 4. Build versioned tx
        msg = MessageV0.try_compile(
            payer=user,
            instructions=[cu_limit_ix, cu_price_ix, init_ix],
            address_lookup_table_accounts=[],
            recent_blockhash=blockhash,
        )
        tx = VersionedTransaction(msg, [keypair])

        # 5. Send
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                base64.b64encode(bytes(tx)).decode(),
                # skipPreflight=true: Helius preflight runs on a sometimes-lagging
                # node that rejects fresh-but-valid blockhashes. We rely on
                # confirm_transaction below to surface any real on-chain failure.
                {"encoding": "base64", "skipPreflight": True, "maxRetries": 0},
            ],
        }
        print("Sending init_user_volume_accumulator transaction…")
        resp = await client.post_rpc(body)
        if not resp or "result" not in resp:
            err = resp.get("error") if resp else "no response"
            print(f"❌ sendTransaction failed: {err}")
            return 2

        sig = resp["result"]
        print(f"Tx submitted: {sig}")
        print("Waiting for confirmation…")

        confirmed = await client.confirm_transaction(sig)
        if not confirmed:
            print(f"❌ Tx not confirmed in time. Check on chain: https://solscan.io/tx/{sig}")
            return 3

        # 6. Verify
        await asyncio.sleep(2)  # small lag for state to propagate
        final = await client.get_account_info(user_vol_acc)
        if final and final.owner == PUMPSWAP_PROGRAM_ID:
            print(f"✅ user_volume_accumulator initialized.")
            print(f"   Owner: {final.owner}")
            print(f"   Tx: https://solscan.io/tx/{sig}")
            print()
            print("Migrated-token Jupiter swaps should now work.")
            return 0
        else:
            print(f"⚠️  Tx confirmed but account state looks wrong. Inspect: https://solscan.io/tx/{sig}")
            return 4

    finally:
        await client.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
