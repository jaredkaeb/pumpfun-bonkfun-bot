"""
Liquidate orphan PumpSwap positions in the wallet.

Background: during early development, the bot bought tokens but the confirm
path timed out → bot didn't create Position records → positions just sit in
the wallet. This script sweeps them.

Usage:
    cd "<bot project root>" && source .venv/bin/activate
    python scripts/sell_orphan_positions.py [--dry-run] [--mint MINT_ADDRESS]

Strategy:
  - Scan wallet for all SPL Token / Token-2022 accounts with non-zero balance
  - For each, look up the pool on Dexscreener (must be PumpSwap-pumpfun pair)
  - Use the direct PumpSwap trader to sell the full balance at market
  - Skip mints with no PumpSwap pool (likely already migrated elsewhere or dust)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import aiohttp
import base58
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

load_dotenv()


WSOL = "So11111111111111111111111111111111111111112"
TOKEN_PROG = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


async def find_pumpswap_pool_for_mint(mint: str) -> tuple[str, str] | None:
    """Return (pool_address, dex_id) or None if not on PumpSwap."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    async with aiohttp.ClientSession() as sess:
        async with sess.get(
            url, timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    pairs = data.get("pairs") or []
    sol_pairs = [
        p for p in pairs
        if p.get("chainId") == "solana"
        and p.get("quoteToken", {}).get("address") == WSOL
        and p.get("dexId") in {"pumpswap", "pumpfun"}
    ]
    if not sol_pairs:
        return None
    canon = max(sol_pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
    return canon.get("pairAddress"), canon.get("dexId")


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mint", help="Sell only this mint")
    parser.add_argument("--slippage-bps", type=int, default=2000, help="Default 20%")
    args = parser.parse_args()

    secret = os.environ.get("SOLANA_PRIVATE_KEY")
    rpc = os.environ.get("SOLANA_NODE_RPC_ENDPOINT")
    if not (secret and rpc):
        print("Missing SOLANA_PRIVATE_KEY or SOLANA_NODE_RPC_ENDPOINT")
        return 1

    keypair = Keypair.from_bytes(base58.b58decode(secret))
    wallet = keypair.pubkey()
    print(f"Wallet: {wallet}")
    print(f"Dry run: {args.dry_run}")

    c = AsyncClient(rpc)
    # Enumerate token holdings
    holdings: list[tuple[str, float, int]] = []  # (mint, ui_amount, raw)
    for prog in (TOKEN_PROG, TOKEN_2022):
        opts = TokenAccountOpts(program_id=Pubkey.from_string(prog))
        resp = await c.get_token_accounts_by_owner_json_parsed(wallet, opts=opts)
        for a in resp.value:
            info = a.account.data.parsed["info"]
            mint = info["mint"]
            amt = info["tokenAmount"]
            ui = float(amt.get("uiAmountString") or 0)
            raw = int(amt.get("amount") or 0)
            if raw > 0 and ui > 0.001:  # ignore dust
                holdings.append((mint, ui, raw))

    await c.close()

    if args.mint:
        holdings = [h for h in holdings if h[0] == args.mint]
        if not holdings:
            print(f"Mint {args.mint} not held with non-zero balance")
            return 0

    print(f"\nFound {len(holdings)} non-dust holdings:")
    for m, ui, raw in holdings:
        print(f"  {m}: {ui} ({raw} raw)")
    if not holdings:
        return 0

    # Set up trader
    from core.client import SolanaClient
    from core.wallet import Wallet
    from interfaces.core import Platform, TokenInfo
    from integrations.pumpswap_trader import PumpSwapTrader

    bot_client = SolanaClient(rpc, max_rps=15)
    bot_wallet = Wallet(secret)
    trader = PumpSwapTrader(
        client=bot_client,
        wallet=bot_wallet,
        slippage_bps=args.slippage_bps,
        prioritization_fee_lamports=2_000_000,
        compute_unit_limit=250_000,
    )

    try:
        for mint, ui, raw in holdings:
            print(f"\n--- Selling {mint} ({ui} tokens) ---")
            pool_info = await find_pumpswap_pool_for_mint(mint)
            if pool_info is None:
                print("  No PumpSwap pool — skipping")
                continue
            pool, dex_id = pool_info
            print(f"  Pool: {pool} (dex={dex_id})")

            token_info = TokenInfo(
                name="orphan",
                symbol=mint[:8],
                uri="",
                mint=Pubkey.from_string(mint),
                platform=Platform.PUMP_FUN,
                additional_data={
                    "source": "orphan_sweep",
                    "pumpswap_pool": pool,
                    "dex_id": dex_id,
                },
            )
            if args.dry_run:
                print("  [dry-run] would sell")
                continue
            result = await trader.sell(token_info, raw)
            if result.success:
                print(f"  ✅ Sold. Sig: {result.tx_signature}")
                print(f"     SOL received (est): {result.amount:.6f}")
            else:
                print(f"  ❌ Sell failed: {result.error_message}")
                if result.tx_signature:
                    print(f"     Check tx: https://solscan.io/tx/{result.tx_signature}")
    finally:
        await bot_client.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
