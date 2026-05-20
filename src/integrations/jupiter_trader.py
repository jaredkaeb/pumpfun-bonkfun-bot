"""
Jupiter aggregator trader — buys and sells migrated pump.fun tokens on PumpSwap/Raydium
without needing to know AMM math directly.

For pump.fun bonding-curve tokens (pre-migration) the bot's existing
trading/platform_aware.py path knows how to construct the right instructions.
After migration, those instructions don't apply — the token now trades on
PumpSwap (or Raydium, for older migrations). Rather than reimplement AMM
math for each protocol, this module routes through Jupiter v6 aggregator:

  1. Quote: ask Jupiter for the best route + expected output
  2. Swap: get back a serialized VersionedTransaction
  3. Sign + broadcast via the existing SolanaClient

Jupiter handles routing, slippage protection, and protocol-specific quirks.

Reference: https://station.jup.ag/docs/apis/swap-api
"""

from __future__ import annotations

import asyncio
import base64
import logging
from dataclasses import dataclass

import aiohttp
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

from core.client import SolanaClient
from core.wallet import Wallet
from trading.base import TradeResult
from interfaces.core import Platform, TokenInfo
from utils.logger import get_logger

logger = get_logger(__name__)

JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_URL = "https://quote-api.jup.ag/v6/swap"
SOL_MINT = "So11111111111111111111111111111111111111112"
QUOTE_TIMEOUT_SECONDS = 5.0
SWAP_TIMEOUT_SECONDS = 8.0


@dataclass
class JupiterQuote:
    """Minimal subset of Jupiter's quote response we need."""
    in_amount: int
    out_amount: int
    other_amount_threshold: int  # min out (or max in for ExactOut) after slippage
    price_impact_pct: float
    raw: dict  # full response — passed back to /swap


class JupiterTrader:
    """Buy/sell via Jupiter aggregator. Stateless — one instance shared by all trades."""

    def __init__(
        self,
        client: SolanaClient,
        wallet: Wallet,
        slippage_bps: int = 1000,
        prioritization_fee_lamports: int = 500_000,
    ):
        """
        Args:
            client: SolanaClient for tx submission
            wallet: Our trading wallet
            slippage_bps: Max accepted slippage in basis points (1000 = 10%, 500 = 5%)
            prioritization_fee_lamports: Same priority fee model as the rest of the bot
        """
        self.client = client
        self.wallet = wallet
        self.slippage_bps = slippage_bps
        self.prioritization_fee_lamports = prioritization_fee_lamports
        self._http_session: aiohttp.ClientSession | None = None

    async def close(self) -> None:
        if self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()

    async def _session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    # ---- public buy/sell -------------------------------------------------------

    async def buy(
        self,
        token_info: TokenInfo,
        sol_amount_lamports: int,
    ) -> TradeResult:
        """Buy a token using SOL via Jupiter routing."""
        return await self._swap(
            input_mint=SOL_MINT,
            output_mint=str(token_info.mint),
            amount=sol_amount_lamports,
            token_info=token_info,
            is_buy=True,
        )

    async def sell(
        self,
        token_info: TokenInfo,
        token_amount_raw: int,
    ) -> TradeResult:
        """Sell a token for SOL via Jupiter routing.

        `token_amount_raw` is in the token's smallest unit (not decimal). Caller
        is responsible for converting if needed.
        """
        return await self._swap(
            input_mint=str(token_info.mint),
            output_mint=SOL_MINT,
            amount=token_amount_raw,
            token_info=token_info,
            is_buy=False,
        )

    # ---- internals -------------------------------------------------------------

    async def _get_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
    ) -> JupiterQuote | None:
        """Call Jupiter /quote. Returns None on failure."""
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": str(self.slippage_bps),
            "onlyDirectRoutes": "false",
            "asLegacyTransaction": "false",
        }
        try:
            sess = await self._session()
            async with sess.get(
                JUPITER_QUOTE_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=QUOTE_TIMEOUT_SECONDS),
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning("Jupiter quote HTTP %d: %s", resp.status, text[:200])
                    return None
                data = await resp.json()
                return JupiterQuote(
                    in_amount=int(data["inAmount"]),
                    out_amount=int(data["outAmount"]),
                    other_amount_threshold=int(data["otherAmountThreshold"]),
                    price_impact_pct=float(data.get("priceImpactPct", 0)),
                    raw=data,
                )
        except asyncio.TimeoutError:
            logger.warning("Jupiter quote timed out")
            return None
        except Exception:  # noqa: BLE001
            logger.exception("Jupiter quote unexpected error")
            return None

    async def _get_swap_tx(self, quote: JupiterQuote) -> bytes | None:
        """Call Jupiter /swap with the quote, returns the unsigned tx bytes."""
        payload = {
            "quoteResponse": quote.raw,
            "userPublicKey": str(self.wallet.pubkey),
            "wrapAndUnwrapSol": True,
            "prioritizationFeeLamports": self.prioritization_fee_lamports,
            "asLegacyTransaction": False,
            "dynamicComputeUnitLimit": True,
        }
        try:
            sess = await self._session()
            async with sess.post(
                JUPITER_SWAP_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=SWAP_TIMEOUT_SECONDS),
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning("Jupiter swap HTTP %d: %s", resp.status, text[:200])
                    return None
                data = await resp.json()
                return base64.b64decode(data["swapTransaction"])
        except asyncio.TimeoutError:
            logger.warning("Jupiter swap timed out")
            return None
        except Exception:  # noqa: BLE001
            logger.exception("Jupiter swap unexpected error")
            return None

    async def _swap(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        token_info: TokenInfo,
        is_buy: bool,
    ) -> TradeResult:
        """Quote, build tx, sign, send. Returns TradeResult for consistency with
        the bot's existing buyer/seller interfaces.
        """
        if amount <= 0:
            return TradeResult(
                success=False,
                platform=Platform.PUMP_FUN,
                error_message="Jupiter: amount must be positive",
            )

        # 1. Quote
        quote = await self._get_quote(input_mint, output_mint, amount)
        if quote is None:
            return TradeResult(
                success=False,
                platform=Platform.PUMP_FUN,
                error_message="Jupiter quote failed",
            )

        # 2. Build swap tx
        raw_tx = await self._get_swap_tx(quote)
        if raw_tx is None:
            return TradeResult(
                success=False,
                platform=Platform.PUMP_FUN,
                error_message="Jupiter swap tx build failed",
            )

        # 3. Sign and broadcast
        try:
            # Deserialize Jupiter's tx, replace its (placeholder) signature with ours,
            # then re-serialize. solders does this via VersionedTransaction with
            # signers passed at construction.
            unsigned = VersionedTransaction.from_bytes(raw_tx)
            keypair: Keypair = self.wallet.keypair
            signed = VersionedTransaction(unsigned.message, [keypair])
            tx_bytes = bytes(signed)

            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendTransaction",
                "params": [
                    base64.b64encode(tx_bytes).decode(),
                    {"encoding": "base64", "skipPreflight": False, "maxRetries": 0},
                ],
            }
            resp = await self.client.post_rpc(body)
            if not resp or "result" not in resp:
                err = resp.get("error") if resp else "no response"
                logger.error("Jupiter sendTransaction failed: %s", err)
                return TradeResult(
                    success=False,
                    platform=Platform.PUMP_FUN,
                    error_message=f"sendTransaction: {err}",
                )

            sig = resp["result"]

            # Confirm — block until finalized OR timeout
            confirmed = await self.client.confirm_transaction(sig)
            if not confirmed:
                return TradeResult(
                    success=False,
                    platform=Platform.PUMP_FUN,
                    tx_signature=sig,
                    error_message=f"Transaction failed to confirm: {sig}",
                )

            # Compute price (SOL per token) from the quote
            if is_buy:
                # Buying: input is SOL, output is token. Price = SOL_in / tokens_out
                sol_in = quote.in_amount / 1_000_000_000
                tokens_out = quote.out_amount  # raw units, we don't know decimals here
                price = sol_in / tokens_out if tokens_out > 0 else 0
                amount_field = tokens_out  # raw token amount we now hold
            else:
                # Selling: input is token (raw), output is SOL. Price = SOL_out / tokens_in
                sol_out = quote.out_amount / 1_000_000_000
                tokens_in = quote.in_amount
                price = sol_out / tokens_in if tokens_in > 0 else 0
                amount_field = sol_out

            logger.info(
                "Jupiter %s OK: sig=%s in=%d out=%d price_impact=%.3f%%",
                "buy" if is_buy else "sell",
                sig[:12] + "...",
                quote.in_amount,
                quote.out_amount,
                quote.price_impact_pct * 100,
            )

            return TradeResult(
                success=True,
                platform=Platform.PUMP_FUN,
                tx_signature=sig,
                price=price,
                amount=amount_field,
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("Jupiter swap signing/sending failed")
            return TradeResult(
                success=False,
                platform=Platform.PUMP_FUN,
                error_message=f"Jupiter sign/send: {e}",
            )
