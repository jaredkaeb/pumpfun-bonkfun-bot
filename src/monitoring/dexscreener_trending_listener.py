"""
Dexscreener trending listener — polls Dexscreener for Solana tokens showing
real momentum (1h–24h old, healthy liquidity, rising volume, more buys than
sells) and emits them as TokenInfo for the trader.

This is the post-graduation momentum strategy. Unlike snipe (catch at t=0) or
migration (catch the exact graduation moment), this listener watches the
*second wave* — the 1h–24h window after a token has already proven it can
trade, where degens find it on Dexscreener and pump it before the chart goes
vertical-then-dead. We can't out-MEV the snipers; we can out-discipline them
on filters + exits.

Signal sources (all free, no auth):
  1. /token-boosts/top/v1            — currently-boosted tokens (paid boost
                                       = real money behind the token)
  2. /token-profiles/latest/v1       — newly profiled tokens (likely launched
                                       in the last few hours)
  3. /latest/dex/search?q=SOL        — broad SOL-pair sample we can re-rank
                                       client-side by volume/liquidity

For each seed token we fetch the full pair payload via
`/latest/dex/tokens/{mint}` and apply hard filters. The first pair (sorted by
liquidity) is treated as canonical.

Filters are configurable per-bot via the YAML so the AI Strategy Manager
can tune them on real PnL data.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aiohttp
from solders.pubkey import Pubkey

from interfaces.core import Platform, TokenInfo
from monitoring.base_listener import BaseTokenListener
from utils.logger import get_logger

logger = get_logger(__name__)

DEXSCREENER_BASE = "https://api.dexscreener.com"
BOOSTS_TOP_URL = f"{DEXSCREENER_BASE}/token-boosts/top/v1"
PROFILES_LATEST_URL = f"{DEXSCREENER_BASE}/token-profiles/latest/v1"
SEARCH_URL = f"{DEXSCREENER_BASE}/latest/dex/search"
TOKEN_PAIRS_URL_T = f"{DEXSCREENER_BASE}/latest/dex/tokens/{{mint}}"

HTTP_TIMEOUT_SECONDS = 8.0
SOL_CHAIN_ID = "solana"
SOL_MINT = "So11111111111111111111111111111111111111112"


@dataclass
class TrendingFilters:
    """All thresholds are inclusive.

    EVIDENCE-BASED DEFAULTS (recalibrated from 7 losing trades):
    - All 7 losers had price_1h ≥ +24% at entry; biggest losers were +200%+ →
      cap max_price_change_1h_pct at 30%. Anything higher = local top.
    - Rug pools (FeABU, DDZ9, 7EW44, 2ZW) all had liq $20-30K → raise floor to $40K.
    - Tokens older than 4h were past their move → narrow window.
    """

    min_age_seconds: int = 3600           # 1h
    max_age_seconds: int = 28800          # 8h — was 4h (too strict, killed 29/63 candidates)
    min_liquidity_usd: float = 25_000.0   # was 40K (too strict, killed 11/63). Settled at 25K.
    min_volume_1h_usd: float = 10_000.0   # was 5K
    min_volume_6h_usd: float = 10_000.0   # NEW: sustained activity (relaxed from 30K)
    min_volume_rising_ratio: float = 1.0
    min_buys_vs_sells_ratio: float = 1.0  # buys >= sells (not strict dominance)
    min_price_change_1h_pct: float = 0.0  # positive 1h momentum
    min_price_change_6h_pct: float = 0.0
    # CRITICAL FIX: cap max 1h gain. We were entering at +200% 1h tokens which
    # are past local top. Stay in the early-momentum zone.
    max_price_change_1h_pct: float = 100.0   # cap losers at +100% 1h (was +30%, too strict for paper-data gathering)
    max_price_change_24h_pct: float = 1000.0 # was 500
    min_holders: int | None = None
    only_pump_fun_or_pumpswap: bool = True
    # 5-min momentum
    min_price_change_5m_pct: float = 0.0
    min_volume_5m_usd: float = 300.0      # loosened from 500
    min_buys_5m_vs_sells_ratio: float = 1.0  # buys >= sells in last 5min
    require_h1_gte_h6_pct: bool = False  # disabled during paper-data gathering


class DexscreenerTrendingListener(BaseTokenListener):
    """Polls Dexscreener and emits filtered Solana tokens as TokenInfo."""

    def __init__(
        self,
        platforms: list[Platform] | None = None,
        poll_interval_seconds: int = 45,
        filters: TrendingFilters | None = None,
        dedup_window_seconds: float = 300.0,  # was 600 — re-eval more often
    ):
        super().__init__()
        self.platforms = platforms or [Platform.PUMP_FUN]
        self.poll_interval = poll_interval_seconds
        self.filters = filters or TrendingFilters()
        self._dedup_window = dedup_window_seconds
        self._seen_mints: dict[str, float] = {}  # mint -> last emit monotonic
        self._http: aiohttp.ClientSession | None = None

    async def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession()
        return self._http

    async def close(self) -> None:
        if self._http and not self._http.closed:
            await self._http.close()

    # ------------------------------------------------------------------
    # Public entry point — same signature as other listeners
    # ------------------------------------------------------------------
    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        logger.info(
            "Dexscreener trending listener starting "
            "(poll=%ds, age=%d-%ds, min_liq=$%.0f, min_vol_1h=$%.0f)",
            self.poll_interval,
            self.filters.min_age_seconds,
            self.filters.max_age_seconds,
            self.filters.min_liquidity_usd,
            self.filters.min_volume_1h_usd,
        )
        while True:
            try:
                candidates = await self._gather_candidates()
                logger.info("Dexscreener cycle: %d candidate mints", len(candidates))
                emitted = 0
                for mint in candidates:
                    if self._is_deduped(mint):
                        continue
                    token_info = await self._evaluate_mint(mint)
                    if token_info is None:
                        continue
                    if match_string and match_string.lower() not in token_info.symbol.lower():
                        continue
                    if creator_address:
                        if (
                            token_info.creator is None
                            or str(token_info.creator) != creator_address
                        ):
                            continue
                    self._mark_seen(mint)
                    try:
                        await token_callback(token_info)
                        emitted += 1
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Dexscreener callback raised for %s", mint[:12]
                        )
                if emitted:
                    logger.info("Dexscreener cycle: emitted %d tokens", emitted)
            except Exception:  # noqa: BLE001
                logger.exception("Dexscreener poll cycle crashed; continuing")
            await asyncio.sleep(self.poll_interval)

    # ------------------------------------------------------------------
    # Candidate gathering — multiple seed sources, deduped to set of mints
    # ------------------------------------------------------------------
    async def _gather_candidates(self) -> list[str]:
        seeds: set[str] = set()
        sess = await self._session()

        # Source 1: top boosted (paid boost = $ behind the token)
        try:
            async with sess.get(
                BOOSTS_TOP_URL,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        for item in data:
                            if item.get("chainId") == SOL_CHAIN_ID:
                                mint = item.get("tokenAddress")
                                if mint:
                                    seeds.add(mint)
        except Exception:  # noqa: BLE001
            logger.debug("Boosts source failed (non-fatal)", exc_info=True)

        # Source 2: latest profiles (newly listed metadata)
        try:
            async with sess.get(
                PROFILES_LATEST_URL,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        for item in data:
                            if item.get("chainId") == SOL_CHAIN_ID:
                                mint = item.get("tokenAddress")
                                if mint:
                                    seeds.add(mint)
        except Exception:  # noqa: BLE001
            logger.debug("Profiles source failed (non-fatal)", exc_info=True)

        # Source 3: broad SOL search — returns ~30 pairs sorted by activity.
        # Cheap way to surface what Dexscreener's own UI is showing as hot.
        try:
            async with sess.get(
                SEARCH_URL,
                params={"q": "SOL"},
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs") or []
                    for p in pairs:
                        if p.get("chainId") != SOL_CHAIN_ID:
                            continue
                        # Non-SOL base mint only — the SOL-pegged token is
                        # the project token, not WSOL itself
                        base = p.get("baseToken", {}).get("address")
                        if base and base != SOL_MINT:
                            seeds.add(base)
        except Exception:  # noqa: BLE001
            logger.debug("Search source failed (non-fatal)", exc_info=True)

        return list(seeds)

    # ------------------------------------------------------------------
    # Per-mint evaluation — fetch all pairs for the mint, pick canonical,
    # apply filters, build TokenInfo
    # ------------------------------------------------------------------
    async def _evaluate_mint(self, mint: str) -> TokenInfo | None:
        url = TOKEN_PAIRS_URL_T.format(mint=mint)
        sess = await self._session()
        try:
            async with sess.get(
                url,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        except Exception:  # noqa: BLE001
            return None

        pairs = data.get("pairs") or []
        if not pairs:
            return None

        # Pick the SOL-quoted pair with the most liquidity. We ignore USDC
        # pairs because PumpSwap graduates are always SOL-quoted.
        sol_pairs = [
            p for p in pairs
            if p.get("chainId") == SOL_CHAIN_ID
            and p.get("quoteToken", {}).get("address") == SOL_MINT
        ]
        if not sol_pairs:
            return None
        canonical = max(
            sol_pairs,
            key=lambda p: (p.get("liquidity") or {}).get("usd") or 0,
        )

        ok, reason = self._passes_filters(canonical)
        if not ok:
            logger.debug("Dexscreener reject %s: %s", mint[:12], reason)
            return None

        # Build TokenInfo. We stash all the Dexscreener pair payload in
        # additional_data so downstream code (PumpSwapTrader, trailing-TP
        # logic, volume-fade exit) can re-query it without another HTTP hop.
        try:
            mint_pk = Pubkey.from_string(mint)
        except ValueError:
            return None

        base = canonical.get("baseToken", {})
        pair_created_ms = canonical.get("pairCreatedAt")
        creation_ts = (pair_created_ms / 1000.0) if pair_created_ms else None
        dex_id = canonical.get("dexId", "unknown")
        pool_address = canonical.get("pairAddress")

        logger.info(
            "Dexscreener pick: %s (%s) dex=%s liq=$%.0f vol1h=$%.0f "
            "price1h=%+.1f%% buys/sells=%d/%d",
            base.get("symbol", "?"),
            mint[:12],
            dex_id,
            (canonical.get("liquidity") or {}).get("usd") or 0,
            (canonical.get("volume") or {}).get("h1") or 0,
            (canonical.get("priceChange") or {}).get("h1") or 0,
            (canonical.get("txns") or {}).get("h1", {}).get("buys") or 0,
            (canonical.get("txns") or {}).get("h1", {}).get("sells") or 0,
        )

        return TokenInfo(
            name=base.get("name") or f"dex-{mint[:8]}",
            symbol=base.get("symbol") or mint[:8],
            uri="",
            mint=mint_pk,
            platform=Platform.PUMP_FUN,
            creation_timestamp=creation_ts,
            additional_data={
                "source": "dexscreener_trending",
                "dex_id": dex_id,
                "pumpswap_pool": pool_address if dex_id in {"pumpswap", "pumpfun"} else None,
                "pair_address": pool_address,
                "price_usd": canonical.get("priceUsd"),
                "liquidity_usd": (canonical.get("liquidity") or {}).get("usd"),
                "volume_h1": (canonical.get("volume") or {}).get("h1"),
                "volume_h6": (canonical.get("volume") or {}).get("h6"),
                "volume_h24": (canonical.get("volume") or {}).get("h24"),
                "price_change_h1": (canonical.get("priceChange") or {}).get("h1"),
                "price_change_h6": (canonical.get("priceChange") or {}).get("h6"),
                "price_change_h24": (canonical.get("priceChange") or {}).get("h24"),
                "price_change_m5": (canonical.get("priceChange") or {}).get("m5"),
                "volume_m5": (canonical.get("volume") or {}).get("m5"),
                "txns_h1_buys": (canonical.get("txns") or {}).get("h1", {}).get("buys"),
                "txns_h1_sells": (canonical.get("txns") or {}).get("h1", {}).get("sells"),
                "txns_m5_buys": (canonical.get("txns") or {}).get("m5", {}).get("buys"),
                "txns_m5_sells": (canonical.get("txns") or {}).get("m5", {}).get("sells"),
                "fdv": canonical.get("fdv"),
                "market_cap": canonical.get("marketCap"),
                "full_pair_payload": canonical,
            },
        )

    # ------------------------------------------------------------------
    # Filter logic
    # ------------------------------------------------------------------
    def _passes_filters(self, pair: dict) -> tuple[bool, str]:
        f = self.filters

        # DEX restriction
        if f.only_pump_fun_or_pumpswap:
            dex = pair.get("dexId", "")
            if dex not in {"pumpswap", "pumpfun", "raydium"}:
                return False, f"dex={dex}"

        # Age
        pair_created_ms = pair.get("pairCreatedAt")
        if pair_created_ms is None:
            return False, "no pair age"
        age_seconds = time.time() - (pair_created_ms / 1000.0)
        if age_seconds < f.min_age_seconds:
            return False, f"too young ({age_seconds:.0f}s)"
        if age_seconds > f.max_age_seconds:
            return False, f"too old ({age_seconds:.0f}s)"

        # Liquidity
        liq = (pair.get("liquidity") or {}).get("usd") or 0
        if liq < f.min_liquidity_usd:
            return False, f"low liq (${liq:.0f})"

        # Volume (1h)
        vol_h1 = (pair.get("volume") or {}).get("h1") or 0
        if vol_h1 < f.min_volume_1h_usd:
            return False, f"low vol_1h (${vol_h1:.0f})"

        # Volume rising: last 1h vs prior 1h (h6 - h1 averaged across 5 hours
        # = approximate prior-hour volume)
        vol_h6 = (pair.get("volume") or {}).get("h6") or 0
        prior_avg = max((vol_h6 - vol_h1) / 5.0, 1.0)
        if vol_h1 / prior_avg < f.min_volume_rising_ratio:
            return False, f"vol declining ({vol_h1:.0f}/{prior_avg:.0f})"

        # Buy/sell ratio in last hour
        txns_h1 = (pair.get("txns") or {}).get("h1", {})
        buys = txns_h1.get("buys") or 0
        sells = txns_h1.get("sells") or 0
        if sells > 0 and (buys / sells) < f.min_buys_vs_sells_ratio:
            return False, f"sells > buys ({buys}/{sells})"
        if buys + sells < 20:
            return False, f"too few txns ({buys+sells})"

        # Price action
        pc1 = (pair.get("priceChange") or {}).get("h1") or 0
        pc6 = (pair.get("priceChange") or {}).get("h6") or 0
        pc24 = (pair.get("priceChange") or {}).get("h24") or 0
        if pc1 < f.min_price_change_1h_pct:
            return False, f"price_1h={pc1:.1f}%"
        if pc1 > f.max_price_change_1h_pct:
            return False, f"price_1h={pc1:.1f}% (past local top)"
        if pc6 < f.min_price_change_6h_pct:
            return False, f"price_6h={pc6:.1f}%"
        if pc24 > f.max_price_change_24h_pct:
            return False, f"price_24h={pc24:.1f}% (exit liquidity)"

        # Sustained activity (h6 volume) — single-hour pumps without h6 support
        # are dump-and-go patterns we want to skip.
        vol_h6 = (pair.get("volume") or {}).get("h6") or 0
        if vol_h6 < f.min_volume_6h_usd:
            return False, f"vol_6h=${vol_h6:.0f} (no sustained activity)"

        # ---- 5-MINUTE GATES — catch BUILDING momentum, not peaks ----
        pc5m = (pair.get("priceChange") or {}).get("m5") or 0
        if pc5m < f.min_price_change_5m_pct:
            return False, f"price_5m={pc5m:.1f}% (not building)"

        vol_m5 = (pair.get("volume") or {}).get("m5") or 0
        if vol_m5 < f.min_volume_5m_usd:
            return False, f"vol_5m=${vol_m5:.0f} (dead)"

        txns_m5 = (pair.get("txns") or {}).get("m5", {})
        buys_m5 = txns_m5.get("buys") or 0
        sells_m5 = txns_m5.get("sells") or 0
        if sells_m5 > 0 and (buys_m5 / sells_m5) < f.min_buys_5m_vs_sells_ratio:
            return False, (
                f"5m sell pressure: buys/sells={buys_m5}/{sells_m5} "
                f"(need >={f.min_buys_5m_vs_sells_ratio:.1f}x)"
            )

        # Recent acceleration: 1h % gain > 6h average → momentum building, not fading
        if f.require_h1_gte_h6_pct:
            # If 6h shows much bigger gain than 1h, the move already happened
            # an hour+ ago and we'd be entering after the peak.
            if pc6 > 5 and pc1 < pc6 * 0.3:
                return False, (
                    f"momentum fading: pc1={pc1:.1f}% << pc6={pc6:.1f}%"
                )

        return True, "ok"

    # ------------------------------------------------------------------
    # Dedup
    # ------------------------------------------------------------------
    def _is_deduped(self, mint: str) -> bool:
        now = time.monotonic()
        # Evict old entries lazily
        self._seen_mints = {
            k: v for k, v in self._seen_mints.items()
            if now - v < self._dedup_window
        }
        return mint in self._seen_mints

    def _mark_seen(self, mint: str) -> None:
        self._seen_mints[mint] = time.monotonic()
