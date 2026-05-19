"""Pre-trade safety filters wired to the AI Strategy Manager.

Each filter runs BEFORE the bot places a buy. If any returns FilterResult(passed=False),
we log to skipped_tokens and bail.

Filters (in roughly increasing cost order):
  1. curve_complete         — free, just inspects already-fetched curve state
  2. liquidity_floor        — reads curve reserves (cheap, one RPC if not cached)
  3. market_cap_ceiling     — uses curve state + SOL/USD price (one cached HTTP/min)
  4. rugcheck_score         — calls rugcheck.xyz API (one HTTP per token)
  5. simulated_sell         — STUB. Pump.fun bonding curve sells are protocol-fixed;
                              this hook is in place for post-migration or other DEXes.

A FilterContext bundles everything a filter might need so each filter has a stable
signature and we can add/remove filters without rewiring call sites.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_DECIMALS = 6  # pump.fun standard

# SOL/USD price cache — refresh every PRICE_CACHE_TTL seconds.
PRICE_CACHE_TTL = 60.0
_sol_price_usd: float | None = None
_sol_price_fetched_at: float = 0.0
_sol_price_lock = asyncio.Lock()


@dataclass
class FilterResult:
    passed: bool
    skip_reason: str | None = None  # populated when passed=False
    metrics: dict[str, Any] | None = None  # data the filter computed, for logging


@dataclass
class FilterContext:
    """All data a filter might need. Built once per token, passed to every filter."""
    token_info: Any  # TokenInfo from the bot
    pool_address: Any  # Pubkey of the bonding curve
    curve_manager: Any  # PumpFunCurveManager — fetches pool state
    filters_config: dict[str, Any]  # the Strategy Manager's filter dict


# --------------------------------------------------------------------------------------
# Filter 1: bonding curve has not completed (token hasn't migrated yet)
# --------------------------------------------------------------------------------------

async def filter_curve_not_complete(ctx: FilterContext) -> FilterResult:
    """Reject if the bonding curve has already completed (token has migrated to AMM).

    Post-migration tokens have completely different liquidity dynamics and our
    bonding-curve-based sniping strategy doesn't apply.
    """
    try:
        state = await ctx.curve_manager.get_pool_state(ctx.pool_address)
        if state.get("complete"):
            return FilterResult(
                passed=False,
                skip_reason="bonding_curve_complete",
                metrics={"complete": True},
            )
        return FilterResult(passed=True, metrics={"complete": False})
    except Exception as e:  # noqa: BLE001
        logger.warning("curve_not_complete check failed: %s", e)
        # Fail closed — if we can't read the curve, we don't trade.
        return FilterResult(passed=False, skip_reason="curve_state_unreadable")


# --------------------------------------------------------------------------------------
# Filter 2: liquidity floor (real SOL in the curve, in USD)
# --------------------------------------------------------------------------------------

async def filter_liquidity_floor(ctx: FilterContext) -> FilterResult:
    """Reject if real SOL in the bonding curve is below the configured USD floor.

    "Real" reserves matter, not "virtual" — virtual reserves are a constant offset
    used in the curve formula, but real_sol_reserves is what you can actually sell into.
    """
    floor_usd = ctx.filters_config.get("min_liquidity_usd")
    if floor_usd is None:
        return FilterResult(passed=True)  # filter disabled

    try:
        state = await ctx.curve_manager.get_pool_state(ctx.pool_address)
        real_sol = state.get("real_sol_reserves", 0) / LAMPORTS_PER_SOL
        sol_price = await get_sol_price_usd()
        liquidity_usd = real_sol * sol_price

        if liquidity_usd < floor_usd:
            return FilterResult(
                passed=False,
                skip_reason="liquidity_below_min",
                metrics={
                    "liquidity_usd": liquidity_usd,
                    "min_required_usd": floor_usd,
                    "real_sol": real_sol,
                },
            )
        return FilterResult(passed=True, metrics={"liquidity_usd": liquidity_usd})
    except Exception as e:  # noqa: BLE001
        logger.warning("liquidity_floor check failed: %s", e)
        return FilterResult(passed=False, skip_reason="liquidity_check_failed")


# --------------------------------------------------------------------------------------
# Filter 3: market cap ceiling
# --------------------------------------------------------------------------------------

async def filter_market_cap_ceiling(ctx: FilterContext) -> FilterResult:
    """Reject if estimated market cap is above the configured USD ceiling.

    Market cap = current price per token × total supply. Conservative — uses virtual
    reserves for price (which is what the bot pays on buy).
    """
    ceiling_usd = ctx.filters_config.get("max_market_cap_usd")
    if ceiling_usd is None:
        return FilterResult(passed=True)

    try:
        state = await ctx.curve_manager.get_pool_state(ctx.pool_address)
        virt_token = state["virtual_token_reserves"]
        virt_sol = state["virtual_sol_reserves"]
        total_supply_raw = state.get("token_total_supply", 0)

        if virt_token <= 0 or virt_sol <= 0 or total_supply_raw <= 0:
            return FilterResult(
                passed=False,
                skip_reason="invalid_curve_state",
                metrics={"virt_token": virt_token, "virt_sol": virt_sol},
            )

        # price (in SOL) per whole token
        price_per_token_sol = (
            (virt_sol / virt_token) * (10**TOKEN_DECIMALS) / LAMPORTS_PER_SOL
        )
        sol_price = await get_sol_price_usd()
        price_per_token_usd = price_per_token_sol * sol_price
        total_supply = total_supply_raw / (10**TOKEN_DECIMALS)
        market_cap_usd = price_per_token_usd * total_supply

        if market_cap_usd > ceiling_usd:
            return FilterResult(
                passed=False,
                skip_reason="market_cap_above_ceiling",
                metrics={
                    "market_cap_usd": market_cap_usd,
                    "max_allowed_usd": ceiling_usd,
                },
            )
        return FilterResult(passed=True, metrics={"market_cap_usd": market_cap_usd})
    except Exception as e:  # noqa: BLE001
        logger.warning("market_cap_ceiling check failed: %s", e)
        return FilterResult(passed=False, skip_reason="market_cap_check_failed")


# --------------------------------------------------------------------------------------
# Filter 4: rugcheck.xyz API integration
# --------------------------------------------------------------------------------------

RUGCHECK_API = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"
RUGCHECK_TIMEOUT_SECONDS = 4.0


async def filter_rugcheck(ctx: FilterContext) -> FilterResult:
    """Call rugcheck.xyz for the token's risk score. Reject if score exceeds threshold.

    rugcheck.xyz returns a structured report with a numeric `score` (0=clean, higher=risky)
    plus a list of `risks`. We use the score against the strategy's rug_risk_threshold.

    Threshold semantics: filters_config["rug_risk_threshold"] is in [0, 1] where higher
    = more tolerant of risk. We map this to rugcheck's 0-100 score by multiplying by 100.
    A threshold of 0.50 means "reject if rugcheck score > 50".
    """
    threshold = ctx.filters_config.get("rug_risk_threshold")
    if threshold is None:
        return FilterResult(passed=True)

    mint = str(ctx.token_info.mint)
    url = RUGCHECK_API.format(mint=mint)
    threshold_score = float(threshold) * 100

    try:
        timeout = aiohttp.ClientTimeout(total=RUGCHECK_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session, \
                   session.get(url) as resp:
            if resp.status == 404:
                # Token not yet indexed by rugcheck — common for very fresh launches.
                # Fail-open here is the wrong call (we don't know if it's safe).
                # But rejecting every fresh token would defeat the bot.
                # Compromise: pass with a warning + log to api_events later.
                logger.info("rugcheck: %s not yet indexed (HTTP 404)", mint)
                return FilterResult(
                    passed=True,
                    metrics={"rugcheck": "not_indexed"},
                )
            if resp.status != 200:
                logger.warning("rugcheck returned HTTP %d for %s", resp.status, mint)
                return FilterResult(
                    passed=False,
                    skip_reason="rugcheck_api_error",
                    metrics={"http_status": resp.status},
                )
            data = await resp.json()
    except asyncio.TimeoutError:
        logger.warning("rugcheck timed out for %s", mint)
        return FilterResult(passed=False, skip_reason="rugcheck_timeout")
    except aiohttp.ClientError as e:
        logger.warning("rugcheck network error for %s: %s", mint, e)
        return FilterResult(passed=False, skip_reason="rugcheck_network_error")

    score = float(data.get("score_normalised", data.get("score", 0)))
    if score > threshold_score:
        risks = [r.get("name") for r in data.get("risks", [])][:3]
        return FilterResult(
            passed=False,
            skip_reason="rug_risk_above_threshold",
            metrics={
                "rugcheck_score": score,
                "threshold": threshold_score,
                "top_risks": risks,
            },
        )

    return FilterResult(
        passed=True,
        metrics={"rugcheck_score": score},
    )


# --------------------------------------------------------------------------------------
# Filter 5: simulated sell — STUB for future expansion
# --------------------------------------------------------------------------------------

async def filter_simulated_sell(ctx: FilterContext) -> FilterResult:
    """Simulate the sell transaction before allowing the buy.

    NOT IMPLEMENTED for pump.fun. The pump.fun bonding curve program guarantees
    sell mechanics — as long as filter_curve_not_complete passes, sells will work.
    Implementing sim-sell here would require constructing the seller's exact tx
    (including correct cashback/mayhem account counts post-2026-04-28) and
    calling RPC simulateTransaction. Defer until we support a DEX where sims
    catch real failure modes (Raydium post-migration, Jupiter routes, etc.).
    """
    return FilterResult(passed=True, metrics={"simulated_sell": "skipped_for_pumpfun"})


# --------------------------------------------------------------------------------------
# SOL/USD price oracle (CoinGecko, cached)
# --------------------------------------------------------------------------------------

async def get_sol_price_usd() -> float:
    """Return SOL price in USD, cached for PRICE_CACHE_TTL seconds.

    Falls back to last known price on network error. Returns 100.0 as a hard
    fallback if we've never fetched successfully (better than crashing).
    """
    global _sol_price_usd, _sol_price_fetched_at

    async with _sol_price_lock:
        if _sol_price_usd is not None and (time.monotonic() - _sol_price_fetched_at) < PRICE_CACHE_TTL:
            return _sol_price_usd

        url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
        try:
            timeout = aiohttp.ClientTimeout(total=3.0)
            async with aiohttp.ClientSession(timeout=timeout) as session, \
                       session.get(url) as resp:
                data = await resp.json()
                price = float(data["solana"]["usd"])
                _sol_price_usd = price
                _sol_price_fetched_at = time.monotonic()
                return price
        except Exception as e:  # noqa: BLE001
            logger.warning("SOL price fetch failed: %s", e)
            return _sol_price_usd if _sol_price_usd is not None else 100.0


# --------------------------------------------------------------------------------------
# Orchestrator — run all enabled filters in order, short-circuit on first failure
# --------------------------------------------------------------------------------------

# Order matters: cheapest filters first so we can fail fast.
FILTER_PIPELINE = [
    filter_curve_not_complete,
    filter_liquidity_floor,
    filter_market_cap_ceiling,
    filter_simulated_sell,  # stub — always passes for pump.fun
    filter_rugcheck,  # slowest (HTTP), runs last
]


async def run_safety_filters(ctx: FilterContext) -> FilterResult:
    """Run all filters; return the first failure or pass if all succeed.

    The aggregate `metrics` dict on a passing result is the union of every
    filter's individual metrics — useful for logging to the trade row at buy time.
    """
    aggregate_metrics: dict[str, Any] = {}
    for filter_fn in FILTER_PIPELINE:
        result = await filter_fn(ctx)
        if result.metrics:
            aggregate_metrics.update(result.metrics)
        if not result.passed:
            logger.info(
                "Filter %s rejected %s: %s",
                filter_fn.__name__,
                ctx.token_info.symbol,
                result.skip_reason,
            )
            return FilterResult(
                passed=False,
                skip_reason=result.skip_reason,
                metrics=aggregate_metrics,
            )
    return FilterResult(passed=True, metrics=aggregate_metrics)
