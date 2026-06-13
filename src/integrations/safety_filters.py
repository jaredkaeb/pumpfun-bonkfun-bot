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

# Pump.fun bonding curve genesis constants. Every newly-created pump.fun token
# starts with these reserves; price grows from there as buys flow in.
# Source: pump-public-docs and verified empirically on-chain.
PUMP_FUN_INITIAL_VIRTUAL_SOL_RESERVES = 30_000_000_000        # 30 SOL in lamports
PUMP_FUN_INITIAL_VIRTUAL_TOKEN_RESERVES = 1_073_000_191_000_000  # 1.073B tokens raw
PUMP_FUN_LAUNCH_PRICE_SOL_PER_TOKEN = (
    PUMP_FUN_INITIAL_VIRTUAL_SOL_RESERVES
    / PUMP_FUN_INITIAL_VIRTUAL_TOKEN_RESERVES
    * (10**TOKEN_DECIMALS)
    / LAMPORTS_PER_SOL
)  # ≈ 2.796e-8 SOL per whole token at genesis

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


def _is_amm_token(ctx: FilterContext) -> bool:
    """True if this token came from an AMM source (Dexscreener trending or migration listener).

    AMM tokens have no bonding curve — every curve-state-dependent filter
    should short-circuit pass for them, because the AMM listener already
    enforced its own liquidity/volume/age signals from Dexscreener.
    """
    ad = getattr(ctx.token_info, "additional_data", None) or {}
    return bool(
        ad.get("migration")
        or ad.get("source") == "dexscreener_trending"
        or ad.get("dex_id") in {"pumpswap", "raydium"}
    )


# --------------------------------------------------------------------------------------
# Filter 0: token age window (strategy-level age check)
# --------------------------------------------------------------------------------------

async def filter_token_age(ctx: FilterContext) -> FilterResult:
    """Enforce the strategy's min/max age window using TokenInfo.creation_timestamp.

    The bot's existing `max_token_age` (in YAML) is a queue-staleness check — it
    catches tokens that aged out of OUR queue between detection and processing.
    This filter is different: it enforces the STRATEGY's age window, e.g. for
    patient_v1 (only buy tokens 5-30 min old). Without this, the bot would
    process every fresh token regardless of strategy.

    Uses TokenInfo.creation_timestamp which is the on-chain CreateEvent timestamp.
    """
    min_age = ctx.filters_config.get("min_token_age_seconds")
    max_age = ctx.filters_config.get("max_token_age_seconds")
    if min_age is None and max_age is None:
        return FilterResult(passed=True)

    ts = getattr(ctx.token_info, "creation_timestamp", None)
    if ts is None or ts <= 0:
        # No on-chain timestamp — fail closed for strategies that care
        return FilterResult(
            passed=False,
            skip_reason="token_age_unknown",
        )

    import time as _time
    age_seconds = _time.time() - float(ts)

    if min_age is not None and age_seconds < min_age:
        return FilterResult(
            passed=False,
            skip_reason="token_age_below_min",
            metrics={"token_age_seconds": age_seconds, "min_required": min_age},
        )
    if max_age is not None and age_seconds > max_age:
        return FilterResult(
            passed=False,
            skip_reason="token_age_above_max",
            metrics={"token_age_seconds": age_seconds, "max_allowed": max_age},
        )

    return FilterResult(passed=True, metrics={"token_age_seconds": age_seconds})


# --------------------------------------------------------------------------------------
# Filter 1: bonding curve has not completed (token hasn't migrated yet)
# --------------------------------------------------------------------------------------

async def filter_curve_not_complete(ctx: FilterContext) -> FilterResult:
    """Reject if the bonding curve has already completed (token has migrated to AMM).

    Post-migration tokens have completely different liquidity dynamics and our
    bonding-curve-based sniping strategy doesn't apply.
    """
    if _is_amm_token(ctx):
        # AMM tokens have no curve — the listener routes them deliberately.
        return FilterResult(passed=True, metrics={"amm_skip": "curve_check"})
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

    if _is_amm_token(ctx):
        # Dexscreener listener filtered on its own liquidity_usd field;
        # cross-check here against the additional_data payload.
        ad = getattr(ctx.token_info, "additional_data", None) or {}
        liquidity_usd = float(ad.get("liquidity_usd") or 0)
        if liquidity_usd < floor_usd:
            return FilterResult(
                passed=False,
                skip_reason="liquidity_below_min",
                metrics={"liquidity_usd": liquidity_usd, "min_required_usd": floor_usd},
            )
        return FilterResult(passed=True, metrics={"liquidity_usd": liquidity_usd})

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

    if _is_amm_token(ctx):
        # Dexscreener tells us market_cap directly
        ad = getattr(ctx.token_info, "additional_data", None) or {}
        mc = float(ad.get("market_cap") or ad.get("fdv") or 0)
        if mc > 0 and mc > ceiling_usd:
            return FilterResult(
                passed=False,
                skip_reason="market_cap_above_ceiling",
                metrics={"market_cap_usd": mc, "max_allowed_usd": ceiling_usd},
            )
        return FilterResult(passed=True, metrics={"market_cap_usd": mc})

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
# Filter 3a: don't buy if curve has already pumped beyond launch
# --------------------------------------------------------------------------------------

async def filter_not_already_pumped(ctx: FilterContext) -> FilterResult:
    """Skip tokens whose price is already > max_price_ratio_from_launch × launch price.

    The single biggest reason our snipes lost ~90% per trade: by the time we
    were broadcasting a buy, faster snipers had already pumped the curve 50-150%
    above launch. We were the exit liquidity. This filter says: if the curve
    has already moved more than we tolerate from genesis, the snipe window has
    closed — skip.

    Threshold semantics: filters_config["max_price_ratio_from_launch"] is a
    multiplier (e.g. 1.5 means "skip if current price > 1.5x launch price").
    Disabled if absent.
    """
    max_ratio = ctx.filters_config.get("max_price_ratio_from_launch")
    if max_ratio is None:
        return FilterResult(passed=True)

    if _is_amm_token(ctx):
        # AMM tokens are post-graduation by definition — launch-ratio doesn't apply.
        return FilterResult(passed=True, metrics={"amm_skip": "launch_ratio"})

    try:
        state = await ctx.curve_manager.get_pool_state(ctx.pool_address)
        virt_token = state["virtual_token_reserves"]
        virt_sol = state["virtual_sol_reserves"]
        if virt_token <= 0 or virt_sol <= 0:
            return FilterResult(
                passed=False,
                skip_reason="invalid_curve_state",
                metrics={"virt_token": virt_token, "virt_sol": virt_sol},
            )

        current_price_sol_per_token = (
            (virt_sol / virt_token) * (10**TOKEN_DECIMALS) / LAMPORTS_PER_SOL
        )
        ratio = current_price_sol_per_token / PUMP_FUN_LAUNCH_PRICE_SOL_PER_TOKEN

        if ratio > max_ratio:
            return FilterResult(
                passed=False,
                skip_reason="already_pumped",
                metrics={
                    "price_ratio_from_launch": ratio,
                    "max_allowed_ratio": max_ratio,
                },
            )
        return FilterResult(
            passed=True,
            metrics={"price_ratio_from_launch": ratio},
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("not_already_pumped check failed: %s", e)
        return FilterResult(passed=False, skip_reason="pumped_check_failed")


# --------------------------------------------------------------------------------------
# Filter 3b: require substantive real SOL in the curve (not just creator + bots)
# --------------------------------------------------------------------------------------

async def filter_substantive_curve_liquidity(ctx: FilterContext) -> FilterResult:
    """Skip tokens whose curve has < min_real_sol_reserves_sol of REAL SOL.

    Pump.fun virtual reserves are an accounting trick; real_sol_reserves is the
    actual SOL deposited by buyers. A curve with <2 SOL real liquidity is either
    creator-only or a microcap that no one outside the sniper bot pool has
    touched — both correlate with immediate dumps.

    Distinct from filter_liquidity_floor which checks USD value of liquidity.
    This filter checks SOL count directly (decoupled from SOL price), which is
    a better proxy for "real buyer interest" — a $300 USD position at $84/SOL
    is 3.6 SOL, healthy real interest; a $300 position at $300/SOL is 1 SOL,
    creator-only.
    """
    min_real_sol = ctx.filters_config.get("min_real_sol_reserves_sol")
    if min_real_sol is None:
        return FilterResult(passed=True)

    if _is_amm_token(ctx):
        # No bonding curve. Use Dexscreener liquidity_usd / SOL price as an
        # equivalent "real liquidity" check.
        ad = getattr(ctx.token_info, "additional_data", None) or {}
        liq_usd = float(ad.get("liquidity_usd") or 0)
        sol_price = await get_sol_price_usd()
        real_sol = liq_usd / sol_price if sol_price > 0 else 0
        if real_sol < min_real_sol:
            return FilterResult(
                passed=False,
                skip_reason="real_sol_below_min",
                metrics={
                    "real_sol_reserves": real_sol,
                    "min_required_sol": min_real_sol,
                },
            )
        return FilterResult(
            passed=True,
            metrics={"real_sol_reserves": real_sol},
        )

    try:
        state = await ctx.curve_manager.get_pool_state(ctx.pool_address)
        real_sol = state.get("real_sol_reserves", 0) / LAMPORTS_PER_SOL

        if real_sol < min_real_sol:
            return FilterResult(
                passed=False,
                skip_reason="real_sol_below_min",
                metrics={
                    "real_sol_reserves": real_sol,
                    "min_required_sol": min_real_sol,
                },
            )
        return FilterResult(
            passed=True,
            metrics={"real_sol_reserves": real_sol},
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("substantive_curve_liquidity check failed: %s", e)
        return FilterResult(passed=False, skip_reason="real_sol_check_failed")


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
    except Exception as e:  # noqa: BLE001 — JSON decode errors, encoding issues, etc.
        logger.warning("rugcheck unexpected error for %s: %s", mint, e)
        return FilterResult(passed=False, skip_reason="rugcheck_unexpected_error")

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

# Order matters: cheapest filters first, slowest (HTTP) last.
# filter_token_age runs FIRST because it's pure-Python and is the most natural
# gate for patient_v1 strategy.
FILTER_PIPELINE = [
    filter_token_age,                    # strategy-level age window
    filter_curve_not_complete,
    filter_liquidity_floor,
    filter_market_cap_ceiling,
    filter_not_already_pumped,           # smart-snipe v1: skip post-pump tokens
    filter_substantive_curve_liquidity,  # smart-snipe v1: require real buyer interest
    filter_simulated_sell,               # stub — always passes for pump.fun
    filter_rugcheck,                     # slowest (HTTP), runs last
]


async def run_safety_filters(ctx: FilterContext) -> FilterResult:
    """Run all filters; return the first failure or pass if all succeed.

    The aggregate `metrics` dict on a passing result is the union of every
    filter's individual metrics — useful for logging to the trade row at buy time.

    Fail-closed: if any filter itself raises an unhandled exception, treat the
    token as rejected (filter_internal_error). Better to skip a possibly-good
    token than to trade on uninspected state.
    """
    aggregate_metrics: dict[str, Any] = {}
    for filter_fn in FILTER_PIPELINE:
        try:
            result = await filter_fn(ctx)
        except Exception as e:  # noqa: BLE001
            logger.exception("Filter %s raised: %s", filter_fn.__name__, e)
            return FilterResult(
                passed=False,
                skip_reason=f"filter_internal_error_{filter_fn.__name__}",
                metrics=aggregate_metrics,
            )
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
