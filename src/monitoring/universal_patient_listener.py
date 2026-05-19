"""
Patient listener — wraps the logs listener but defers callbacks until tokens mature.

The snipe strategy buys tokens at age <90s. That competes with faster sniper bots
and we usually lose. The patient strategy waits 5-30 minutes after a token launches
and only buys the ones that survived the initial dump (still on the bonding curve,
with substantive liquidity, not pumped beyond a sane multiple).

This listener:
  1. Subscribes to CREATE events via an inner UniversalLogsListener
  2. Adds every detected token to an internal watchlist with a monotonic timestamp
  3. Runs a background task every `scan_interval_seconds` that dispatches tokens
     whose age has reached `min_age_seconds`
  4. Evicts tokens older than `max_age_seconds` without firing (they missed the window)

The downstream `token_callback` (UniversalTrader._queue_token) receives matured tokens
the same way it would receive fresh ones — the trader has no idea they were delayed.
The trader's safety filter pipeline then runs as normal; the new `filter_token_age`
in safety_filters.py enforces the strategy's min/max age window.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from time import monotonic

from interfaces.core import Platform, TokenInfo
from monitoring.base_listener import BaseTokenListener
from monitoring.universal_logs_listener import UniversalLogsListener
from utils.logger import get_logger

logger = get_logger(__name__)


class UniversalPatientListener(BaseTokenListener):
    """Token listener that delays dispatch until tokens have aged into a target window."""

    def __init__(
        self,
        wss_endpoint: str,
        platforms: list[Platform] | None = None,
        patient_min_age_seconds: int = 300,
        patient_max_age_seconds: int = 1800,
        scan_interval_seconds: int = 30,
    ):
        """
        Args:
            wss_endpoint: WSS URL for the underlying logs subscription.
            platforms: Platforms to monitor (defaults to all supported).
            patient_min_age_seconds: Don't dispatch until token reaches this age.
            patient_max_age_seconds: Stop watching after this age (token missed window).
            scan_interval_seconds: How often the background loop checks for matured tokens.
        """
        super().__init__()
        if patient_max_age_seconds <= patient_min_age_seconds:
            raise ValueError(
                f"patient_max_age_seconds ({patient_max_age_seconds}) must be > "
                f"patient_min_age_seconds ({patient_min_age_seconds})"
            )

        self.wss_endpoint = wss_endpoint
        self.patient_min_age_seconds = patient_min_age_seconds
        self.patient_max_age_seconds = patient_max_age_seconds
        self.scan_interval_seconds = scan_interval_seconds

        # Inner listener handles the actual WSS subscription
        self._inner = UniversalLogsListener(wss_endpoint, platforms=platforms)

        # Watchlist: mint_str -> (token_info, monotonic_seen_at)
        # Note we track the time WE first saw the token, not its claimed
        # creation_timestamp — the former is reliably ours, the latter could
        # be spoofed or drifted by the chain's slot clock.
        self._watchlist: dict[str, tuple[TokenInfo, float]] = {}
        self._lock = asyncio.Lock()

    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        """Start the maturity-dispatch loop, then begin WSS subscription."""
        # Background task that scans watchlist and fires callbacks when ready
        dispatch_task = asyncio.create_task(
            self._dispatch_matured_loop(token_callback),
            name="patient-dispatch-loop",
        )

        async def _add_to_watchlist(token_info: TokenInfo) -> None:
            """Called by the inner listener for every CREATE event."""
            mint_str = str(token_info.mint)
            async with self._lock:
                if mint_str in self._watchlist:
                    return  # dedup
                self._watchlist[mint_str] = (token_info, monotonic())
            logger.info(
                "Patient: watching %s (%s) — will dispatch in ~%ds",
                token_info.symbol,
                mint_str[:12],
                self.patient_min_age_seconds,
            )

        try:
            await self._inner.listen_for_tokens(
                _add_to_watchlist,
                match_string=match_string,
                creator_address=creator_address,
            )
        finally:
            dispatch_task.cancel()
            try:
                await dispatch_task
            except asyncio.CancelledError:
                pass

    async def _dispatch_matured_loop(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
    ) -> None:
        """Periodically check the watchlist and dispatch matured tokens."""
        while True:
            try:
                await asyncio.sleep(self.scan_interval_seconds)
                now = monotonic()

                # Snapshot under lock so concurrent additions don't break the iter
                async with self._lock:
                    snapshot = list(self._watchlist.items())

                to_dispatch: list[TokenInfo] = []
                to_evict: list[str] = []

                for mint_str, (token_info, seen_at) in snapshot:
                    age = now - seen_at
                    if age >= self.patient_max_age_seconds:
                        to_evict.append(mint_str)
                    elif age >= self.patient_min_age_seconds:
                        to_dispatch.append(token_info)
                        to_evict.append(mint_str)
                    # else: still too young, keep watching

                # Remove processed entries from watchlist
                async with self._lock:
                    for mint_str in to_evict:
                        self._watchlist.pop(mint_str, None)

                if to_dispatch:
                    logger.info(
                        "Patient: dispatching %d matured token(s); watchlist size=%d",
                        len(to_dispatch),
                        len(self._watchlist),
                    )

                # Fire callbacks OUTSIDE the lock so a slow callback doesn't block
                # new additions. Each callback runs sequentially — same as the
                # logs listener behavior.
                for token_info in to_dispatch:
                    try:
                        await token_callback(token_info)
                    except Exception:
                        logger.exception(
                            "Patient dispatch callback raised for %s",
                            token_info.symbol,
                        )

            except asyncio.CancelledError:
                logger.info("Patient dispatch loop cancelled")
                raise
            except Exception:
                logger.exception("Error in patient dispatch loop (continuing)")
                # Don't break the loop — keep watching tokens even if one cycle errors
