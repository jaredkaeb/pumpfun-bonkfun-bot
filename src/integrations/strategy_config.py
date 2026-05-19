"""Read the Strategy Manager's adaptive config and translate to the bot's shape.

The Strategy Manager owns `adaptive_strategy_config.json` (see schema below).
Claude rewrites it every 30-60 minutes. The bot must re-read it periodically
so Claude's changes take effect without a restart.

This module:
  - locates the config file (env var override or sibling-dir default)
  - reads + caches with a configurable TTL (default 30s — well under the
    Strategy Manager's 45min cadence)
  - translates each Strategy Manager strategy entry into the dict shape
    universal_trader expects
  - exposes pause/emergency-stop flags as a single check
  - exposes a strategy selector based on `allocation_percent` weights

Strategy Manager config shape (what we READ):
{
  "version": int,
  "global": {
    "bot_paused": bool,
    "emergency_stop_triggered": bool,
    "max_open_positions": int,
    "default_position_size_usd": float,
    "default_cooldown_seconds": int
  },
  "strategies": {
    "<strategy_id>": {
      "name": str,
      "status": "active" | "paused",
      "allocation_percent": float,
      "position_size_usd": float,
      "filters": {
        "min_token_age_seconds": int,
        "max_token_age_seconds": int,
        "min_liquidity_usd": float,
        "max_market_cap_usd": float,
        "min_volume_change_percent": float,
        "rug_risk_threshold": float
      },
      "entry": { "min_holder_growth_percent_5m": float },
      "exit": {
        "stop_loss_percent": float,
        "take_profit_levels": [{"gain_percent": float, "sell_percent": float}, ...],
        "trailing_stop_percent": float,
        "max_hold_minutes": int
      },
      "cooldown_seconds": int
    }
  },
  "blacklist": { "tokens": [str, ...], "creators": [str, ...] }
}
"""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Default refresh interval. Lower bound on how stale a strategy choice can be.
DEFAULT_REFRESH_SECONDS = 30.0


def _default_config_path() -> Path:
    env = os.environ.get("STRATEGY_CONFIG_PATH")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.cwd().parent / "ai-strategy-manager" / "config" / "adaptive_strategy_config.json").resolve()


@dataclass
class StrategyDecision:
    """One strategy's parameters, resolved for the bot."""
    strategy_id: str
    name: str
    position_size_usd: float
    filters: dict[str, Any]
    entry: dict[str, Any]
    exit_config: dict[str, Any]
    cooldown_seconds: int


class StrategyConfig:
    """Cached, hot-reloadable view of the Strategy Manager config.

    Use a single instance per bot process. Thread-safe.
    """

    def __init__(self, refresh_seconds: float = DEFAULT_REFRESH_SECONDS):
        self._path = _default_config_path()
        self._refresh_seconds = refresh_seconds
        self._data: dict[str, Any] | None = None
        self._loaded_at: float = 0.0
        self._lock = threading.Lock()

        if not self._path.exists():
            logger.error(
                "Strategy config not found at %s. The bot cannot run without it.",
                self._path,
            )

    # ---- core load / cache ----------------------------------------------------

    def _load_if_stale(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            if self._data is None or (now - self._loaded_at) > self._refresh_seconds:
                try:
                    with open(self._path) as f:
                        self._data = json.load(f)
                    self._loaded_at = now
                    logger.debug(
                        "Reloaded strategy config v%s from %s",
                        self._data.get("version", "?"),
                        self._path,
                    )
                except (OSError, json.JSONDecodeError) as e:
                    logger.exception("Failed to read strategy config: %s", e)
                    if self._data is None:
                        # First load failed and we have nothing usable — return a
                        # safe-default that pauses the bot.
                        self._data = {
                            "global": {
                                "bot_paused": True,
                                "emergency_stop_triggered": True,
                            },
                            "strategies": {},
                            "blacklist": {"tokens": [], "creators": []},
                        }
            return self._data

    def reload(self) -> None:
        """Force a re-read on the next access."""
        with self._lock:
            self._loaded_at = 0.0

    # ---- public accessors -----------------------------------------------------

    def is_bot_paused(self) -> bool:
        cfg = self._load_if_stale()
        return bool(cfg.get("global", {}).get("bot_paused"))

    def is_emergency_stopped(self) -> bool:
        cfg = self._load_if_stale()
        return bool(cfg.get("global", {}).get("emergency_stop_triggered"))

    def max_open_positions(self) -> int:
        cfg = self._load_if_stale()
        return int(cfg.get("global", {}).get("max_open_positions", 5))

    def is_token_blacklisted(self, token_address: str) -> bool:
        cfg = self._load_if_stale()
        return token_address in cfg.get("blacklist", {}).get("tokens", [])

    def is_creator_blacklisted(self, creator_address: str) -> bool:
        cfg = self._load_if_stale()
        return creator_address in cfg.get("blacklist", {}).get("creators", [])

    def active_strategies(self) -> list[tuple[str, dict[str, Any]]]:
        """Return [(strategy_id, strategy_def), ...] for all active strategies."""
        cfg = self._load_if_stale()
        return [
            (sid, sdef)
            for sid, sdef in cfg.get("strategies", {}).items()
            if sdef.get("status") == "active"
        ]

    def select_strategy(
        self,
        *,
        rng: random.Random | None = None,
    ) -> StrategyDecision | None:
        """Pick one active strategy weighted by `allocation_percent`.

        Returns None if no active strategy is available. Callers should treat
        None as 'do not trade this token'.
        """
        active = self.active_strategies()
        if not active:
            return None

        weights = [float(sdef.get("allocation_percent", 0)) for _, sdef in active]
        if sum(weights) <= 0:
            return None

        chooser = rng or random
        strategy_id, sdef = chooser.choices(active, weights=weights, k=1)[0]
        return self._to_decision(strategy_id, sdef)

    def get_strategy(self, strategy_id: str) -> StrategyDecision | None:
        cfg = self._load_if_stale()
        sdef = cfg.get("strategies", {}).get(strategy_id)
        if not sdef or sdef.get("status") != "active":
            return None
        return self._to_decision(strategy_id, sdef)

    def token_passes_static_filters(
        self,
        decision: StrategyDecision,
        *,
        token_age_seconds: float,
        token_address: str | None = None,
        creator_address: str | None = None,
    ) -> tuple[bool, str | None]:
        """Quick filter checks that don't require RPC calls.

        Returns (passed, skip_reason). Skip-reason is non-None when passed=False.
        Dynamic filters (liquidity, market_cap, rug_risk) are checked elsewhere.
        """
        f = decision.filters

        if token_address and self.is_token_blacklisted(token_address):
            return False, "blacklist_token"
        if creator_address and self.is_creator_blacklisted(creator_address):
            return False, "blacklist_creator"

        min_age = f.get("min_token_age_seconds")
        if min_age is not None and token_age_seconds < min_age:
            return False, "token_age_below_min"

        max_age = f.get("max_token_age_seconds")
        if max_age is not None and token_age_seconds > max_age:
            return False, "token_age_above_max"

        return True, None

    # ---- internal -------------------------------------------------------------

    @staticmethod
    def _to_decision(strategy_id: str, sdef: dict[str, Any]) -> StrategyDecision:
        return StrategyDecision(
            strategy_id=strategy_id,
            name=str(sdef.get("name", strategy_id)),
            position_size_usd=float(sdef.get("position_size_usd", 5)),
            filters=dict(sdef.get("filters", {})),
            entry=dict(sdef.get("entry", {})),
            exit_config=dict(sdef.get("exit", {})),
            cooldown_seconds=int(sdef.get("cooldown_seconds", 60)),
        )


# Singleton — bots are one-config-per-process. Use get_strategy_config() everywhere.
_singleton: StrategyConfig | None = None
_singleton_lock = threading.Lock()


def get_strategy_config() -> StrategyConfig:
    global _singleton
    with _singleton_lock:
        if _singleton is None:
            _singleton = StrategyConfig()
        return _singleton
