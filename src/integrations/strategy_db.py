"""SQLite writer for the AI Strategy Manager schema.

Writes to the same database the Strategy Manager reads. Three tables:
  - trades         — one row per buy, updated on sell
  - skipped_tokens — one row per token the bot evaluated but didn't buy
  - api_events     — one row per outbound RPC / rug-check / price-feed call

Concurrency model: the Strategy Manager reads on its 45-min cadence, the bot
writes continuously. SQLite's WAL mode (set on connection) handles concurrent
readers fine. We open one short-lived connection per write to avoid holding a
file lock across the bot's async loop.

Path resolution: defaults to ../ai-strategy-manager/strategy_manager.db relative
to the bot's working directory. Override with STRATEGY_DB_PATH env var.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _default_db_path() -> Path:
    """Default to the sibling Strategy Manager's DB file."""
    env = os.environ.get("STRATEGY_DB_PATH")
    if env:
        return Path(env).expanduser().resolve()
    # cwd is the bot's directory when run via `pump_bot` or `uv run`
    return (Path.cwd().parent / "ai-strategy-manager" / "strategy_manager.db").resolve()


_db_path: Path | None = None
_db_path_lock = threading.Lock()


def get_db_path() -> Path:
    global _db_path
    with _db_path_lock:
        if _db_path is None:
            _db_path = _default_db_path()
            if not _db_path.exists():
                logger.warning(
                    "Strategy DB does not exist at %s. The Strategy Manager will create it "
                    "on its first run; writes from the bot before then will fail.",
                    _db_path,
                )
        return _db_path


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    """Open a short-lived connection in WAL mode."""
    conn = sqlite3.connect(get_db_path(), timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _write(sql: str, params: tuple) -> int | None:
    """Run a single INSERT/UPDATE. Returns lastrowid on success, None on failure.

    Never raises — the bot must keep trading even if the DB write fails.
    """
    try:
        with _connect() as conn:
            cur = conn.execute(sql, params)
            return cur.lastrowid
    except sqlite3.Error:
        logger.exception("Strategy DB write failed: %s", sql.split()[1] if len(sql.split()) > 1 else sql)
        return None


# Async wrappers that hand the work to a thread so we don't block the bot's
# event loop on disk I/O. SQLite writes are usually fast but WAL contention
# during heavy trade bursts could spike.

async def insert_trade_open(
    *,
    strategy_id: str,
    token_address: str,
    entry_price_usd: float,
    position_size_usd: float,
    slippage_entry_percent: float | None = None,
    liquidity_at_entry_usd: float | None = None,
    token_age_at_entry_sec: int | None = None,
    market_cap_at_entry_usd: float | None = None,
    volume_change_pre_entry: float | None = None,
    holder_growth_5m: float | None = None,
    rug_risk_score_at_entry: float | None = None,
    is_paper_trade: bool = False,
) -> int | None:
    """Write a new open trade row. Returns the trade id for later update."""
    sql = """
        INSERT INTO trades (
            strategy_id, token_address, entered_at, entry_price_usd,
            position_size_usd, slippage_entry_percent, liquidity_at_entry_usd,
            token_age_at_entry_sec, market_cap_at_entry_usd, volume_change_pre_entry,
            holder_growth_5m, rug_risk_score_at_entry, is_paper_trade
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        strategy_id,
        token_address,
        _now_iso(),
        entry_price_usd,
        position_size_usd,
        slippage_entry_percent,
        liquidity_at_entry_usd,
        token_age_at_entry_sec,
        market_cap_at_entry_usd,
        volume_change_pre_entry,
        holder_growth_5m,
        rug_risk_score_at_entry,
        1 if is_paper_trade else 0,
    )
    return await asyncio.to_thread(_write, sql, params)


async def update_trade_close(
    *,
    trade_id: int,
    exit_price_usd: float,
    pnl_usd: float,
    pnl_percent: float,
    exit_reason: str,
    time_in_trade_seconds: int,
    slippage_exit_percent: float | None = None,
    liquidity_at_exit_usd: float | None = None,
    failed_exit: bool = False,
    api_errors_during_trade: int = 0,
) -> None:
    """Update an open trade with exit info."""
    sql = """
        UPDATE trades
        SET exited_at = ?,
            exit_price_usd = ?,
            pnl_usd = ?,
            pnl_percent = ?,
            exit_reason = ?,
            time_in_trade_seconds = ?,
            slippage_exit_percent = ?,
            liquidity_at_exit_usd = ?,
            failed_exit = ?,
            api_errors_during_trade = ?
        WHERE id = ?
    """
    params = (
        _now_iso(),
        exit_price_usd,
        pnl_usd,
        pnl_percent,
        exit_reason,
        time_in_trade_seconds,
        slippage_exit_percent,
        liquidity_at_exit_usd,
        1 if failed_exit else 0,
        api_errors_during_trade,
        trade_id,
    )
    await asyncio.to_thread(_write, sql, params)


async def insert_skipped_token(
    *,
    strategy_id: str,
    token_address: str,
    skip_reason: str,
    notes: str | None = None,
) -> None:
    """Log a token we evaluated but didn't trade."""
    sql = """
        INSERT INTO skipped_tokens (
            skipped_at, strategy_id, token_address, skip_reason, notes
        ) VALUES (?, ?, ?, ?, ?)
    """
    params = (_now_iso(), strategy_id, token_address, skip_reason, notes)
    await asyncio.to_thread(_write, sql, params)


async def insert_api_event(
    *,
    provider: str,
    success: bool,
    latency_ms: int | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    """Log an outbound API call (RPC, rug check, price feed, etc.)."""
    sql = """
        INSERT INTO api_events (
            occurred_at, provider, success, latency_ms, error_code, error_message
        ) VALUES (?, ?, ?, ?, ?, ?)
    """
    params = (
        _now_iso(),
        provider,
        1 if success else 0,
        latency_ms,
        error_code,
        error_message,
    )
    await asyncio.to_thread(_write, sql, params)


# Synchronous variants for places that aren't in an async context
# (the bot is mostly async, but a few utility paths aren't).

def insert_skipped_token_sync(**kwargs: Any) -> None:
    sql = """
        INSERT INTO skipped_tokens (
            skipped_at, strategy_id, token_address, skip_reason, notes
        ) VALUES (?, ?, ?, ?, ?)
    """
    _write(sql, (
        _now_iso(),
        kwargs["strategy_id"],
        kwargs["token_address"],
        kwargs["skip_reason"],
        kwargs.get("notes"),
    ))


def insert_api_event_sync(**kwargs: Any) -> None:
    sql = """
        INSERT INTO api_events (
            occurred_at, provider, success, latency_ms, error_code, error_message
        ) VALUES (?, ?, ?, ?, ?, ?)
    """
    _write(sql, (
        _now_iso(),
        kwargs["provider"],
        1 if kwargs["success"] else 0,
        kwargs.get("latency_ms"),
        kwargs.get("error_code"),
        kwargs.get("error_message"),
    ))
