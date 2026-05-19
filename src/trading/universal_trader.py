"""
Universal trading coordinator that works with any platform.
Cleaned up to remove all platform-specific hardcoding.
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any

from solders.pubkey import Pubkey

from cleanup.modes import (
    handle_cleanup_after_failure,
    handle_cleanup_after_sell,
    handle_cleanup_post_session,
)
from core.client import SolanaClient
from core.priority_fee.manager import PriorityFeeManager
from core.wallet import Wallet
from integrations import strategy_db  # AI Strategy Manager: SQLite writer
from integrations.strategy_config import get_strategy_config  # AI Strategy Manager: config reader
from interfaces.core import Platform, TokenInfo
from monitoring.listener_factory import ListenerFactory
from platforms import get_platform_implementations
from trading.base import TradeResult
from trading.platform_aware import PlatformAwareBuyer, PlatformAwareSeller
from trading.position import Position
from utils.logger import get_logger

# Try to use uvloop on Unix or winloop on Windows for better performance
# Fall back to standard asyncio if not available
try:
    if sys.platform == "win32":
        import winloop

        asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
    else:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    # Standard asyncio is fine, just slightly slower
    pass

logger = get_logger(__name__)


class UniversalTrader:
    """Universal trading coordinator that works with any supported platform."""

    def __init__(
        self,
        rpc_endpoint: str,
        wss_endpoint: str,
        private_key: str,
        buy_amount: float,
        buy_slippage: float,
        sell_slippage: float,
        # AI Strategy Manager integration
        strategy_id: str = "default",
        is_paper_trade: bool = False,
        # Platform configuration
        platform: Platform | str = Platform.PUMP_FUN,
        # Listener configuration
        listener_type: str = "logs",
        geyser_endpoint: str | None = None,
        geyser_api_token: str | None = None,
        geyser_auth_type: str = "x-token",
        pumpportal_url: str = "wss://pumpportal.fun/api/data",
        # Trading configuration
        extreme_fast_mode: bool = False,
        extreme_fast_token_amount: int = 30,
        # Exit strategy configuration
        exit_strategy: str = "time_based",
        take_profit_percentage: float | None = None,
        stop_loss_percentage: float | None = None,
        max_hold_time: int | None = None,
        price_check_interval: int = 10,
        # Priority fee configuration
        enable_dynamic_priority_fee: bool = False,
        enable_fixed_priority_fee: bool = True,
        fixed_priority_fee: int = 200_000,
        extra_priority_fee: float = 0.0,
        hard_cap_prior_fee: int = 200_000,
        # Retry and timeout settings
        max_retries: int = 3,
        wait_time_after_creation: int = 15,
        wait_time_after_buy: int = 15,
        wait_time_before_new_token: int = 15,
        max_token_age: int | float = 0.001,
        token_wait_timeout: int = 30,
        # Cleanup settings
        cleanup_mode: str = "disabled",
        cleanup_force_close_with_burn: bool = False,
        cleanup_with_priority_fee: bool = False,
        # Trading filters
        match_string: str | None = None,
        bro_address: str | None = None,
        marry_mode: bool = False,
        yolo_mode: bool = False,
        # Compute unit configuration
        compute_units: dict | None = None,
        # Node provider configuration
        max_rps: float = 25.0,
        # Patient listener config — only used when listener_type == "patient"
        patient_min_age_seconds: int = 300,
        patient_max_age_seconds: int = 1800,
        patient_scan_interval_seconds: int = 30,
    ):
        """Initialize the universal trader."""
        # Core components
        self.solana_client = SolanaClient(rpc_endpoint, max_rps=max_rps)
        self.wallet = Wallet(private_key)
        self.priority_fee_manager = PriorityFeeManager(
            client=self.solana_client,
            enable_dynamic_fee=enable_dynamic_priority_fee,
            enable_fixed_fee=enable_fixed_priority_fee,
            fixed_fee=fixed_priority_fee,
            extra_fee=extra_priority_fee,
            hard_cap=hard_cap_prior_fee,
        )

        # Platform setup
        if isinstance(platform, str):
            self.platform = Platform(platform)
        else:
            self.platform = platform

        logger.info(f"Initialized Universal Trader for platform: {self.platform.value}")

        # Validate platform support
        try:
            from platforms import platform_factory

            if not platform_factory.registry.is_platform_supported(self.platform):
                raise ValueError(f"Platform {self.platform.value} is not supported")
        except Exception:
            logger.exception("Platform validation failed")
            raise

        # Get platform-specific implementations
        self.platform_implementations = get_platform_implementations(
            self.platform, self.solana_client
        )

        # Store compute unit configuration
        self.compute_units = compute_units or {}

        # Create platform-aware traders
        self.buyer = PlatformAwareBuyer(
            self.solana_client,
            self.wallet,
            self.priority_fee_manager,
            buy_amount,
            buy_slippage,
            max_retries,
            extreme_fast_token_amount,
            extreme_fast_mode,
            compute_units=self.compute_units,
        )

        self.seller = PlatformAwareSeller(
            self.solana_client,
            self.wallet,
            self.priority_fee_manager,
            sell_slippage,
            max_retries,
            compute_units=self.compute_units,
        )

        # Initialize the appropriate listener with platform filtering
        self.token_listener = ListenerFactory.create_listener(
            listener_type=listener_type,
            wss_endpoint=wss_endpoint,
            geyser_endpoint=geyser_endpoint,
            geyser_api_token=geyser_api_token,
            geyser_auth_type=geyser_auth_type,
            pumpportal_url=pumpportal_url,
            platforms=[self.platform],  # Only listen for our platform
            # Patient listener config — only relevant when listener_type=="patient"
            patient_min_age_seconds=patient_min_age_seconds,
            patient_max_age_seconds=patient_max_age_seconds,
            patient_scan_interval_seconds=patient_scan_interval_seconds,
        )

        # Trading parameters
        self.buy_amount = buy_amount
        self.buy_slippage = buy_slippage
        self.sell_slippage = sell_slippage
        self.max_retries = max_retries
        self.extreme_fast_mode = extreme_fast_mode
        self.extreme_fast_token_amount = extreme_fast_token_amount

        # Exit strategy parameters
        self.exit_strategy = exit_strategy.lower()
        self.take_profit_percentage = take_profit_percentage
        self.stop_loss_percentage = stop_loss_percentage
        self.max_hold_time = max_hold_time
        self.price_check_interval = price_check_interval

        # Timing parameters
        self.wait_time_after_creation = wait_time_after_creation
        self.wait_time_after_buy = wait_time_after_buy
        self.wait_time_before_new_token = wait_time_before_new_token
        self.max_token_age = max_token_age
        self.token_wait_timeout = token_wait_timeout

        # Cleanup parameters
        self.cleanup_mode = cleanup_mode
        self.cleanup_force_close_with_burn = cleanup_force_close_with_burn
        self.cleanup_with_priority_fee = cleanup_with_priority_fee

        # Trading filters/modes
        self.match_string = match_string
        self.bro_address = bro_address
        self.marry_mode = marry_mode
        self.yolo_mode = yolo_mode

        # State tracking
        self.traded_mints: set[Pubkey] = set()
        self.traded_token_programs: dict[
            str, Pubkey
        ] = {}  # Maps mint (as string) to token_program_id
        self.token_queue: asyncio.Queue = asyncio.Queue()
        self.processing: bool = False
        self.processed_tokens: set[str] = set()
        self.token_timestamps: dict[str, float] = {}

        # AI Strategy Manager integration
        self.strategy_id: str = strategy_id
        self.is_paper_trade: bool = is_paper_trade
        self.strategy_config = get_strategy_config()
        # Maps mint string -> (db_trade_id, buy_ts_monotonic, entry_price,
        # sol_price_at_buy_usd, balance_before_sol). Last field is the wallet's
        # SOL balance captured BEFORE the buy — post-sell balance minus this
        # equals the true PnL including gas, slippage, ATA creation, everything.
        self._open_trade_rows: dict[
            str, tuple[int, float, float, float, float]
        ] = {}
        # Filter metrics from the most recent run_safety_filters call, passed
        # from _handle_token into _handle_successful_buy.
        self._pending_trade_metrics: dict[str, Any] = {}
        # Pre-buy SOL balance captured for the trade currently in flight.
        self._pending_balance_before_sol: float = 0.0

    async def start(self) -> None:
        """Start the trading bot and listen for new tokens."""
        logger.info(f"Starting Universal Trader for {self.platform.value}")
        logger.info(
            f"Match filter: {self.match_string if self.match_string else 'None'}"
        )
        logger.info(
            f"Creator filter: {self.bro_address if self.bro_address else 'None'}"
        )
        logger.info(f"Marry mode: {self.marry_mode}")
        logger.info(f"YOLO mode: {self.yolo_mode}")
        logger.info(f"Exit strategy: {self.exit_strategy}")

        if self.exit_strategy == "tp_sl":
            logger.info(
                f"Take profit: {self.take_profit_percentage * 100 if self.take_profit_percentage else 'None'}%"
            )
            logger.info(
                f"Stop loss: {self.stop_loss_percentage * 100 if self.stop_loss_percentage else 'None'}%"
            )
            logger.info(
                f"Max hold time: {self.max_hold_time if self.max_hold_time else 'None'} seconds"
            )

        logger.info(f"Max token age: {self.max_token_age} seconds")

        try:
            health_resp = await self.solana_client.get_health()
            logger.info(f"RPC warm-up successful (getHealth passed: {health_resp})")
        except Exception as e:
            logger.warning(f"RPC warm-up failed: {e!s}")

        try:
            # Choose operating mode based on yolo_mode
            if not self.yolo_mode:
                # Single token mode: process one token and exit
                logger.info(
                    "Running in single token mode - will process one token and exit"
                )
                token_info = await self._wait_for_token()
                if token_info:
                    await self._handle_token(token_info)
                    logger.info("Finished processing single token. Exiting...")
                else:
                    logger.info(
                        f"No suitable token found within timeout period ({self.token_wait_timeout}s). Exiting..."
                    )
            else:
                # Continuous mode: process tokens until interrupted
                logger.info(
                    "Running in continuous mode - will process tokens until interrupted"
                )
                processor_task = asyncio.create_task(self._process_token_queue())

                try:
                    await self.token_listener.listen_for_tokens(
                        lambda token: self._queue_token(token),
                        self.match_string,
                        self.bro_address,
                    )
                except Exception:
                    logger.exception("Token listening stopped due to error")
                finally:
                    processor_task.cancel()
                    try:
                        await processor_task
                    except asyncio.CancelledError:
                        pass

        except Exception:
            logger.exception("Trading stopped due to error")

        finally:
            await self._cleanup_resources()
            logger.info("Universal Trader has shut down")

    async def _wait_for_token(self) -> TokenInfo | None:
        """Wait for a single token to be detected."""
        # Create a one-time event to signal when a token is found
        token_found = asyncio.Event()
        found_token = None

        async def token_callback(token: TokenInfo) -> None:
            nonlocal found_token
            token_key = str(token.mint)

            # Only process if not already processed and fresh
            if token_key not in self.processed_tokens:
                # Record when the token was discovered
                self.token_timestamps[token_key] = monotonic()
                found_token = token
                self.processed_tokens.add(token_key)
                token_found.set()

        listener_task = asyncio.create_task(
            self.token_listener.listen_for_tokens(
                token_callback,
                self.match_string,
                self.bro_address,
            )
        )

        # Wait for a token with a timeout
        try:
            logger.info(
                f"Waiting for a suitable token (timeout: {self.token_wait_timeout}s)..."
            )
            await asyncio.wait_for(token_found.wait(), timeout=self.token_wait_timeout)
            logger.info(f"Found token: {found_token.symbol} ({found_token.mint})")
            return found_token
        except TimeoutError:
            logger.info(
                f"Timed out after waiting {self.token_wait_timeout}s for a token"
            )
            return None
        finally:
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_resources(self) -> None:
        """Perform cleanup operations before shutting down."""
        if self.traded_mints:
            try:
                logger.info(f"Cleaning up {len(self.traded_mints)} traded token(s)...")
                # Build parallel lists of mints and token_program_ids
                mints_list = list(self.traded_mints)
                token_program_ids = [
                    self.traded_token_programs.get(str(mint)) for mint in mints_list
                ]
                await handle_cleanup_post_session(
                    self.solana_client,
                    self.wallet,
                    mints_list,
                    token_program_ids,
                    self.priority_fee_manager,
                    self.cleanup_mode,
                    self.cleanup_with_priority_fee,
                    self.cleanup_force_close_with_burn,
                )
            except Exception:
                logger.exception("Error during cleanup")

        old_keys = {k for k in self.token_timestamps if k not in self.processed_tokens}
        for key in old_keys:
            self.token_timestamps.pop(key, None)

        await self.solana_client.close()

    async def _queue_token(self, token_info: TokenInfo) -> None:
        """Queue a token for processing if not already processed."""
        token_key = str(token_info.mint)

        if token_key in self.processed_tokens:
            logger.debug(f"Token {token_info.symbol} already processed. Skipping...")
            return

        # Record timestamp when token was discovered
        self.token_timestamps[token_key] = monotonic()

        await self.token_queue.put(token_info)
        logger.info(
            f"Queued new token: {token_info.symbol} ({token_info.mint}) on {token_info.platform.value}"
        )

    async def _process_token_queue(self) -> None:
        """Continuously process tokens from the queue, only if they're fresh."""
        while True:
            try:
                token_info = await self.token_queue.get()
                token_key = str(token_info.mint)

                # Check if token is still "fresh"
                current_time = monotonic()
                token_age = current_time - self.token_timestamps.get(
                    token_key, current_time
                )

                if token_age > self.max_token_age:
                    logger.info(
                        f"Skipping token {token_info.symbol} - too old ({token_age:.1f}s > {self.max_token_age}s)"
                    )
                    # AI Strategy Manager: log to skipped_tokens
                    await strategy_db.insert_skipped_token(
                        strategy_id=self.strategy_id,
                        token_address=str(token_info.mint),
                        skip_reason="token_age_above_max",
                        notes=f"age={token_age:.1f}s max={self.max_token_age}s",
                    )
                    continue

                self.processed_tokens.add(token_key)

                logger.info(
                    f"Processing fresh token: {token_info.symbol} (age: {token_age:.1f}s)"
                )
                await self._handle_token(token_info)

            except asyncio.CancelledError:
                logger.info("Token queue processor was cancelled")
                break
            except Exception:
                logger.exception("Error in token queue processor")
            finally:
                self.token_queue.task_done()

    async def _handle_token(self, token_info: TokenInfo) -> None:
        """Handle a new token creation event."""
        try:
            mint_str = str(token_info.mint)

            # AI Strategy Manager: pause + emergency stop checks.
            # These re-read the config file every 30s so Claude's pause directives
            # take effect without a bot restart.
            if self.strategy_config.is_emergency_stopped():
                logger.warning(
                    "Emergency stop active. Skipping token %s.", token_info.symbol
                )
                await strategy_db.insert_skipped_token(
                    strategy_id=self.strategy_id,
                    token_address=mint_str,
                    skip_reason="emergency_stop_active",
                )
                return

            if self.strategy_config.is_bot_paused():
                logger.info(
                    "Bot paused by Strategy Manager. Skipping token %s.",
                    token_info.symbol,
                )
                await strategy_db.insert_skipped_token(
                    strategy_id=self.strategy_id,
                    token_address=mint_str,
                    skip_reason="bot_paused",
                )
                return

            if self.strategy_config.is_token_blacklisted(mint_str):
                logger.info("Token %s is blacklisted. Skipping.", token_info.symbol)
                await strategy_db.insert_skipped_token(
                    strategy_id=self.strategy_id,
                    token_address=mint_str,
                    skip_reason="blacklist_token",
                )
                return

            if token_info.creator and self.strategy_config.is_creator_blacklisted(
                str(token_info.creator)
            ):
                logger.info(
                    "Creator %s is blacklisted. Skipping token %s.",
                    token_info.creator,
                    token_info.symbol,
                )
                await strategy_db.insert_skipped_token(
                    strategy_id=self.strategy_id,
                    token_address=mint_str,
                    skip_reason="blacklist_creator",
                    notes=f"creator={token_info.creator}",
                )
                return

            # Validate that token is for our platform
            if token_info.platform != self.platform:
                logger.warning(
                    f"Token platform mismatch: expected {self.platform.value}, got {token_info.platform.value}"
                )
                return

            # Wait for pool/curve to stabilize (unless in extreme fast mode)
            if not self.extreme_fast_mode:
                await self._save_token_info(token_info)
                logger.info(
                    f"Waiting for {self.wait_time_after_creation} seconds for the pool/curve to stabilize..."
                )
                await asyncio.sleep(self.wait_time_after_creation)

            # AI Strategy Manager: run safety filters BEFORE buying.
            # Resolve the strategy's filter config from JSON every cycle so Claude's
            # tuned thresholds (liquidity floor, mcap ceiling, rug threshold) apply.
            decision = self.strategy_config.get_strategy(self.strategy_id)
            if decision is None:
                logger.info(
                    "Strategy '%s' is not active. Skipping %s.",
                    self.strategy_id,
                    token_info.symbol,
                )
                await strategy_db.insert_skipped_token(
                    strategy_id=self.strategy_id,
                    token_address=mint_str,
                    skip_reason="strategy_not_active",
                )
                return

            # Lazy import to avoid a hard dependency cycle at module load time.
            from integrations.safety_filters import FilterContext, run_safety_filters

            filter_ctx = FilterContext(
                token_info=token_info,
                pool_address=self._get_pool_address(token_info),
                curve_manager=self.platform_implementations.curve_manager,
                filters_config=decision.filters,
            )
            filter_result = await run_safety_filters(filter_ctx)
            if not filter_result.passed:
                metrics_str = ""
                if filter_result.metrics:
                    metrics_str = "; ".join(
                        f"{k}={v}" for k, v in filter_result.metrics.items()
                    )
                await strategy_db.insert_skipped_token(
                    strategy_id=self.strategy_id,
                    token_address=mint_str,
                    skip_reason=filter_result.skip_reason or "safety_filter_failed",
                    notes=metrics_str[:500] if metrics_str else None,
                )
                return

            # Stash the filter metrics so we can attach them to the trade row on buy.
            self._pending_trade_metrics = filter_result.metrics or {}

            # AI Strategy Manager: capture wallet SOL balance BEFORE the buy.
            # The delta between this and the post-sell balance is the TRUE
            # round-trip PnL — including gas, slippage, ATA creation, and the
            # actual trade outcome. Replaces the buggy sell_result.price math
            # that always reported 0% PnL.
            self._pending_balance_before_sol = await self._get_wallet_sol_balance()

            # Buy token
            logger.info(
                f"Buying {self.buy_amount:.6f} SOL worth of {token_info.symbol} on {token_info.platform.value}..."
            )
            buy_result: TradeResult = await self.buyer.execute(token_info)

            if buy_result.success:
                await self._handle_successful_buy(token_info, buy_result)
            else:
                await self._handle_failed_buy(token_info, buy_result)

            # Only wait for next token in yolo mode
            if self.yolo_mode:
                logger.info(
                    f"YOLO mode enabled. Waiting {self.wait_time_before_new_token} seconds before looking for next token..."
                )
                await asyncio.sleep(self.wait_time_before_new_token)

        except Exception:
            logger.exception(f"Error handling token {token_info.symbol}")

    async def _handle_successful_buy(
        self, token_info: TokenInfo, buy_result: TradeResult
    ) -> None:
        """Handle successful token purchase."""
        logger.info(
            f"Successfully bought {token_info.symbol} on {token_info.platform.value}"
        )
        self._log_trade(
            "buy",
            token_info,
            buy_result.price,
            buy_result.amount,
            buy_result.tx_signature,
        )

        # AI Strategy Manager: write the open trade row with whatever metrics the
        # safety filters computed at evaluation time (liquidity, mcap, rugcheck score).
        mint_str = str(token_info.mint)
        metrics = getattr(self, "_pending_trade_metrics", {}) or {}

        # Convert position size from SOL to USD using the SOL price we already cached
        position_size_usd = self.buy_amount
        try:
            from integrations.safety_filters import get_sol_price_usd
            sol_price = await get_sol_price_usd()
            position_size_usd = self.buy_amount * sol_price
        except Exception:  # noqa: BLE001
            logger.debug("SOL price unavailable; storing position_size in SOL units")

        # Compute sol_price for downstream USD conversions. Cache on the open
        # trade row so we use a SINGLE consistent price across buy + sell of
        # this trade (avoids weird PnL math if SOL price moves mid-trade).
        sol_price_for_trade = 0.0
        try:
            from integrations.safety_filters import get_sol_price_usd
            sol_price_for_trade = await get_sol_price_usd()
        except Exception:  # noqa: BLE001
            pass

        try:
            db_trade_id = await strategy_db.insert_trade_open(
                strategy_id=self.strategy_id,
                token_address=mint_str,
                entry_price_usd=buy_result.price or 0.0,
                position_size_usd=position_size_usd,
                liquidity_at_entry_usd=metrics.get("liquidity_usd"),
                market_cap_at_entry_usd=metrics.get("market_cap_usd"),
                rug_risk_score_at_entry=(
                    metrics.get("rugcheck_score") / 100
                    if isinstance(metrics.get("rugcheck_score"), (int, float))
                    else None
                ),
                is_paper_trade=self.is_paper_trade,
            )
            if db_trade_id is not None:
                # 5-tuple: (db_id, buy_ts, entry_price_sol, sol_price_usd, balance_before_sol)
                self._open_trade_rows[mint_str] = (
                    db_trade_id,
                    monotonic(),
                    buy_result.price or 0.0,
                    sol_price_for_trade,
                    self._pending_balance_before_sol,
                )
        except Exception:
            logger.exception("Strategy DB: failed to record open trade for %s", mint_str)
        finally:
            self._pending_trade_metrics = {}

        self.traded_mints.add(token_info.mint)
        # Track token program for cleanup
        mint_str = str(token_info.mint)
        if token_info.token_program_id:
            self.traded_token_programs[mint_str] = token_info.token_program_id

        # Choose exit strategy
        if not self.marry_mode:
            if self.exit_strategy == "tp_sl":
                await self._handle_tp_sl_exit(token_info, buy_result)
            elif self.exit_strategy == "time_based":
                await self._handle_time_based_exit(token_info, buy_result)
            elif self.exit_strategy == "manual":
                logger.info("Manual exit strategy - position will remain open")
        else:
            logger.info("Marry mode enabled. Skipping sell operation.")

    async def _handle_failed_buy(
        self, token_info: TokenInfo, buy_result: TradeResult
    ) -> None:
        """Handle failed token purchase."""
        logger.error(f"Failed to buy {token_info.symbol}: {buy_result.error_message}")
        # AI Strategy Manager: log as a skipped token with the underlying error
        await strategy_db.insert_skipped_token(
            strategy_id=self.strategy_id,
            token_address=str(token_info.mint),
            skip_reason="buy_tx_failed",
            notes=(buy_result.error_message or "")[:500],
        )
        # Close ATA if enabled
        await handle_cleanup_after_failure(
            self.solana_client,
            self.wallet,
            token_info.mint,
            token_info.token_program_id,
            self.priority_fee_manager,
            self.cleanup_mode,
            self.cleanup_with_priority_fee,
            self.cleanup_force_close_with_burn,
        )

    async def _handle_tp_sl_exit(
        self, token_info: TokenInfo, buy_result: TradeResult
    ) -> None:
        """Handle take profit/stop loss exit strategy."""
        # Create position
        position = Position.create_from_buy_result(
            mint=token_info.mint,
            symbol=token_info.symbol,
            entry_price=buy_result.price,
            quantity=buy_result.amount,
            take_profit_percentage=self.take_profit_percentage,
            stop_loss_percentage=self.stop_loss_percentage,
            max_hold_time=self.max_hold_time,
        )

        logger.info(f"Created position: {position}")
        if position.take_profit_price:
            logger.info(f"Take profit target: {position.take_profit_price:.8f} SOL")
        if position.stop_loss_price:
            logger.info(f"Stop loss target: {position.stop_loss_price:.8f} SOL")

        # Monitor position until exit condition is met
        await self._monitor_position_until_exit(token_info, position)

    async def _handle_time_based_exit(
        self, token_info: TokenInfo, buy_result: TradeResult
    ) -> None:
        """Handle legacy time-based exit strategy.

        Args:
            token_info: Token information
            buy_result: Result from the buy operation (contains token amount)
        """
        logger.info(f"Waiting for {self.wait_time_after_buy} seconds before selling...")
        await asyncio.sleep(self.wait_time_after_buy)

        logger.info(f"Selling {token_info.symbol}...")
        # Pass token amount and price from buy result to avoid RPC delays
        sell_result: TradeResult = await self.seller.execute(
            token_info, token_amount=buy_result.amount, token_price=buy_result.price
        )

        if sell_result.success:
            logger.info(f"Successfully sold {token_info.symbol}")
            self._log_trade(
                "sell",
                token_info,
                sell_result.price,
                sell_result.amount,
                sell_result.tx_signature,
            )
            # AI Strategy Manager: update the open trade row with exit info
            await self._record_trade_close(
                token_info=token_info,
                exit_price_usd=sell_result.price or 0.0,
                exit_reason="time_based",
                failed_exit=False,
            )
            # Close ATA if enabled
            await handle_cleanup_after_sell(
                self.solana_client,
                self.wallet,
                token_info.mint,
                token_info.token_program_id,
                self.priority_fee_manager,
                self.cleanup_mode,
                self.cleanup_with_priority_fee,
                self.cleanup_force_close_with_burn,
            )
        else:
            logger.error(
                f"Failed to sell {token_info.symbol}: {sell_result.error_message}"
            )
            # AI Strategy Manager: failed exits are critical — mark the trade
            await self._record_trade_close(
                token_info=token_info,
                exit_price_usd=0.0,
                exit_reason="failed_exit",
                failed_exit=True,
            )

    async def _record_trade_close(
        self,
        *,
        token_info: TokenInfo,
        exit_price_usd: float,
        exit_reason: str,
        failed_exit: bool,
    ) -> None:
        """AI Strategy Manager: update the open trade row with close info.

        Looks up the trade_id we stashed at buy time, computes pnl + time-in-trade,
        and updates the row. Never raises.
        """
        mint_str = str(token_info.mint)
        rec = self._open_trade_rows.pop(mint_str, None)
        if rec is None:
            logger.debug(
                "No open trade row for %s — skipping close update.", mint_str
            )
            return

        trade_id, buy_ts, entry_price, sol_price_at_buy, balance_before_sol = rec
        time_in_trade = int(monotonic() - buy_ts)
        position_size_usd = self.buy_amount * sol_price_at_buy

        # TRUE PnL via wallet SOL balance delta. This is the only honest measure:
        # it includes gas, ATA creation/cleanup, slippage on both buy and sell,
        # and the actual trade outcome. The Chainstack bot's sell_result.price
        # mirrors entry_price (a quirk of how the Seller is wired) and would
        # always report 0% PnL — useless for strategy analysis.
        balance_after_sol = await self._get_wallet_sol_balance()
        delta_sol = balance_after_sol - balance_before_sol  # negative = lost SOL
        pnl_usd = delta_sol * sol_price_at_buy
        # pnl_percent = return on position size (delta vs the SOL we put in)
        if self.buy_amount > 0:
            pnl_percent = (delta_sol / self.buy_amount) * 100
        else:
            pnl_percent = 0.0

        # Sanity overrides — if we genuinely couldn't get a balance reading or
        # the trade closed in a degenerate state, keep the prior behavior.
        if balance_before_sol <= 0 or balance_after_sol <= 0:
            logger.warning(
                "Wallet balance lookup failed; falling back to position-size full-loss for %s",
                mint_str,
            )
            if failed_exit:
                pnl_usd = -position_size_usd
                pnl_percent = -100.0
            else:
                pnl_usd = 0.0
                pnl_percent = 0.0

        logger.info(
            "True PnL for %s: delta_sol=%.6f, pnl_usd=$%.4f, pnl_pct=%.2f%%, "
            "reason=%s, failed_exit=%s",
            mint_str[:12],
            delta_sol,
            pnl_usd,
            pnl_percent,
            exit_reason,
            failed_exit,
        )

        try:
            await strategy_db.update_trade_close(
                trade_id=trade_id,
                exit_price_usd=exit_price_usd,
                pnl_usd=pnl_usd,
                pnl_percent=pnl_percent,
                exit_reason=exit_reason,
                time_in_trade_seconds=time_in_trade,
                failed_exit=failed_exit,
            )
        except Exception:
            logger.exception(
                "Strategy DB: failed to update trade close for %s (trade_id=%s)",
                mint_str,
                trade_id,
            )

    async def _monitor_position_until_exit(
        self, token_info: TokenInfo, position: Position
    ) -> None:
        """Monitor a position until exit conditions are met."""
        logger.info(
            f"Starting position monitoring (check interval: {self.price_check_interval}s)"
        )

        # Get pool address for price monitoring using platform-agnostic method
        pool_address = self._get_pool_address(token_info)
        curve_manager = self.platform_implementations.curve_manager

        while position.is_active:
            try:
                # Get current price from pool/curve
                current_price = await curve_manager.calculate_price(pool_address)

                # Check if position should be exited
                should_exit, exit_reason = position.should_exit(current_price)

                if should_exit and exit_reason:
                    logger.info(f"Exit condition met: {exit_reason.value}")
                    logger.info(f"Current price: {current_price:.8f} SOL")

                    # Log PnL before exit
                    pnl = position.get_pnl(current_price)
                    logger.info(
                        f"Position PnL: {pnl['price_change_pct']:.2f}% ({pnl['unrealized_pnl_sol']:.6f} SOL)"
                    )

                    # Execute sell with position quantity and entry price to avoid RPC delays
                    sell_result = await self.seller.execute(
                        token_info,
                        token_amount=position.quantity,
                        token_price=position.entry_price,
                    )

                    if sell_result.success:
                        # Close position with actual exit price
                        position.close_position(sell_result.price, exit_reason)

                        logger.info(
                            f"Successfully exited position: {exit_reason.value}"
                        )
                        self._log_trade(
                            "sell",
                            token_info,
                            sell_result.price,
                            sell_result.amount,
                            sell_result.tx_signature,
                        )

                        # AI Strategy Manager: update the open trade row
                        await self._record_trade_close(
                            token_info=token_info,
                            exit_price_usd=sell_result.price or 0.0,
                            exit_reason=exit_reason.value,
                            failed_exit=False,
                        )

                        # Log final PnL
                        final_pnl = position.get_pnl()
                        logger.info(
                            f"Final PnL: {final_pnl['price_change_pct']:.2f}% ({final_pnl['unrealized_pnl_sol']:.6f} SOL)"
                        )

                        # Close ATA if enabled
                        await handle_cleanup_after_sell(
                            self.solana_client,
                            self.wallet,
                            token_info.mint,
                            token_info.token_program_id,
                            self.priority_fee_manager,
                            self.cleanup_mode,
                            self.cleanup_with_priority_fee,
                            self.cleanup_force_close_with_burn,
                        )
                    else:
                        logger.error(
                            f"Failed to exit position: {sell_result.error_message}"
                        )
                        # AI Strategy Manager: the loop break below ends monitoring,
                        # so we MUST close out the open trade row here or it stays
                        # phantom-open forever. Marking failed_exit=True is the
                        # critical signal Claude uses to weight a strategy as broken.
                        await self._record_trade_close(
                            token_info=token_info,
                            exit_price_usd=0.0,
                            exit_reason="failed_exit",
                            failed_exit=True,
                        )

                    break
                else:
                    # Log current status
                    pnl = position.get_pnl(current_price)
                    logger.debug(
                        f"Position status: {current_price:.8f} SOL ({pnl['price_change_pct']:+.2f}%)"
                    )

                # Wait before next price check
                await asyncio.sleep(self.price_check_interval)

            except Exception:
                logger.exception("Error monitoring position")
                await asyncio.sleep(
                    self.price_check_interval
                )  # Continue monitoring despite errors

    async def _get_wallet_sol_balance(self) -> float:
        """AI Strategy Manager: fetch the wallet's native SOL balance in SOL units.

        Used by the true-PnL flow: balance_before_buy minus balance_after_sell
        gives the real round-trip outcome including gas, slippage, ATA creation,
        and the actual trade. Returns 0.0 on any failure — the caller treats
        zero as 'no signal' and falls back to position-size accounting.
        """
        try:
            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [str(self.wallet.pubkey)],
            }
            resp = await self.solana_client.post_rpc(body)
            if resp and "result" in resp and "value" in resp["result"]:
                return resp["result"]["value"] / 1_000_000_000
        except Exception:  # noqa: BLE001
            logger.exception("Failed to fetch wallet SOL balance")
        return 0.0

    def _get_pool_address(self, token_info: TokenInfo) -> Pubkey:
        """Get the pool/curve address for price monitoring using platform-agnostic method."""
        address_provider = self.platform_implementations.address_provider

        # Use platform-specific logic to get the appropriate address
        if hasattr(token_info, "bonding_curve") and token_info.bonding_curve:
            return token_info.bonding_curve
        elif hasattr(token_info, "pool_state") and token_info.pool_state:
            return token_info.pool_state
        else:
            # Fallback to deriving the address using platform provider
            return address_provider.derive_pool_address(token_info.mint)

    async def _save_token_info(self, token_info: TokenInfo) -> None:
        """Save token information to a file."""
        try:
            trades_dir = Path("trades")
            trades_dir.mkdir(exist_ok=True)
            file_path = trades_dir / f"{token_info.mint}.txt"

            # Convert to dictionary for saving - platform-agnostic
            token_dict = {
                "name": token_info.name,
                "symbol": token_info.symbol,
                "uri": token_info.uri,
                "mint": str(token_info.mint),
                "platform": token_info.platform.value,
                "user": str(token_info.user) if token_info.user else None,
                "creator": str(token_info.creator) if token_info.creator else None,
                "creation_timestamp": token_info.creation_timestamp,
            }

            # Add platform-specific fields only if they exist
            platform_fields = {
                "bonding_curve": token_info.bonding_curve,
                "associated_bonding_curve": token_info.associated_bonding_curve,
                "creator_vault": token_info.creator_vault,
                "pool_state": token_info.pool_state,
                "base_vault": token_info.base_vault,
                "quote_vault": token_info.quote_vault,
            }

            for field_name, field_value in platform_fields.items():
                if field_value is not None:
                    token_dict[field_name] = str(field_value)

            file_path.write_text(json.dumps(token_dict, indent=2))

            logger.info(f"Token information saved to {file_path}")
        except OSError:
            logger.exception("Failed to save token information")

    def _log_trade(
        self,
        action: str,
        token_info: TokenInfo,
        price: float,
        amount: float,
        tx_hash: str | None,
    ) -> None:
        """Log trade information."""
        try:
            trades_dir = Path("trades")
            trades_dir.mkdir(exist_ok=True)

            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "action": action,
                "platform": token_info.platform.value,
                "token_address": str(token_info.mint),
                "symbol": token_info.symbol,
                "price": price,
                "amount": amount,
                "tx_hash": str(tx_hash) if tx_hash else None,
            }

            log_file_path = trades_dir / "trades.log"
            with log_file_path.open("a", encoding="utf-8") as log_file:
                log_file.write(json.dumps(log_entry) + "\n")
        except OSError:
            logger.exception("Failed to log trade information")


# Backward compatibility alias
PumpTrader = UniversalTrader  # Legacy name for backward compatibility
