"""
Factory for creating platform-aware token listeners.
"""

from interfaces.core import Platform
from monitoring.base_listener import BaseTokenListener
from utils.logger import get_logger

logger = get_logger(__name__)


class ListenerFactory:
    """Factory for creating appropriate token listeners based on configuration."""

    @staticmethod
    def create_listener(
        listener_type: str,
        wss_endpoint: str | None = None,
        geyser_endpoint: str | None = None,
        geyser_api_token: str | None = None,
        geyser_auth_type: str = "x-token",
        pumpportal_url: str = "wss://pumpportal.fun/api/data",
        platforms: list[Platform] | None = None,
        # Patient listener config (only used when listener_type == "patient")
        patient_min_age_seconds: int = 300,
        patient_max_age_seconds: int = 1800,
        patient_scan_interval_seconds: int = 30,
        # Dexscreener trending listener config
        dex_poll_interval_seconds: int = 45,
        dex_min_age_seconds: int = 3600,
        dex_max_age_seconds: int = 86400,
        dex_min_liquidity_usd: float = 30_000.0,
        dex_min_volume_1h_usd: float = 5_000.0,
        dex_min_price_change_1h_pct: float = 0.0,
        dex_min_price_change_6h_pct: float = 0.0,
        dex_max_price_change_24h_pct: float = 2000.0,
        # PumpSwap new-pool listener config
        rpc_endpoint: str | None = None,
        newpool_min_post_creation_seconds: int = 30,
        newpool_max_post_creation_seconds: int = 600,
        newpool_min_quote_reserve_sol: float = 30.0,
    ) -> BaseTokenListener:
        """Create a token listener based on the specified type.

        Args:
            listener_type: Type of listener ('logs', 'blocks', 'geyser', or 'pumpportal')
            wss_endpoint: WebSocket endpoint URL (for logs/blocks listeners)
            geyser_endpoint: Geyser gRPC endpoint URL (for geyser listener)
            geyser_api_token: Geyser API token (for geyser listener)
            geyser_auth_type: Geyser authentication type
            pumpportal_url: PumpPortal WebSocket URL (for pumpportal listener)
            platforms: List of platforms to monitor (if None, monitor all)

        Returns:
            Configured token listener

        Raises:
            ValueError: If listener type is invalid or required parameters are missing
        """
        listener_type = listener_type.lower()

        if listener_type == "geyser":
            if not geyser_endpoint or not geyser_api_token:
                raise ValueError(
                    "Geyser endpoint and API token are required for geyser listener"
                )

            from monitoring.universal_geyser_listener import UniversalGeyserListener

            listener = UniversalGeyserListener(
                geyser_endpoint=geyser_endpoint,
                geyser_api_token=geyser_api_token,
                geyser_auth_type=geyser_auth_type,
                platforms=platforms,
            )
            logger.info("Created Universal Geyser listener for token monitoring")
            return listener

        elif listener_type == "logs":
            if not wss_endpoint:
                raise ValueError("WebSocket endpoint is required for logs listener")

            from monitoring.universal_logs_listener import UniversalLogsListener

            listener = UniversalLogsListener(
                wss_endpoint=wss_endpoint,
                platforms=platforms,
            )
            logger.info("Created Universal Logs listener for token monitoring")
            return listener

        elif listener_type == "blocks":
            if not wss_endpoint:
                raise ValueError("WebSocket endpoint is required for blocks listener")

            from monitoring.universal_block_listener import UniversalBlockListener

            listener = UniversalBlockListener(
                wss_endpoint=wss_endpoint,
                platforms=platforms,
            )
            logger.info("Created Universal Block listener for token monitoring")
            return listener

        elif listener_type == "pumpportal":
            # Import the new universal PumpPortal listener
            from monitoring.universal_pumpportal_listener import (
                UniversalPumpPortalListener,
            )

            # Validate that requested platforms support PumpPortal
            supported_pumpportal_platforms = [Platform.PUMP_FUN, Platform.LETS_BONK]

            if platforms:
                unsupported = [
                    p for p in platforms if p not in supported_pumpportal_platforms
                ]
                if unsupported:
                    logger.warning(
                        f"Platforms {[p.value for p in unsupported]} do not support PumpPortal"
                    )

                # Filter to only supported platforms
                filtered_platforms = [
                    p for p in platforms if p in supported_pumpportal_platforms
                ]
                if not filtered_platforms:
                    raise ValueError(
                        "No supported platforms specified for PumpPortal listener"
                    )
                platforms = filtered_platforms

            listener = UniversalPumpPortalListener(
                pumpportal_url=pumpportal_url,
                platforms=platforms,
            )
            logger.info(
                f"Created Universal PumpPortal listener for platforms: {[p.value for p in (platforms or supported_pumpportal_platforms)]}"
            )
            return listener

        elif listener_type == "migration":
            # Migration listener detects pump.fun bonding curves that just
            # graduated to PumpSwap. These tokens have $80K+ locked liquidity
            # by definition and trade on AMM, not bonding curve.
            if wss_endpoint is None:
                raise ValueError("wss_endpoint is required for 'migration' listener")
            from monitoring.universal_migration_listener import (
                UniversalMigrationListener,
            )

            listener = UniversalMigrationListener(
                wss_endpoint=wss_endpoint,
                platforms=platforms,
            )
            logger.info("Created Universal Migration listener")
            return listener

        elif listener_type == "patient":
            # Patient listener wraps the logs listener but defers callbacks
            # until tokens have aged into [min_age, max_age] window. Used by
            # patient_v1 strategy to buy mature tokens that survived launch.
            if wss_endpoint is None:
                raise ValueError("wss_endpoint is required for 'patient' listener")
            from monitoring.universal_patient_listener import UniversalPatientListener

            listener = UniversalPatientListener(
                wss_endpoint=wss_endpoint,
                platforms=platforms,
                patient_min_age_seconds=patient_min_age_seconds,
                patient_max_age_seconds=patient_max_age_seconds,
                scan_interval_seconds=patient_scan_interval_seconds,
            )
            logger.info(
                "Created Universal Patient listener (min_age=%ds max_age=%ds scan=%ds) "
                "for platforms: %s",
                patient_min_age_seconds,
                patient_max_age_seconds,
                patient_scan_interval_seconds,
                [p.value for p in (platforms or [])],
            )
            return listener

        elif listener_type == "dexscreener_trending":
            # Polls Dexscreener for Solana tokens with real momentum signals
            # (age, liquidity, rising volume, buy/sell ratio, price action).
            # No WSS endpoint needed — it's pure HTTP polling.
            from monitoring.dexscreener_trending_listener import (
                DexscreenerTrendingListener,
                TrendingFilters,
            )

            filters = TrendingFilters(
                min_age_seconds=dex_min_age_seconds,
                max_age_seconds=dex_max_age_seconds,
                min_liquidity_usd=dex_min_liquidity_usd,
                min_volume_1h_usd=dex_min_volume_1h_usd,
                min_price_change_1h_pct=dex_min_price_change_1h_pct,
                min_price_change_6h_pct=dex_min_price_change_6h_pct,
                max_price_change_24h_pct=dex_max_price_change_24h_pct,
            )
            listener = DexscreenerTrendingListener(
                platforms=platforms,
                poll_interval_seconds=dex_poll_interval_seconds,
                filters=filters,
            )
            logger.info(
                "Created Dexscreener trending listener "
                "(poll=%ds, age=%d-%ds, min_liq=$%.0f)",
                dex_poll_interval_seconds,
                dex_min_age_seconds,
                dex_max_age_seconds,
                dex_min_liquidity_usd,
            )
            return listener

        elif listener_type == "pumpswap_new_pool":
            # Catches PumpSwap pool creations in real time = pump.fun graduates
            # at hour 0, before Dexscreener trending picks them up. This is the
            # leading indicator the strategy needs.
            if wss_endpoint is None or rpc_endpoint is None:
                raise ValueError(
                    "wss_endpoint AND rpc_endpoint required for 'pumpswap_new_pool'"
                )
            from monitoring.pumpswap_new_pool_listener import (
                PumpSwapNewPoolListener,
                NewPoolFilters,
            )

            filters = NewPoolFilters(
                min_post_creation_seconds=newpool_min_post_creation_seconds,
                max_post_creation_seconds=newpool_max_post_creation_seconds,
                min_quote_reserve_sol=newpool_min_quote_reserve_sol,
            )
            listener = PumpSwapNewPoolListener(
                wss_endpoint=wss_endpoint,
                rpc_endpoint=rpc_endpoint,
                platforms=platforms,
                filters=filters,
            )
            logger.info(
                "Created PumpSwap new-pool listener (wait=%ds, min_quote=%.1f SOL)",
                newpool_min_post_creation_seconds, newpool_min_quote_reserve_sol,
            )
            return listener

        else:
            raise ValueError(
                f"Invalid listener type '{listener_type}'. "
                f"Must be one of: 'logs', 'blocks', 'geyser', 'pumpportal', "
                f"'patient', 'migration', 'dexscreener_trending', "
                f"'pumpswap_new_pool'"
            )

    @staticmethod
    def get_supported_listener_types() -> list[str]:
        """Get list of supported listener types.

        Returns:
            List of supported listener type strings
        """
        return [
            "logs",
            "blocks",
            "geyser",
            "pumpportal",
            "patient",
            "migration",
            "dexscreener_trending",
            "pumpswap_new_pool",
        ]

    @staticmethod
    def get_platform_compatible_listeners(platform: Platform) -> list[str]:
        """Get list of listener types compatible with a specific platform.

        Args:
            platform: Platform to check compatibility for

        Returns:
            List of compatible listener types
        """
        if platform == Platform.PUMP_FUN:
            return [
            "logs",
            "blocks",
            "geyser",
            "pumpportal",
            "patient",
            "migration",
            "dexscreener_trending",
            "pumpswap_new_pool",
        ]
        elif platform == Platform.LETS_BONK:
            return ["blocks", "geyser", "pumpportal"]  # Added pumpportal support
        else:
            return ["blocks", "geyser"]  # Default universal listeners

    @staticmethod
    def get_pumpportal_supported_platforms() -> list[Platform]:
        """Get list of platforms that support PumpPortal listener.

        Returns:
            List of platforms with PumpPortal support
        """
        return [Platform.PUMP_FUN, Platform.LETS_BONK]
