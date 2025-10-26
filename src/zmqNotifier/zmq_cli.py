"""
For development with ipython autoreload:
    %load_ext autoreload
    %autoreload 2
"""

import logging

from .DWX_ZeroMQ_Connector_v2_0_1_RC8 import DWX_ZeroMQ_Connector
from .config import configure_logging
from .market_data import MarketDataHandler
from .market_data import get_timeframe_minutes
from .market_data import validate_symbol
from .market_data import validate_timeframe

LOGGING_SETTINGS = configure_logging()
logger = logging.getLogger(__name__)


class ZmqMt4Client(DWX_ZeroMQ_Connector):
    """
    ZeroMQ client for MT4 market data streaming with clean architecture.

    Usage:
        from zmqNotifier.zmq_cli import ZmqMt4Client

        client = ZmqMt4Client(_host='10.211.55.4')
        client.subscribe('EURUSD')
        client.track_bars('EURUSD', ['M1', 'M5'])
        client.track_ticks('EURUSD')

        # When done
        client.shutdown()


    Delegates to specialized handlers:
    - MarketDataHandler: Parse and validate raw data
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize ZMQ MT4 client.

        Args:
        ----
            *args: Passed to DWX_ZeroMQ_Connector
            **kwargs: Passed to DWX_ZeroMQ_Connector

        """
        super().__init__(*args, **kwargs)

        # Core components
        self.data_handler = MarketDataHandler(self)

        # DWX MT4 handler Register data callback, and on_data_recv()
        # will refer to the data_handler
        self._subdata_handlers.append(self.on_data_recv)

        # Settings
        self._verbose = True
        logger.debug(
            "ZmqMt4Client logger initialised at level %s; log file: %s",
            LOGGING_SETTINGS.level.upper(),
            LOGGING_SETTINGS.log_dir / LOGGING_SETTINGS.file_name,
        )

        logger.info("ZmqMt4Client initialized")

    @property
    def is_active(self) -> bool:
        """Check if DWX MT4 client is active."""
        return self._ACTIVE

    @property
    def verbose(self) -> bool:
        """Get DWX MT4 verbose mode status."""
        return self._verbose

    @verbose.setter
    def verbose(self, value: bool) -> None:
        """Set DWX MT4 verbose mode."""
        self._verbose = value

    @property
    def market_data(self) -> dict:
        """
        Access raw market data buffer.

        Returns
        -------
            Dictionary with structure:
            {
                'symbol': {utc_time_key: values},        # tick data
                'symbol_tf': {utc_time_key: values},     # bar data
            }

        """
        return self._Market_Data_DB

    def subscribe(
        self,
        symbol: str,
        *,
        for_tickdata: bool = False,
        for_ohlc_m1: bool = False,
    ) -> None:
        """
        Subscribe to market data channel for a symbol.

        Creates a channel on the MT4 ZMQ server. After subscribing, explicitly
        request tick data and/or OHLC data using track_ticks() and track_bars().

        Args:
        ----
            symbol: Trading symbol (e.g., 'EURUSD', 'BTCUSD')
            for_tickdata: Also request tick data
            for_ohlc_m1: Also request M1 bars

        """
        validate_symbol(symbol)
        self._DWX_MTX_SUBSCRIBE_MARKETDATA_(symbol)
        logger.info("Subscribed to %s", symbol)

        if for_tickdata:
            self.track_ticks(symbol)

        if for_ohlc_m1:
            self.track_bars(symbol, ["M1"])

    def unsubscribe(self, symbol: str) -> None:
        """
        Unsubscribe from market data channel.

        Note: There is no way to unsubscribe from specific sub-channels
        (tick data or specific timeframes). Must restart MT4 server or
        unsubscribe completely from the symbol.

        Args:
        ----
            symbol: Trading symbol to unsubscribe from

        """
        self._DWX_MTX_UNSUBSCRIBE_MARKETDATA_(symbol)
        logger.info("Unsubscribed from %s", symbol)

    def track_ticks(self, symbol: str) -> None:
        """
        Request tick data for a subscribed symbol.

        Must call subscribe() first.

        Args:
        ----
            symbol: Trading symbol

        """
        validate_symbol(symbol)
        self._DWX_MTX_SEND_TRACKPRICES_REQUEST_([symbol])
        logger.info("Tracking ticks for %s", symbol)

    def track_bars(self, symbol: str, timeframes: list[str] | None = None) -> None:
        """
        Request OHLC bar data for specific timeframes.

        Must call subscribe() first.

        Args:
        ----
            symbol: Trading symbol
            timeframes: List of timeframe codes (e.g., ['M1', 'M5', 'H1'])
                       If None or empty, does nothing.

        """
        if not timeframes:
            return

        validate_symbol(symbol)

        # Build subscription channels
        sub_channels = []
        for tf in timeframes:
            validate_timeframe(tf)
            minutes = get_timeframe_minutes(tf)
            # Format: (channel_name, symbol, timeframe_in_minutes)
            sub_channels.append((f"{symbol}_{tf}", symbol, minutes))

        self._DWX_MTX_SEND_TRACKRATES_REQUEST_(sub_channels)
        logger.info("Tracking rates for %s: %s", symbol, timeframes)

    def on_data_recv(self, _) -> None:
        """
        Process market data received from MT4 ZMQ server.

        This runs in a separate thread with a polling loop (poll_delay=0.001s).
        Raw data is stored in self._Market_Data_DB before this callback.

        Data format:
        - Tick: {'2025-10-09 05:52:24.825553': (bid, ask)}
        - Bar: {'2025-10-09 06:32:00.259239': (broker_time, O, H, L, C, V, ...)}

        The parsed data is stored in self._Market_Data_DB, an entry is like this:
        {'2025-10-09 06:32:00.259239':
            (1760002260, 122116.68, 122179.17, 122115.79, 122170.36, 89, 0, 0)}
        where the key is UTC time by Mao, and 1760002260 is the broker time in seconds, and
        OHLCV follows.

        Or tick data:
        {'2025-10-09 05:52:24.825553': (121989.53, 122006.49)}

        Args:
        ----
            _: Unused parameter (required by parent class)

        """
        if not self._Market_Data_DB:
            logger.warning("on_data_recv called with empty market data")
            return

        try:
            # Delegate to handler for parsing, validation, and routing
            self.data_handler.process(self._Market_Data_DB)
        except Exception:
            logger.exception("Failed to process market data")
        finally:
            # Always clear the buffer
            self._Market_Data_DB.clear()

    def shutdown(self) -> None:
        """Shutdown the client and clean up resources."""
        logger.info("Shutting down ZmqMt4Client")

        self._DWX_ZMQ_SHUTDOWN_()
        logger.info("ZmqMt4Client shutdown complete")
