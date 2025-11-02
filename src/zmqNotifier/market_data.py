"""Market data parsing, validation, and routing with Pydantic models."""

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal

from .config import configure_logging
from .config import settings
from .market_data_logger import MarketDataLogger
from .models import MarketDataMessage
from .models import OHLCData
from .models import TickData

configure_logging()
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SameOHLCError(Exception):
    """Raised when identical OHLC bars occur consecutively beyond tolerance."""

    symbol: str
    timeframe: str
    count: int
    data: OHLCData


FLAT_BAR_THRESHOLD = 30


class MarketDataHandler:
    """
    Parse and validate raw market data from MT4 Vantage ZMQ connector.

    Converts raw tuples/dicts into type-safe Pydantic models (TickData, OHLCData).
    Routes validated messages to data logger for persistence.

    Attrs:
    ----
    brokertime_tz: Broker time zone offset in hours UTC, it is determined by
    algining olhc bar: {'BTCUSD_M1': {'2025-09-15 09:55:58.893002': (1757940840, 114898.16,
    114917.86, 114888.64, 114890.42, 66, 0, 0)}}
    datetime.datetime.fromtimestamp(1757940840- 3* 3600,datetime.UTC), which is closer to
    2025-09-15 09:55:58.893002. The vantage broker is UTC+3
    """

    def __init__(self, client):
        """
        Initialize market data handler.

        Args:
        ----
            client: ZmqMt4Client instance (parent)

        """
        self._client = client
        self._data_logger: MarketDataLogger = MarketDataLogger(settings.storage)
        self._flat_bar_counts: defaultdict[tuple[str, str], int] = defaultdict(int)
        self.brokertime_tz = settings.broker.brokertime_tz
        logger.debug("MarketDataHandler initialized with brokertime_tz=%s", self.brokertime_tz)

    def process(self, raw_data: dict) -> None:
        """
        Process raw market data from MT4 ZMQ connector.

        Args:
        ----
            raw_data: Dictionary from _Market_Data_DB with structure:
                      {'channel': {utc_time: data_tuple, ...}, ...}

        """
        if not raw_data:
            return

        data_logger = self._data_logger

        for channel, time_series in raw_data.items():
            try:
                messages = self._parse_channel(channel, time_series)
                if not messages:
                    logger.debug("No messages parsed for channel %s", channel)
                    continue

                for message in messages:
                    logger.debug(
                        "Processed message from %s: %s",
                        channel,
                        message.model_dump(mode="json", round_trip=True),
                    )

                    if isinstance(message.data, TickData):
                        data_logger.log_tick(message.symbol, message.data)
                    elif isinstance(message.data, OHLCData):
                        if message.timeframe:
                            data_logger.log_ohlc(message.symbol, message.timeframe, message.data)

            except SameOHLCError as exc:
                self._handle_flat_bar_exception(exc)
            except Exception:
                logger.exception("Failed to process channel: %s", channel)

        data_logger.maintenance()

    def shutdown(self) -> None:
        self._data_logger.shutdown()
        logger.info("MarketDataHandler shutdown complete")

    def _handle_flat_bar_exception(self, exc: SameOHLCError) -> None:
        """Handle consecutive flat OHLC bars by logging and unsubscribing."""
        key = (exc.symbol, exc.timeframe)
        self._flat_bar_counts.pop(key, None)

        logger.warning(
            "Detected %d consecutive flat OHLC bars for %s (%s); last bar=%s",
            exc.count,
            exc.symbol,
            exc.timeframe,
            exc.data.model_dump(mode="json", round_trip=True),
        )

        unsubscribe = getattr(self._client, "unsubscribe", None)
        if callable(unsubscribe):
            try:
                unsubscribe(exc.symbol)
                logger.info("Unsubscribed from %s after flat OHLC detection", exc.symbol)
            except Exception:
                logger.exception(
                    "Failed to unsubscribe from %s following flat OHLC detection", exc.symbol,
                )
        else:
            logger.error(
                "Client does not implement 'unsubscribe'; unable to act on flat OHLC for %s",
                exc.symbol,
            )

    def _parse_channel(self, channel: str, time_series: dict) -> list[MarketDataMessage]:
        """
        Parse channel and time series data into MarketDataMessage objects.

        Args:
        ----
            channel: Channel name (e.g., 'EURUSD' for tick, 'EURUSD_M1' for bar)
            time_series: Dict of {utc_time_str: data_tuple}

        Returns:
        -------
            List of validated MarketDataMessage objects

        """
        messages = []
        symbol, timeframe = self._parse_channel_name(channel)

        for utc_time_str, data_tuple in time_series.items():
            try:
                if timeframe is None:
                    # Tick data: (bid, ask)
                    msg = self._parse_tick(symbol, utc_time_str, data_tuple)
                else:
                    # OHLC bar data: (broker_time, O, H, L, C, V, ...)
                    msg = self._parse_ohlc(symbol, timeframe, utc_time_str, data_tuple)

                messages.append(msg)
            except SameOHLCError:
                raise
            except Exception:
                logger.exception(
                    "Failed to parse data for %s at %s: %s", channel, utc_time_str, data_tuple,
                )

        logger.debug("Parsed %d messages from channel %s", len(messages), channel)
        return messages

    def _parse_channel_name(self, channel: str) -> tuple[str, str | None]:
        parts = channel.split("_")
        if len(parts) == 1:
            # Tick data channel
            return parts[0], None
        # OHLC channel
        return parts[0], parts[1]

    def _parse_tick(self, symbol: str, utc_time_str: str, data_tuple: tuple) -> MarketDataMessage:
        """
        Parse tick data tuple into MarketDataMessage with TickData.

        Args:
        ----
            symbol: Trading symbol
            utc_time_str: UTC timestamp string (e.g., '2025-10-09 05:52:24.825553')
            data_tuple: (bid, ask)

        Returns:
        -------
            Validated MarketDataMessage with TickData

        """
        bid, ask = data_tuple[:2]  # Ignore any extra fields
        dt = datetime.fromisoformat(utc_time_str).replace(tzinfo=UTC)

        tick_data = TickData(datetime=dt, bid=Decimal(str(bid)), ask=Decimal(str(ask)))

        return MarketDataMessage(symbol=symbol, timeframe=None, data=tick_data)

    def _parse_ohlc(
        self, symbol: str, timeframe: str, utc_time_str: str, data_tuple: tuple,
    ) -> MarketDataMessage:
        """
        Parse OHLC bar data tuple into MarketDataMessage with OHLCData.

        Args:
        ----
            symbol: Trading symbol
            timeframe: Timeframe code (e.g., 'M1', 'H1')
            utc_time_str: UTC timestamp string
            data_tuple: (broker_time, open, high, low, close, volume, ...)

        Returns:
        -------
            Validated MarketDataMessage with OHLCData

        """
        # MT4 data format: (broker_time_seconds, O, H, L, C, V, ...)
        broker_time, open_price, high, low, close, volume = data_tuple[:6]

        # Apply timezone offset to convert broker time to UTC
        utc_timestamp = broker_time - (self.brokertime_tz * 3600)
        dt = datetime.fromtimestamp(utc_timestamp, tz=UTC)

        ohlc_data = OHLCData(
            datetime=dt,
            open=Decimal(str(open_price)),
            high=Decimal(str(high)),
            low=Decimal(str(low)),
            close=Decimal(str(close)),
            volume=int(volume),
        )

        self._check_flat_ohlc(symbol, timeframe, ohlc_data)

        return MarketDataMessage(symbol=symbol, timeframe=timeframe, data=ohlc_data)

    def _check_flat_ohlc(self, symbol: str, timeframe: str, ohlc: OHLCData) -> None:
        """Track consecutive OHLC bars where open/high/low/close are identical."""
        key = (symbol, timeframe)

        if ohlc.open == ohlc.high and ohlc.open == ohlc.low and ohlc.open == ohlc.close:
            self._flat_bar_counts[key] += 1
            logger.debug(
                "Flat OHLC detected for %s (%s) - streak=%d",
                symbol,
                timeframe,
                self._flat_bar_counts[key],
            )
        else:
            self._flat_bar_counts[key] = 0
            return

        if self._flat_bar_counts[key] > FLAT_BAR_THRESHOLD:
            raise SameOHLCError(symbol, timeframe, self._flat_bar_counts[key], ohlc)

# Timeframe code to minutes mapping
# TODO such is globally used, put into zmqNotifier __init__.py instead
TIMEFRAME_MINUTES = {
    "M1": 1, "M5": 5, "M15": 15, "M30": 30, "H1": 60,
    "H4": 240, "D1": 1440, "W1": 10080, "MN": 43200,
}

def get_timeframe_minutes(timeframe: str) -> int:
    if timeframe not in TIMEFRAME_MINUTES:
        msg = f"Unsupported timeframe: {timeframe}"
        raise ValueError(msg)
    return TIMEFRAME_MINUTES[timeframe]


def validate_timeframe(timeframe: str) -> None:
    if timeframe not in settings.validation.supported_timeframes:
        msg = f"Invalid timeframe: {timeframe}"
        raise ValueError(msg)


def validate_symbol(symbol: str) -> None:
    if symbol not in settings.validation.supported_symbols:
        msg = f"Unsupported symbol: {symbol}"
        raise ValueError(msg)
