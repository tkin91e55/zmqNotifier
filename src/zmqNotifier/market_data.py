"""Market data parsing, validation, and routing with Pydantic models."""

import logging
from datetime import UTC
from datetime import datetime
from decimal import Decimal

from .config import settings
from .models import MarketDataMessage
from .models import OHLCData
from .models import TickData

logger = logging.getLogger(__name__)


# Timeframe code to minutes mapping
TIMEFRAME_MINUTES = {
    "M1": 1,
    "M5": 5,
    "M15": 15,
    "M30": 30,
    "H1": 60,
    "H4": 240,
    "D1": 1440,
    "W1": 10080,
    "MN": 43200,
}


class MarketDataHandler:
    """
    Parse and validate raw market data from MT4 ZMQ connector.

    Converts raw tuples/dicts into type-safe Pydantic models (TickData, OHLCData).
    TODO Routes validated messages to registered handlers (loggers, notifiers, etc.).
    """

    def __init__(self, client):
        """
        Initialize market data handler.

        Args:
        ----
            client: ZmqMt4Client instance (parent)

        """
        self._client = client
        logger.info("MarketDataHandler initialized")

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

        for channel, time_series in raw_data.items():
            try:
                messages = self._parse_channel(channel, time_series)
                # TODO this is where to deal with logger, notifiers etc
                print(messages)
            except Exception:
                logger.exception("Failed to process channel: %s", channel)

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
            except Exception:
                logger.exception(
                    "Failed to parse data for %s at %s: %s",
                    channel,
                    utc_time_str,
                    data_tuple,
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

    def _parse_tick(
        self,
        symbol: str,
        utc_time_str: str,
        data_tuple: tuple,
    ) -> MarketDataMessage:
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

        tick_data = TickData(
            datetime=dt,
            bid=Decimal(str(bid)),
            ask=Decimal(str(ask)),
        )

        return MarketDataMessage(
            symbol=symbol,
            timeframe=None,
            data=tick_data,
        )

    def _parse_ohlc(
        self,
        symbol: str,
        timeframe: str,
        utc_time_str: str,
        data_tuple: tuple,
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

        # Use broker time for the bar timestamp (more accurate than UTC string)
        dt = datetime.fromtimestamp(broker_time, tz=UTC)

        ohlc_data = OHLCData(
            datetime=dt,
            open=Decimal(str(open_price)),
            high=Decimal(str(high)),
            low=Decimal(str(low)),
            close=Decimal(str(close)),
            volume=int(volume),
        )

        return MarketDataMessage(
            symbol=symbol,
            timeframe=timeframe,
            data=ohlc_data,
        )

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
