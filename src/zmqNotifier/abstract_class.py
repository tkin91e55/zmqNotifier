from abc import ABC
from abc import abstractmethod
from datetime import datetime

from .models import OHLCData
from .models import TickData


class StorageBackend(ABC):
    """Abstract base class for market data storage backends."""

    @abstractmethod
    def log_tick(self, symbol: str, tick: TickData) -> None:
        """Log tick data for a symbol."""
        ...

    @abstractmethod
    def log_ohlc(self, symbol: str, timeframe: str, ohlc: OHLCData) -> None:
        """Log OHLC bar data for a symbol and timeframe."""
        ...

    @abstractmethod
    def flush(self) -> None:
        """Flush any buffered data to storage."""
        ...

    @abstractmethod
    def rotate(self, current_date: str) -> None:
        """Perform rotation operations (daily file rotation, etc.)."""
        ...

    @abstractmethod
    def cleanup(self, retention_days: int, current_utc: datetime) -> None:
        """Remove data older than retention period."""
        ...

    @abstractmethod
    def compress(self, current_utc: datetime) -> None:
        """Compress old data files."""
        ...
