"""Market data logging with pluggable storage backends."""

import logging
import zipfile
from collections import defaultdict
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from io import StringIO
from pathlib import Path

from cachetools import TTLCache
from cachetools import cached

from .abstract_class import StorageBackend
from .config import StorageSettings
from .models import OHLCData
from .models import TickData

logger = logging.getLogger(__name__)


def _extract_year_month(date_str: str) -> str:
    """Extract YYYY_MM from YYYY-MM-DD date string."""
    return date_str[:7].replace("-", "_")


@cached(cache=TTLCache(maxsize=1024, ttl=1))
def _get_current_date() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _get_next_flush_time(flush_interval_minutes) -> datetime:
    utc = datetime.now(UTC)
    flush_interval = timedelta(minutes=flush_interval_minutes)
    return utc + flush_interval


def _compress_csv(root_path: Path, current_utc: datetime) -> None:
    last_month = current_utc.replace(day=1) - timedelta(days=1)
    year_month_dir = last_month.strftime("%Y_%m")
    year_month_archive = last_month.strftime("%Y-%m")

    for symbol_dir in (p for p in root_path.iterdir() if p.is_dir()):
        month_dir = symbol_dir / year_month_dir
        if not month_dir.exists():
            continue

        files_by_type: dict[str, list[Path]] = defaultdict(list)
        for csv_path in month_dir.glob("*.csv"):
            parts = csv_path.stem.split("_")
            if len(parts) >= 3:
                files_by_type[parts[1]].append(csv_path)

        if not files_by_type:
            logger.info("No CSV files to compress in %s", month_dir)
            continue

        for data_type, files in files_by_type.items():
            archive_name = f"{symbol_dir.name}_{data_type}_{year_month_archive}.zip"
            archive_path = symbol_dir / archive_name

            if archive_path.exists():
                logger.info("Archive %s already exists, skipping", archive_name)
                continue

            with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for csv_file in files:
                    zipf.write(csv_file, csv_file.name)

            for csv_file in files:
                csv_file.unlink()
                logger.info("Compressed and removed: %s", csv_file)

            logger.info("Created archive %s with %d files", archive_name, len(files))

        if month_dir.exists() and not any(month_dir.iterdir()):
            month_dir.rmdir()
            logger.info("Removed empty directory: %s", month_dir)


def _clean_old_archives(root_path: Path, retention_days: int, current_utc: datetime) -> None:
    cutoff_date = current_utc - timedelta(days=retention_days)

    def _archive_date(path: Path) -> datetime | None:
        parts = path.stem.split("_")
        if len(parts) < 3:
            return None
        try:
            return datetime.strptime(parts[-1], "%Y-%m").replace(tzinfo=UTC)
        except ValueError:
            return None

    deletions = [
        zip_path
        for zip_path in root_path.rglob("*.zip")
        if (archive_dt := _archive_date(zip_path)) and archive_dt < cutoff_date
    ]

    for zip_path in deletions:
        try:
            zip_path.unlink()
            logger.info("Deleted old ZIP archive: %s", zip_path)
        except Exception as exc:  # pragma: no cover - unlikely IO failure
            logger.warning("Failed to delete %s during cleanup: %s", zip_path, exc)

    if deletions:
        logger.info(
            "Cleanup complete: removed %d ZIP archives older than %d days",
            len(deletions),
            retention_days,
        )


class CSVStorageBackend(StorageBackend):
    """
    CSV-based storage backend with UTC-based rotation.

    1. Logging: csv files are organized under data/symbol/YYYY_MM/, e.g., data/BTCUSD/2024_06/
       named as {symbol}_{type}_{date}.csv, e.g., BTCUSD_tick_2024-06-25.csv
    2. Compression: monthly zip archives per symbol, e.g., data/BTCUSD/{symbol}_{type}_{yyyy-mm}.zip
       Compression triggers when a new month starts.
       When compressed, the original csv files are deleted.
    3. Cleanup: delete the compressed .zip older than retention period (e.g., 180 days).

    Attributes
    ----------
        data_path: Root directory for CSV files.
        _buffers: In-memory StringIO buffers keyed by "{symbol}_{type}_{date}".
                  where type are tick/M1/M5/....

    """

    def __init__(self, data_path: Path):
        self.data_path = data_path
        self.data_path.mkdir(parents=True, exist_ok=True)
        self._buffers: dict[str, StringIO] = {}

    def _make_buffer_key(self, symbol: str, data_type: str, date: str) -> str:
        """
        Generate buffer key: {symbol}_{data_type}_{date}.

        data_type: tick/M1/M5...
        """
        return f"{symbol}_{data_type}_{date}"

    def _ensure_buffer(self, key: str, header: str) -> StringIO:
        """Get or create buffer with header."""
        if key not in self._buffers:
            self._buffers[key] = StringIO()
            self._buffers[key].write(header)
        return self._buffers[key]

    def log_tick(self, symbol: str, tick: TickData) -> None:
        """Log tick data to CSV buffer."""
        current_date = _get_current_date()
        key = self._make_buffer_key(symbol, "tick", current_date)
        buffer = self._ensure_buffer(key, "datetime,bid,ask\n")
        buffer.write(f"{tick.datetime!s},{tick.bid},{tick.ask}\n")

    def log_ohlc(self, symbol: str, timeframe: str, ohlc: OHLCData) -> None:
        """Log OHLC bar data to CSV buffer."""
        current_date = _get_current_date()
        key = self._make_buffer_key(symbol, timeframe, current_date)
        buffer = self._ensure_buffer(key, "datetime,open,high,low,close,volume\n")
        buffer.write(
            f"{ohlc.datetime!s},{ohlc.open},{ohlc.high},{ohlc.low},{ohlc.close},{ohlc.volume}\n",
        )

    def flush(self) -> None:
        """Write buffered data to CSV files organized by symbol/YYYY_MM/."""
        for key, buffer in self._buffers.items():
            if buffer.tell() == 0:
                continue

            parts = key.rsplit("_", 1)
            symbol_type = parts[0]
            date_str = parts[1]
            year_month = _extract_year_month(date_str)

            symbol = symbol_type.split("_")[0]
            file_dir = self.data_path / symbol / year_month
            file_dir.mkdir(parents=True, exist_ok=True)

            filepath = file_dir / f"{key}.csv"
            buffer.seek(0)
            content = buffer.read()

            if filepath.exists() and content.startswith("datetime"):
                content = content.split("\n", 1)[1] if "\n" in content else ""

            with filepath.open("a" if filepath.exists() else "w", encoding="utf-8") as f:
                f.write(content)

            buffer.seek(0)
            buffer.truncate()

    def rotate(self, current_date: str) -> None:
        """
        Rotate to new date-based files.

        Flush yesterday buffers and clear the _buffers with many keys.
        """
        self.flush()
        self._buffers.clear()
        logger.info("Rotated CSV files to date: %s", current_date)

    def cleanup(self, retention_days: int, current_utc: datetime) -> None:
        """
        Delete compressed ZIP archives older than retention period.

        Archives are named {symbol}_{type}_{YYYY-MM}.zip under data/symbol/.
        """
        _clean_old_archives(self.data_path, retention_days, current_utc)

    def compress(self, current_utc: datetime) -> None:
        """
        Compress last month's CSV files into monthly archives per symbol.

        Archives are named {symbol}_{type}_{YYYY-MM}.zip under data/symbol/
        Example: data/BTCUSD/BTCUSD_tick_2024-06.zip.
        """
        _compress_csv(self.data_path, current_utc)


class MarketDataLogger:
    """
    Orchestrator for market data logging with pluggable backends.

    Attributes
    ----------
        settings: Storage configuration settings.
        backend: Active storage backend instance (e.g., CSVStorageBackend).
        _next_flush: Timestamp for next flush operation.
        _last_maintenance_date: UTC date string (YYYY-MM-DD) of last maintenance run.

    """

    def __init__(self, storage_settings: StorageSettings):
        self.settings = storage_settings
        self.backend = self._create_backend()
        self._next_flush = self._update_next_flush()
        self._last_maintenance_date: str = _get_current_date()

    def _create_backend(self) -> StorageBackend:
        from .config import StorageBackend as StorageBackendEnum

        if self.settings.backend == StorageBackendEnum.CSV:
            return CSVStorageBackend(self.settings.data_path)
        msg = f"Unsupported storage backend: {self.settings.backend}"
        raise ValueError(msg)

    def log_tick(self, symbol: str, tick: TickData) -> None:
        """Log tick data via storage backend."""
        self.backend.log_tick(symbol, tick)
        self._poll_flush()

    def log_ohlc(self, symbol: str, timeframe: str, ohlc: OHLCData) -> None:
        """Log OHLC data via storage backend."""
        self.backend.log_ohlc(symbol, timeframe, ohlc)
        self._poll_flush()

    def _update_next_flush(self) -> datetime:
        interval = self.settings.flush_interval_minutes
        return _get_next_flush_time(interval)

    def _poll_flush(self) -> None:
        """Flush buffers if interval has elapsed."""
        utc_now = datetime.now(UTC)

        if utc_now >= self._next_flush:
            self.backend.flush()
            self._next_flush = self._update_next_flush()

    def maintenance(self) -> None:
        """Run periodic maintenance tasks."""
        current_date = _get_current_date()

        if current_date != self._last_maintenance_date:
            utc_now = datetime.now(UTC)
            self.backend.rotate(current_date)
            if self.settings.compression_enabled:
                self.backend.compress(utc_now)
            self.backend.cleanup(self.settings.retention_days, utc_now)
            self._last_maintenance_date = current_date

    def shutdown(self) -> None:
        """Flush and cleanup before shutdown."""
        self.backend.flush()
        logger.info("MarketDataLogger shutdown complete")
