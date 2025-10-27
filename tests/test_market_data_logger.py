"""Tests for market_data_logger module."""

import shutil
import zipfile
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from zmqNotifier.config import StorageBackend
from zmqNotifier.config import StorageSettings
from zmqNotifier.market_data_logger import CSVStorageBackend
from zmqNotifier.market_data_logger import MarketDataLogger
from zmqNotifier.models import OHLCData
from zmqNotifier.models import TickData


@pytest.fixture()
def test_data_path(request):
    """
    Create persistent test_data directory per test for inspection.
    After running tests, a folder created undert tests/test_data/<TestClass>/<test_method>
    """
    test_name = request.node.name
    test_class = request.cls.__name__ if request.cls else "standalone"
    data_path = Path(__file__).parent / "test_data" / test_class / test_name
    if data_path.exists():
        shutil.rmtree(data_path)
    data_path.mkdir(parents=True)
    return data_path


@pytest.fixture()
def storage_settings(test_data_path):
    """Create storage settings with test data path."""
    return StorageSettings(
        data_path=test_data_path,
        backend=StorageBackend.CSV,
        compression_enabled=True,
        retention_days=180,
        flush_interval_minutes=5,
    )


@pytest.fixture()
def csv_backend(test_data_path):
    """Create CSV storage backend."""
    return CSVStorageBackend(test_data_path)


@pytest.fixture()
def sample_tick_data():
    """Standard tick data for testing."""
    return TickData(
        datetime=datetime.now(UTC),
        bid=Decimal("1.1000"),
        ask=Decimal("1.1002"),
    )


@pytest.fixture()
def sample_ohlc_data():
    """Standard OHLC data for testing."""
    return OHLCData(
        datetime=datetime.now(UTC),
        open=Decimal("1.1000"),
        high=Decimal("1.1100"),
        low=Decimal("1.0900"),
        close=Decimal("1.1050"),
        volume=1000,
    )


class TestCSVStorageBackend:
    """Test CSV storage backend."""

    def test_init(self, test_data_path):
        """Test backend initialization."""
        backend = CSVStorageBackend(test_data_path)
        assert backend.data_path == test_data_path
        assert test_data_path.exists()
        assert backend._buffers == {}

    def test_log_tick(self, csv_backend, sample_tick_data):
        """Test logging tick data."""
        csv_backend.log_tick("BTCUSD", sample_tick_data)

        current_date = datetime.now(UTC).strftime("%Y-%m-%d")
        key = f"BTCUSD_tick_{current_date}"
        assert key in csv_backend._buffers
        buffer_content = csv_backend._buffers[key].getvalue()
        assert "datetime,bid,ask" in buffer_content
        assert "1.1000,1.1002" in buffer_content

    def test_log_ohlc(self, csv_backend, sample_ohlc_data):
        """Test logging OHLC data."""
        csv_backend.log_ohlc("BTCUSD", "M1", sample_ohlc_data)

        current_date = datetime.now(UTC).strftime("%Y-%m-%d")
        key = f"BTCUSD_M1_{current_date}"
        assert key in csv_backend._buffers
        buffer_content = csv_backend._buffers[key].getvalue()
        assert "datetime,open,high,low,close,volume" in buffer_content
        assert "1.1000,1.1100,1.0900,1.1050,1000" in buffer_content

    def test_flush_tick(self, csv_backend, test_data_path, sample_tick_data):
        """Test flushing buffers to files."""
        csv_backend.log_tick("BTCUSD", sample_tick_data)
        csv_backend.flush()

        current_date = datetime.now(UTC).strftime("%Y-%m-%d")
        year_month = current_date[:7].replace("-", "_")
        csv_file = test_data_path / "BTCUSD" / year_month / f"BTCUSD_tick_{current_date}.csv"
        assert csv_file.exists()

        content = csv_file.read_text()
        assert "datetime,bid,ask" in content
        assert "1.1000,1.1002" in content

    def test_flush_ohlc(self, csv_backend, test_data_path, sample_ohlc_data):
        """Test logging OHLC data."""
        csv_backend.log_ohlc("BTCUSD", "M1", sample_ohlc_data)
        csv_backend.flush()

        current_date = datetime.now(UTC).strftime("%Y-%m-%d")
        year_month = current_date[:7].replace("-", "_")
        csv_file = test_data_path / "BTCUSD" / year_month / f"BTCUSD_M1_{current_date}.csv"
        assert csv_file.exists()

        content = csv_file.read_text()
        assert "datetime,open,high,low,close,volume" in content
        assert "1.1000,1.1100,1.0900,1.1050,1000" in content

    def test_compress_monthly(self, csv_backend, test_data_path):
        """Test monthly compression per symbol."""
        btc_dir = test_data_path / "BTCUSD" / "2025_09"
        btc_dir.mkdir(parents=True, exist_ok=True)

        csv_file1 = btc_dir / "BTCUSD_tick_2025-09-15.csv"
        csv_file1.write_text("datetime,bid,ask\n")

        csv_file2 = btc_dir / "BTCUSD_tick_2025-09-20.csv"
        csv_file2.write_text("datetime,bid,ask\n")

        csv_file3 = btc_dir / "BTCUSD_M1_2025-09-15.csv"
        csv_file3.write_text("datetime,open,high,low,close,volume\n")

        current_utc = datetime(2025, 10, 1, 12, 0, 0, tzinfo=UTC)
        csv_backend.compress(current_utc)

        tick_archive = test_data_path / "BTCUSD" / "BTCUSD_tick_2025-09.zip"
        m1_archive = test_data_path / "BTCUSD" / "BTCUSD_M1_2025-09.zip"

        assert tick_archive.exists()
        assert m1_archive.exists()
        assert not csv_file1.exists()
        assert not csv_file2.exists()
        assert not csv_file3.exists()

        with zipfile.ZipFile(tick_archive, "r") as zipf:
            names = zipf.namelist()
            assert "BTCUSD_tick_2025-09-15.csv" in names
            assert "BTCUSD_tick_2025-09-20.csv" in names

        with zipfile.ZipFile(m1_archive, "r") as zipf:
            names = zipf.namelist()
            assert "BTCUSD_M1_2025-09-15.csv" in names

    def test_rotate(self, csv_backend, sample_tick_data):
        """Test rotation to new date."""
        csv_backend.log_tick("BTCUSD", sample_tick_data)
        assert len(csv_backend._buffers) == 1

        utc_now = datetime(2025, 10, 26, 12, 0, 0, tzinfo=UTC).strftime("%Y-%m-%d")
        with patch("zmqNotifier.market_data_logger.CSVStorageBackend.flush") as mock_flush:
            csv_backend.rotate(utc_now)
            assert len(csv_backend._buffers) == 0
            assert mock_flush.call_count == 1

        csv_backend.log_tick("BTCUSD", sample_tick_data)
        assert len(csv_backend._buffers) == 1

        utc_tomorrow = datetime(2025, 10, 27, 12, 0, 0, tzinfo=UTC).strftime("%Y-%m-%d")
        csv_backend.rotate(utc_tomorrow)
        assert len(csv_backend._buffers) == 0

    def test_cleanup(self, csv_backend, test_data_path):
        """Test cleanup of old ZIP archives."""
        btc_dir = test_data_path / "BTCUSD"
        btc_dir.mkdir(parents=True, exist_ok=True)

        old_archive = btc_dir / "BTCUSD_tick_2025-04.zip"
        old_archive.write_text("dummy zip content")

        recent_archive = btc_dir / "BTCUSD_tick_2025-10.zip"
        recent_archive.write_text("dummy zip content")

        current_utc = datetime(2025, 10, 26, tzinfo=UTC)
        csv_backend.cleanup(retention_days=180, current_utc=current_utc)

        assert not old_archive.exists()
        assert recent_archive.exists()


class TestMarketDataLogger:
    """Test MarketDataLogger orchestrator."""

    def test_init(self, storage_settings):
        """Test logger initialization."""
        logger = MarketDataLogger(storage_settings)
        assert logger.settings == storage_settings
        assert isinstance(logger.backend, CSVStorageBackend)

    def test_log_tick(self, storage_settings, sample_tick_data):
        """Test logging tick data."""
        logger = MarketDataLogger(storage_settings)
        logger.log_tick("BTCUSD", sample_tick_data)

        current_date = datetime.now(UTC).strftime("%Y-%m-%d")
        key = f"BTCUSD_tick_{current_date}"
        assert key in logger.backend._buffers

    def test_log_ohlc(self, storage_settings, sample_ohlc_data):
        """Test logging OHLC data."""
        logger = MarketDataLogger(storage_settings)
        logger.log_ohlc("BTCUSD", "M1", sample_ohlc_data)

        current_date = datetime.now(UTC).strftime("%Y-%m-%d")
        key = f"BTCUSD_M1_{current_date}"
        assert key in logger.backend._buffers

    def test_maintenance(self, storage_settings, monkeypatch):
        """Test maintenance task execution."""
        logger = MarketDataLogger(storage_settings)

        logger._last_flush = datetime.now(UTC) - timedelta(minutes=10)
        logger._last_maintenance_date = (datetime.now(UTC) - timedelta(days=1)).strftime(
            "%Y-%m-%d",
        )

        flush_called = False
        rotate_called = False
        cleanup_called = False
        compress_called = False

        original_flush = logger.backend.flush
        original_rotate = logger.backend.rotate
        original_cleanup = logger.backend.cleanup
        original_compress = logger.backend.compress

        def mock_flush():
            nonlocal flush_called
            flush_called = True
            original_flush()

        def mock_rotate(*args):
            nonlocal rotate_called
            rotate_called = True
            original_rotate(*args)

        def mock_cleanup(*args):
            nonlocal cleanup_called
            cleanup_called = True
            original_cleanup(*args)

        def mock_compress(*args):
            nonlocal compress_called
            compress_called = True
            original_compress(*args)

        monkeypatch.setattr(logger.backend, "flush", mock_flush)
        monkeypatch.setattr(logger.backend, "rotate", mock_rotate)
        monkeypatch.setattr(logger.backend, "cleanup", mock_cleanup)
        monkeypatch.setattr(logger.backend, "compress", mock_compress)

        logger.maintenance()

        assert flush_called
        assert rotate_called
        assert cleanup_called
        assert compress_called

        # check that the self._last_maintenance_date is updated
        rotate_called = False
        logger.maintenance()
        assert not rotate_called

    def test_shutdown(self, storage_settings, test_data_path, sample_tick_data):
        """Test logger shutdown."""
        logger = MarketDataLogger(storage_settings)
        logger.log_tick("BTCUSD", sample_tick_data)
        logger.shutdown()

        current_date = datetime.now(UTC).strftime("%Y-%m-%d")
        year_month = current_date[:7].replace("-", "_")
        csv_file = test_data_path / "BTCUSD" / year_month / f"BTCUSD_tick_{current_date}.csv"
        assert csv_file.exists()

    def test_unsupported_backend(self, test_data_path):
        """Test error on unsupported backend."""
        settings = StorageSettings(
            data_path=test_data_path,
            backend=StorageBackend.SQLITE,
        )
        with pytest.raises(ValueError, match="Unsupported storage backend"):
            MarketDataLogger(settings)
