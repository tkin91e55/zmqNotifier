# ruff: noqa: SLF001
"""Tests for market data parsing and validation."""

import os
os.environ["ZMQ_NOTIFIER_LOGGING__LEVEL"] = "DEBUG" # some test require debug logging
from decimal import Decimal

import pytest

from fixtures.mock_data import mock_multiple_ohlc
from fixtures.mock_data import mock_multiple_ticks
from fixtures.mock_data import mock_ohlc_data
from fixtures.mock_data import mock_tick_data
from zmqNotifier.market_data import FLAT_BAR_THRESHOLD
from zmqNotifier.market_data import MarketDataHandler
from zmqNotifier.models import OHLCData
from zmqNotifier.models import TickData


@pytest.fixture()
def handler():
    """Create MarketDataHandler with mock client."""

    class MockClient:
        def __init__(self):
            self.unsubscribed: list[str] = []

        def unsubscribe(self, symbol: str) -> None:
            self.unsubscribed.append(symbol)

    mock_client = MockClient()
    return MarketDataHandler(mock_client)


class TestTickDataParsing:
    """Test parsing tick data."""

    def test_parse_valid_tick_data(self, handler):
        """Test parsing valid tick data."""
        raw_data = mock_tick_data()
        symbol, time_series = raw_data.popitem()
        bid, ask = next(iter(time_series.values()))

        messages = handler._parse_channel(symbol, time_series)

        assert len(messages) == 1
        msg = messages[0]
        assert msg.symbol == "BTCUSD"
        assert msg.timeframe is None
        assert isinstance(msg.data, TickData)
        assert msg.data.bid == Decimal(str(bid))
        assert msg.data.ask == Decimal(str(ask))

    def test_parse_multiple_ticks(self, handler):
        """Test parsing multiple tick data points."""
        expected_count = 5
        raw_data = mock_multiple_ticks(symbol="BTCUSD", count=expected_count)

        messages = handler._parse_channel("BTCUSD", raw_data["BTCUSD"])

        assert len(messages) == expected_count
        for msg in messages:
            assert msg.symbol == "BTCUSD"
            assert isinstance(msg.data, TickData)

    def test_parse_tick_with_negative_prices(self, handler):
        """Test that negative prices are skipped."""
        raw_data = {"BTCUSD": {"2025-10-09 05:52:24.825553": (-1.0850, -1.0848)}}

        messages = handler._parse_channel("BTCUSD", raw_data["BTCUSD"])

        assert len(messages) == 0


class TestOHLCDataParsing:
    """Test parsing OHLC data."""

    def test_parse_valid_ohlc_data(self, handler):
        """Test parsing valid OHLC data."""
        raw_data = mock_ohlc_data(symbol="BTCUSD", timeframe="M1")
        channel = "BTCUSD_M1"

        messages = handler._parse_channel(channel, raw_data[channel])

        assert len(messages) == 1
        msg = messages[0]
        assert msg.symbol == "BTCUSD"
        assert msg.timeframe == "M1"
        assert isinstance(msg.data, OHLCData)

    def test_parse_multiple_ohlc_bars(self, handler):
        """Test parsing multiple OHLC bars."""
        expected_count = 5
        raw_data = mock_multiple_ohlc(symbol="BTCUSD", timeframe="M1", count=expected_count)
        channel = "BTCUSD_M1"

        messages = handler._parse_channel(channel, raw_data[channel])

        assert len(messages) == expected_count
        for msg in messages:
            assert msg.symbol == "BTCUSD"
            assert msg.timeframe == "M1"
            assert isinstance(msg.data, OHLCData)

    def test_parse_invalid_ohlc_raises_validation_error(self, handler):
        """Test that invalid OHLC data (high < low) is skipped."""
        raw_data = {
            "BTCUSD_M1": {
                "2025-10-09 06:32:00.259239": (
                    1760002260.0,  # borker_time
                    1.0850,  # O
                    1.0840,  # H (invalid)
                    1.0850,  # L
                    1.0845,  # C
                    100.0,  # V
                ),
            },
        }
        channel = "BTCUSD_M1"

        messages = handler._parse_channel(channel, raw_data[channel])

        assert len(messages) == 0

    def test_parse_ohlc_different_timeframes(self, handler):
        """Test parsing different timeframes."""
        for timeframe in ["M1", "M5"]:
            raw_data = mock_ohlc_data(symbol="BTCUSD", timeframe=timeframe)
            channel = f"BTCUSD_{timeframe}"

            messages = handler._parse_channel(channel, raw_data[channel])

            assert len(messages) == 1
            assert messages[0].timeframe == timeframe

    def test_parse_brokertime_but_not_utc(self, handler):
        raw_data = mock_ohlc_data(symbol="BTCUSD", timeframe="M1")
        channel = "BTCUSD_M1"
        rd = raw_data[channel]
        broker_timestamp = next(iter(rd.values()))[0]
        broker_timestamp_in_utc = broker_timestamp - handler.brokertime_tz * 3600
        from datetime import UTC
        from datetime import datetime

        expected = datetime.fromtimestamp(broker_timestamp_in_utc, UTC)

        messages = handler._parse_channel(channel, raw_data[channel])
        assert messages[0].data.datetime == expected


class TestSymbolValidation:
    """Test symbol validation."""

    def test_supported_symbols_accepted(self, handler):
        """Test that supported symbols are accepted."""
        for symbol in ["BTCUSD", "GBPUSD", "USDJPY"]:
            if symbol == "BTCUSD":
                raw_data = mock_tick_data(symbol=symbol)
                messages = handler._parse_channel(symbol, raw_data[symbol])
                assert len(messages) >= 1
                assert messages[0].symbol == symbol

    def test_unsupported_symbol_rejected(self, handler):
        """Test that unsupported symbols are skipped."""
        raw_data = {"INVALID": {"2025-10-09 05:52:24.825553": (1.0850, 1.0852)}}

        messages = handler._parse_channel("INVALID", raw_data["INVALID"])

        assert len(messages) == 0


class TestTimeframeValidation:
    """Test timeframe validation."""

    def test_supported_timeframes_accepted(self, handler):
        """Test that supported timeframes are accepted."""
        for timeframe in ["M1", "M5"]:
            raw_data = mock_ohlc_data(symbol="BTCUSD", timeframe=timeframe)
            channel = f"BTCUSD_{timeframe}"

            messages = handler._parse_channel(channel, raw_data[channel])

            assert len(messages) == 1
            assert messages[0].timeframe == timeframe

    def test_unsupported_timeframe_rejected(self, handler):
        """Test that unsupported timeframes are skipped."""
        raw_data = {
            "BTCUSD_INVALID": {
                "2025-10-09 06:32:00.259239": (
                    1760002260.0,
                    1.0850,
                    1.0855,
                    1.0848,
                    1.0852,
                    100.0,
                    0,
                ),
            },
        }
        channel = "BTCUSD_INVALID"

        messages = handler._parse_channel(channel, raw_data[channel])

        assert len(messages) == 0


class TestChannelNameParsing:
    """Test channel name parsing."""

    def test_parse_tick_channel(self, handler):
        """Test parsing tick channel name (no underscore)."""
        symbol, timeframe = handler._parse_channel_name("BTCUSD")
        assert symbol == "BTCUSD"
        assert timeframe is None

    def test_parse_ohlc_channel(self, handler):
        """Test parsing OHLC channel name (with underscore)."""
        symbol, timeframe = handler._parse_channel_name("BTCUSD_M1")
        assert symbol == "BTCUSD"
        assert timeframe == "M1"

    def test_parse_various_channels(self, handler):
        """Test parsing various channel formats."""
        test_cases = [
            ("GBPUSD", ("GBPUSD", None)),
            ("USDJPY_H1", ("USDJPY", "H1")),
            ("BTCUSD_D1", ("BTCUSD", "D1")),
        ]

        for channel, expected in test_cases:
            result = handler._parse_channel_name(channel)
            assert result == expected


class TestProcessMethod:
    """Test the main process() method."""

    def test_process_empty_data(self, handler, capsys):
        """Test processing empty data."""
        handler.process({})

        # Should not print anything or raise error
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_process_valid_tick_data(self, handler, capsys):
        """Test processing valid tick data."""
        raw_data = mock_tick_data(symbol="BTCUSD")

        handler.process(raw_data)

        captured = capsys.readouterr()
        assert "BTCUSD" in captured.out

    def test_process_valid_ohlc_data(self, handler, capsys):
        """Test processing valid OHLC data."""
        raw_data = mock_ohlc_data(symbol="BTCUSD", timeframe="M1")

        handler.process(raw_data)

        captured = capsys.readouterr()
        assert "BTCUSD" in captured.out
        assert "M1" in captured.out

    def test_process_mixed_data(self, handler, capsys):
        """Test processing mixed tick and OHLC data."""
        tick_data = mock_tick_data(symbol="BTCUSD")
        ohlc_data = mock_multiple_ohlc(symbol="BTCUSD", timeframe="M1")
        ohlc_data2 = mock_ohlc_data(symbol="BTCUSD", timeframe="M5")

        mixed_data = {**tick_data, **ohlc_data, **ohlc_data2}

        handler.process(mixed_data)

        captured = capsys.readouterr()
        assert "BTCUSD" in captured.out
        assert "M5" in captured.out

    def test_process_handles_exceptions_gracefully(self, handler, capsys):
        """Test that process handles exceptions and continues."""
        invalid_data = {"INVALID": {"2025-10-09 05:52:24.825553": (1.0850, 1.0852)}}
        valid_data = mock_tick_data(symbol="BTCUSD")

        mixed_data = {**invalid_data, **valid_data}

        handler.process(mixed_data)

        captured = capsys.readouterr()
        assert "BTCUSD" in captured.out

    def test_process_detects_flat_ohlc_and_unsubscribes(self, handler, capsys):
        """Consecutive flat OHLC bars should trigger unsubscribe."""
        symbol = "BTCUSD"
        timeframe = "M1"
        base_broker_time = 1_700_000_000

        bar_count = FLAT_BAR_THRESHOLD + 1
        channel = f"{symbol}_{timeframe}"
        raw_data = {
            channel: {
                f"2025-10-09 06:{i:02d}:00.000000": (
                    base_broker_time + i * 60,
                    100.0,
                    100.0,
                    100.0,
                    100.0,
                    50.0,
                )
                for i in range(bar_count)
            },
        }

        handler.process(raw_data)

        captured = capsys.readouterr()
        assert symbol in captured.out
        assert handler._client.unsubscribed == [symbol]


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_parse_tick_with_extra_fields(self, handler):
        """Test that extra fields in tick data are ignored."""
        raw_data = {"BTCUSD": {"2025-10-09 05:52:24.825553": (1.0850, 1.0852, 999)}}

        messages = handler._parse_channel("BTCUSD", raw_data["BTCUSD"])

        assert len(messages) == 1
        msg = messages[0]
        assert msg.data.bid == Decimal("1.0850")
        assert msg.data.ask == Decimal("1.0852")

    def test_parse_ohlc_with_extra_fields(self, handler):
        """Test that extra fields in OHLC data are handled."""
        raw_data = {
            "BTCUSD_M1": {
                "2025-10-09 06:32:00.259239": (
                    1760002260.0,
                    1.0850,
                    1.0855,
                    1.0848,
                    1.0852,
                    100.0,
                    0,
                    999,
                ),
            },
        }

        messages = handler._parse_channel("BTCUSD_M1", raw_data["BTCUSD_M1"])

        assert len(messages) == 1
        assert messages[0].data.open == Decimal("1.0850")
