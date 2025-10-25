"""Tests for ZmqMt4Client."""

import pickle
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest


@pytest.fixture()
def mock_dwx_connector():
    """Mock the DWX_ZeroMQ_Connector parent class methods."""
    with (
        patch(
            "zmqNotifier.zmq_cli.DWX_ZeroMQ_Connector._DWX_MTX_SUBSCRIBE_MARKETDATA_", MagicMock()
        ),
        patch(
            "zmqNotifier.zmq_cli.DWX_ZeroMQ_Connector._DWX_MTX_UNSUBSCRIBE_MARKETDATA_", MagicMock()
        ),
        patch(
            "zmqNotifier.zmq_cli.DWX_ZeroMQ_Connector._DWX_MTX_SEND_TRACKPRICES_REQUEST_",
            MagicMock(),
        ),
        patch(
            "zmqNotifier.zmq_cli.DWX_ZeroMQ_Connector._DWX_MTX_SEND_TRACKRATES_REQUEST_",
            MagicMock(),
        ),
        patch("zmqNotifier.zmq_cli.DWX_ZeroMQ_Connector._DWX_ZMQ_SHUTDOWN_", MagicMock()),
    ):
        yield


@pytest.fixture()
def zmq_client(mock_dwx_connector):
    """Create ZmqMt4Client instance with mocked dependencies."""
    from zmqNotifier.zmq_cli import ZmqMt4Client

    client = ZmqMt4Client(_host="mock_host")
    return client


@pytest.fixture()
def real_market_data():
    """Load real market data from pickle file."""
    pickle_path = Path(__file__).parent.parent / "data" / "zmq_market_db.pickle"
    with pickle_path.open("rb") as pf:
        return pickle.load(pf)  # noqa: S301


class TestZmqMt4ClientInitialization:
    def test_client_initializes_with_data_handler(self, zmq_client):
        """Test client initializes with MarketDataHandler."""
        assert zmq_client.data_handler is not None
        assert hasattr(zmq_client.data_handler, "process")

    def test_client_registers_data_callback(self, zmq_client):
        """Test client registers on_data_recv as callback."""
        assert zmq_client.on_data_recv in zmq_client._subdata_handlers


class TestZmqMt4ClientProperties:
    def test_is_active_property_true(self, zmq_client):
        """Test is_active property returns True when client is active."""
        zmq_client._ACTIVE = True
        assert zmq_client.is_active is True

    def test_is_active_property_false(self, zmq_client):
        """Test is_active property returns False when client is inactive."""
        zmq_client._ACTIVE = False
        assert zmq_client.is_active is False

    def test_verbose_getter(self, zmq_client):
        """Test verbose property getter."""
        zmq_client._verbose = True
        assert zmq_client.verbose is True

    def test_verbose_setter(self, zmq_client):
        """Test verbose property setter."""
        zmq_client.verbose = False
        assert zmq_client._verbose is False

    def test_market_data_property(self, zmq_client):
        """Test market_data property returns raw buffer."""
        test_data = {"BTCUSD": {"2025-10-09 05:52:24.825553": (121989.53, 122006.49)}}
        zmq_client._Market_Data_DB = test_data
        assert zmq_client.market_data == test_data


class TestZmqMt4ClientSubscribe:
    def test_subscribe_validates_symbol(self, zmq_client):
        """Test subscribe validates symbol before subscribing."""
        with patch("zmqNotifier.zmq_cli.validate_symbol") as mock_validate:
            zmq_client.subscribe("BTCUSD")
            mock_validate.assert_called_once_with("BTCUSD")

    def test_subscribe_calls_parent_method(self, zmq_client):
        """Test subscribe calls parent class subscription method."""
        with patch("zmqNotifier.zmq_cli.validate_symbol"):
            zmq_client.subscribe("BTCUSD")
            zmq_client._DWX_MTX_SUBSCRIBE_MARKETDATA_.assert_called_once_with("BTCUSD")

    def test_subscribe_with_tick_data_requests_ticks(self, zmq_client):
        """Test subscribe with for_tickdata=True requests tick data."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch.object(zmq_client, "track_ticks") as mock_track,
        ):
            zmq_client.subscribe("BTCUSD", for_tickdata=True)
            mock_track.assert_called_once_with("BTCUSD")

    def test_subscribe_with_ohlc_m1_requests_bars(self, zmq_client):
        """Test subscribe with for_ohlc_m1=True requests M1 bars."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch.object(zmq_client, "track_bars") as mock_track,
        ):
            zmq_client.subscribe("BTCUSD", for_ohlc_m1=True)
            mock_track.assert_called_once_with("BTCUSD", ["M1"])

    def test_subscribe_with_both_flags(self, zmq_client):
        """Test subscribe with both tick and OHLC flags."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch.object(zmq_client, "track_ticks") as mock_ticks,
            patch.object(zmq_client, "track_bars") as mock_bars,
        ):
            zmq_client.subscribe("BTCUSD", for_tickdata=True, for_ohlc_m1=True)
            mock_ticks.assert_called_once_with("BTCUSD")
            mock_bars.assert_called_once_with("BTCUSD", ["M1"])

    def test_subscribe_logs_subscription(self, zmq_client):
        """Test subscribe logs subscription event."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch("zmqNotifier.zmq_cli.logger") as mock_logger,
        ):
            zmq_client.subscribe("BTCUSD")
            mock_logger.info.assert_called_with("Subscribed to %s", "BTCUSD")


class TestZmqMt4ClientUnsubscribe:
    def test_unsubscribe_calls_parent_method(self, zmq_client):
        """Test unsubscribe calls parent class unsubscription method."""
        zmq_client.unsubscribe("BTCUSD")
        zmq_client._DWX_MTX_UNSUBSCRIBE_MARKETDATA_.assert_called_once_with("BTCUSD")

    def test_unsubscribe_closes_logger(self, zmq_client):
        """Test unsubscribe closes and removes logger for symbol."""
        mock_logger = Mock()
        zmq_client._loggers = {"BTCUSD": mock_logger}

        zmq_client.unsubscribe("BTCUSD")

        mock_logger.close.assert_called_once()
        assert "BTCUSD" not in zmq_client._loggers

    def test_unsubscribe_handles_missing_logger(self, zmq_client):
        """Test unsubscribe works when logger doesn't exist."""
        zmq_client._loggers = {}
        zmq_client.unsubscribe("BTCUSD")
        assert "BTCUSD" not in zmq_client._loggers

    def test_unsubscribe_logs_event(self, zmq_client):
        """Test unsubscribe logs unsubscription event."""
        with patch("zmqNotifier.zmq_cli.logger") as mock_logger:
            zmq_client.unsubscribe("BTCUSD")
            mock_logger.info.assert_called_with("Unsubscribed from %s", "BTCUSD")


class TestZmqMt4ClientTrackTicks:
    def test_track_ticks_validates_symbol(self, zmq_client):
        """Test track_ticks validates symbol."""
        with patch("zmqNotifier.zmq_cli.validate_symbol") as mock_validate:
            zmq_client.track_ticks("BTCUSD")
            mock_validate.assert_called_once_with("BTCUSD")

    def test_track_ticks_calls_parent_method(self, zmq_client):
        """Test track_ticks calls parent class track prices method."""
        with patch("zmqNotifier.zmq_cli.validate_symbol"):
            zmq_client.track_ticks("BTCUSD")
            zmq_client._DWX_MTX_SEND_TRACKPRICES_REQUEST_.assert_called_once_with(["BTCUSD"])

    def test_track_ticks_logs_event(self, zmq_client):
        """Test track_ticks logs tracking event."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch("zmqNotifier.zmq_cli.logger") as mock_logger,
        ):
            zmq_client.track_ticks("BTCUSD")
            mock_logger.info.assert_called_with("Tracking ticks for %s", "BTCUSD")


class TestZmqMt4ClientTrackBars:
    def test_track_bars_empty_timeframes_does_nothing(self, zmq_client):
        """Test track_bars does nothing with empty timeframes."""
        with patch("zmqNotifier.zmq_cli.validate_symbol") as mock_validate:
            zmq_client.track_bars("BTCUSD", [])
            mock_validate.assert_not_called()

    def test_track_bars_none_timeframes_does_nothing(self, zmq_client):
        """Test track_bars does nothing with None timeframes."""
        with patch("zmqNotifier.zmq_cli.validate_symbol") as mock_validate:
            zmq_client.track_bars("BTCUSD", None)
            mock_validate.assert_not_called()

    def test_track_bars_validates_symbol(self, zmq_client):
        """Test track_bars validates symbol."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol") as mock_validate,
            patch("zmqNotifier.zmq_cli.validate_timeframe"),
            patch("zmqNotifier.zmq_cli.get_timeframe_minutes", return_value=1),
        ):
            zmq_client.track_bars("BTCUSD", ["M1"])
            mock_validate.assert_called_once_with("BTCUSD")

    def test_track_bars_validates_timeframes(self, zmq_client):
        """Test track_bars validates each timeframe."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch("zmqNotifier.zmq_cli.validate_timeframe") as mock_validate_tf,
            patch("zmqNotifier.zmq_cli.get_timeframe_minutes", return_value=1),
        ):
            zmq_client.track_bars("BTCUSD", ["M1", "M5"])
            assert mock_validate_tf.call_count == 2

    def test_track_bars_gets_timeframe_minutes(self, zmq_client):
        """Test track_bars gets minutes for each timeframe."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch("zmqNotifier.zmq_cli.validate_timeframe"),
            patch("zmqNotifier.zmq_cli.get_timeframe_minutes", return_value=5) as mock_get_minutes,
        ):
            zmq_client.track_bars("BTCUSD", ["M5"])
            mock_get_minutes.assert_called_with("M5")

    def test_track_bars_builds_correct_channels(self, zmq_client):
        """Test track_bars builds correct channel format."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch("zmqNotifier.zmq_cli.validate_timeframe"),
            patch("zmqNotifier.zmq_cli.get_timeframe_minutes", side_effect=[1, 5]),
        ):
            zmq_client.track_bars("BTCUSD", ["M1", "M5"])

            expected_channels = [
                ("BTCUSD_M1", "BTCUSD", 1),
                ("BTCUSD_M5", "BTCUSD", 5),
            ]
            zmq_client._DWX_MTX_SEND_TRACKRATES_REQUEST_.assert_called_once_with(expected_channels)

    def test_track_bars_logs_event(self, zmq_client):
        """Test track_bars logs tracking event."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch("zmqNotifier.zmq_cli.validate_timeframe"),
            patch("zmqNotifier.zmq_cli.get_timeframe_minutes", return_value=1),
            patch("zmqNotifier.zmq_cli.logger") as mock_logger,
        ):
            zmq_client.track_bars("BTCUSD", ["M1"])
            mock_logger.info.assert_called_with("Tracking rates for %s: %s", "BTCUSD", ["M1"])


class TestZmqMt4ClientOnDataRecv:
    def test_on_data_recv_empty_buffer_logs_warning(self, zmq_client):
        """Test on_data_recv logs warning for empty buffer."""
        zmq_client._Market_Data_DB = {}

        with patch("zmqNotifier.zmq_cli.logger") as mock_logger:
            zmq_client.on_data_recv(None)
            mock_logger.warning.assert_called_once_with(
                "on_data_recv called with empty market data"
            )

    def test_on_data_recv_empty_buffer_skips_processing(self, zmq_client):
        """Test on_data_recv skips processing for empty buffer."""
        zmq_client._Market_Data_DB = {}

        with patch.object(zmq_client.data_handler, "process") as mock_process:
            zmq_client.on_data_recv(None)
            mock_process.assert_not_called()

    def test_on_data_recv_processes_market_data(self, zmq_client):
        """Test on_data_recv processes market data through handler."""
        test_data = {"BTCUSD": {"2025-10-09 05:52:24.825553": (121989.53, 122006.49)}}
        zmq_client._Market_Data_DB = test_data.copy()

        # Capture the data before it gets cleared
        captured_data = None

        def capture_and_process(data):
            nonlocal captured_data
            captured_data = data.copy()

        with patch.object(zmq_client.data_handler, "process", side_effect=capture_and_process):
            zmq_client.on_data_recv(None)
            assert captured_data == test_data

    def test_on_data_recv_clears_buffer_after_processing(self, zmq_client):
        """Test on_data_recv clears buffer after successful processing."""
        test_data = {"BTCUSD": {"2025-10-09 05:52:24.825553": (121989.53, 122006.49)}}
        zmq_client._Market_Data_DB = test_data.copy()

        with patch.object(zmq_client.data_handler, "process"):
            zmq_client.on_data_recv(None)
            assert zmq_client._Market_Data_DB == {}

    def test_on_data_recv_handles_processing_exception(self, zmq_client):
        """Test on_data_recv handles exceptions during processing."""
        test_data = {"BTCUSD": {"2025-10-09 05:52:24.825553": (121989.53, 122006.49)}}
        zmq_client._Market_Data_DB = test_data.copy()

        with (
            patch.object(zmq_client.data_handler, "process", side_effect=ValueError("Test error")),
            patch("zmqNotifier.zmq_cli.logger") as mock_logger,
        ):
            zmq_client.on_data_recv(None)
            mock_logger.exception.assert_called_once_with("Failed to process market data")

    def test_on_data_recv_clears_buffer_even_on_exception(self, zmq_client):
        """Test on_data_recv clears buffer even when exception occurs."""
        test_data = {"BTCUSD": {"2025-10-09 05:52:24.825553": (121989.53, 122006.49)}}
        zmq_client._Market_Data_DB = test_data.copy()

        with (
            patch.object(zmq_client.data_handler, "process", side_effect=ValueError("Test error")),
            patch("zmqNotifier.zmq_cli.logger"),
        ):
            zmq_client.on_data_recv(None)
            assert zmq_client._Market_Data_DB == {}


class TestZmqMt4ClientShutdown:
    def test_shutdown_calls_parent_shutdown(self, zmq_client):
        """Test shutdown calls parent class shutdown method."""
        with patch("zmqNotifier.zmq_cli.logger"):
            zmq_client.shutdown()
            zmq_client._DWX_ZMQ_SHUTDOWN_.assert_called_once()

    def test_shutdown_logs_start(self, zmq_client):
        """Test shutdown logs shutdown start."""
        with patch("zmqNotifier.zmq_cli.logger") as mock_logger:
            zmq_client.shutdown()
            mock_logger.info.assert_any_call("Shutting down ZmqMt4Client")

    def test_shutdown_logs_completion(self, zmq_client):
        """Test shutdown logs shutdown completion."""
        with patch("zmqNotifier.zmq_cli.logger") as mock_logger:
            zmq_client.shutdown()
            mock_logger.info.assert_any_call("ZmqMt4Client shutdown complete")


class TestZmqMt4ClientIntegration:
    """Integration tests with real data processing."""

    def test_data_processing_with_real_tick_data(self, zmq_client, real_market_data):
        """Test processing real tick data from pickle file."""
        if "BTCUSD" not in real_market_data:
            pytest.skip("BTCUSD tick data not available in pickle file")

        tick_data = {"BTCUSD": real_market_data["BTCUSD"]}
        zmq_client._Market_Data_DB = tick_data

        with patch.object(zmq_client.data_handler, "process") as mock_process:
            zmq_client.on_data_recv(None)
            mock_process.assert_called_once()
            assert zmq_client._Market_Data_DB == {}

    def test_data_processing_with_real_ohlc_data(self, zmq_client, real_market_data):
        """Test processing real OHLC data from pickle file."""
        if "BTCUSD_M1" not in real_market_data:
            pytest.skip("BTCUSD_M1 OHLC data not available in pickle file")

        ohlc_data = {"BTCUSD_M1": real_market_data["BTCUSD_M1"]}
        zmq_client._Market_Data_DB = ohlc_data

        with patch.object(zmq_client.data_handler, "process") as mock_process:
            zmq_client.on_data_recv(None)
            mock_process.assert_called_once()
            assert zmq_client._Market_Data_DB == {}

    def test_data_processing_with_mixed_data(self, zmq_client, real_market_data):
        """Test processing mixed tick and OHLC data."""
        mixed_data = {}

        if "BTCUSD" in real_market_data:
            mixed_data["BTCUSD"] = real_market_data["BTCUSD"]

        if "BTCUSD_M1" in real_market_data:
            mixed_data["BTCUSD_M1"] = real_market_data["BTCUSD_M1"]

        if not mixed_data:
            pytest.skip("No test data available in pickle file")

        zmq_client._Market_Data_DB = mixed_data

        with patch.object(zmq_client.data_handler, "process") as mock_process:
            zmq_client.on_data_recv(None)
            mock_process.assert_called_once()
            assert zmq_client._Market_Data_DB == {}


class TestZmqMt4ClientEdgeCases:
    """Edge case and error condition tests."""

    def test_subscribe_with_invalid_symbol_raises_error(self, zmq_client):
        """Test subscribe with invalid symbol raises validation error."""
        with patch("zmqNotifier.zmq_cli.validate_symbol", side_effect=ValueError("Invalid symbol")):
            with pytest.raises(ValueError, match="Invalid symbol"):
                zmq_client.subscribe("INVALID")

    def test_track_ticks_with_invalid_symbol_raises_error(self, zmq_client):
        """Test track_ticks with invalid symbol raises validation error."""
        with patch("zmqNotifier.zmq_cli.validate_symbol", side_effect=ValueError("Invalid symbol")):
            with pytest.raises(ValueError, match="Invalid symbol"):
                zmq_client.track_ticks("INVALID")

    def test_track_bars_with_invalid_timeframe_raises_error(self, zmq_client):
        """Test track_bars with invalid timeframe raises validation error."""
        with (
            patch("zmqNotifier.zmq_cli.validate_symbol"),
            patch(
                "zmqNotifier.zmq_cli.validate_timeframe",
                side_effect=ValueError("Invalid timeframe"),
            ),
        ):
            with pytest.raises(ValueError, match="Invalid timeframe"):
                zmq_client.track_bars("BTCUSD", ["INVALID"])

    def test_on_data_recv_with_malformed_data(self, zmq_client):
        """Test on_data_recv handles malformed data gracefully."""
        malformed_data = {"BTCUSD": "not a dict"}
        zmq_client._Market_Data_DB = malformed_data

        with (
            patch.object(
                zmq_client.data_handler, "process", side_effect=TypeError("Malformed data")
            ),
            patch("zmqNotifier.zmq_cli.logger") as mock_logger,
        ):
            zmq_client.on_data_recv(None)
            mock_logger.exception.assert_called_once()
            assert zmq_client._Market_Data_DB == {}

    def test_market_data_property_with_multiple_symbols(self, zmq_client):
        """Test market_data property with multiple symbols."""
        test_data = {
            "BTCUSD": {"2025-10-09 05:52:24.825553": (121989.53, 122006.49)},
            "EURUSD": {"2025-10-09 05:52:25.123456": (1.0850, 1.0851)},
            "BTCUSD_M1": {
                "2025-10-09 06:32:00.259239": (
                    1760002260,
                    122116.68,
                    122179.17,
                    122115.79,
                    122170.36,
                    89,
                    0,
                    0,
                )
            },
        }
        zmq_client._Market_Data_DB = test_data
        assert zmq_client.market_data == test_data

    def test_unsubscribe_all_symbols_clears_all_loggers(self, zmq_client):
        """Test unsubscribing from all symbols clears all loggers."""
        mock_logger1 = Mock()
        mock_logger2 = Mock()
        zmq_client._loggers = {"BTCUSD": mock_logger1, "EURUSD": mock_logger2}

        zmq_client.unsubscribe("BTCUSD")
        zmq_client.unsubscribe("EURUSD")

        mock_logger1.close.assert_called_once()
        mock_logger2.close.assert_called_once()
        assert zmq_client._loggers == {}
