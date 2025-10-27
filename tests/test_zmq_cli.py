"""Tests for ZmqMt4Client."""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fixtures.mock_data import REAL_MARKET_DATA


@pytest.fixture()
def mock_dwx_connector():
    """Mock the DWX_ZeroMQ_Connector parent class methods."""
    with (
        # OS error: too many files opened testing on ZMQ need to some special knoweldge
        # the DWX code is no good, as __init__ also do connect, side-effet
        # making testing harder and less thorough
        # patch("zmqNotifier.zmq_cli.DWX_ZeroMQ_Connector.__init__", MagicMock()),
        patch(
            "zmqNotifier.zmq_cli.DWX_ZeroMQ_Connector._DWX_MTX_SUBSCRIBE_MARKETDATA_",
            MagicMock(),
        ),
        patch(
            "zmqNotifier.zmq_cli.DWX_ZeroMQ_Connector._DWX_MTX_UNSUBSCRIBE_MARKETDATA_",
            MagicMock(),
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
    yield client
    if hasattr(client, "shutdown"):
        client.shutdown()


@pytest.fixture()
def real_market_data():
    """Load real market data from pickle file."""
    return REAL_MARKET_DATA


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


class TestZmqMt4ClientUnsubscribe:
    def test_unsubscribe_calls_parent_method(self, zmq_client):
        """Test unsubscribe calls parent class unsubscription method."""
        zmq_client.unsubscribe("BTCUSD")
        zmq_client._DWX_MTX_UNSUBSCRIBE_MARKETDATA_.assert_called_once_with("BTCUSD")


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
