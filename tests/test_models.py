"""Tests for data models defined in src.models."""

from __future__ import annotations

import datetime
from datetime import datetime as dt
from decimal import Decimal

import pytest
from pydantic import ValidationError

from zmqNotifier.models import MarketDataMessage
from zmqNotifier.models import OHLCData
from zmqNotifier.models import TickData
from zmqNotifier.models import into_pip


def test_tick_data_valid() -> None:
    tick = TickData(datetime=dt.now(tz=datetime.UTC), bid=Decimal("1.2345"), ask=Decimal("1.2346"))

    assert tick.ask > tick.bid


def test_tick_data_rejects_non_positive_prices() -> None:
    with pytest.raises(ValidationError):
        TickData(datetime=dt.now(tz=datetime.UTC), bid=Decimal("0"), ask=Decimal("1.0"))


def test_tick_data_rejects_ask_not_above_bid() -> None:
    with pytest.raises(ValidationError):
        TickData(datetime=dt.now(tz=datetime.UTC), bid=Decimal("1.5"), ask=Decimal("1.5"))


def test_ohlc_data_valid() -> None:
    ohlc = OHLCData(
        datetime=dt.now(tz=datetime.UTC),
        open=Decimal("1.1000"),
        high=Decimal("1.1500"),
        low=Decimal("1.0900"),
        close=Decimal("1.1200"),
        volume=250,
    )

    assert ohlc.high >= max(ohlc.open, ohlc.close)
    assert ohlc.low <= min(ohlc.open, ohlc.close)


def test_ohlc_data_rejects_high_below_other_prices() -> None:
    with pytest.raises(ValidationError):
        OHLCData(
            datetime=dt.now(tz=datetime.UTC),
            open=Decimal("1.2000"),
            high=Decimal("1.1900"),
            low=Decimal("1.1800"),
            close=Decimal("1.1950"),
            volume=10,
        )


def test_ohlc_data_rejects_low_above_other_prices() -> None:
    with pytest.raises(ValidationError):
        OHLCData(
            datetime=dt.now(tz=datetime.UTC),
            open=Decimal("1.1000"),
            high=Decimal("1.1500"),
            low=Decimal("1.1600"),
            close=Decimal("1.1400"),
            volume=10,
        )


def test_market_data_message_accepts_supported_symbol_and_timeframe() -> None:
    payload = MarketDataMessage(
        symbol="EURUSD",
        timeframe="M1",
        data=OHLCData(
            datetime=dt.now(tz=datetime.UTC),
            open=Decimal("1.1000"),
            high=Decimal("1.1500"),
            low=Decimal("1.0900"),
            close=Decimal("1.1200"),
            volume=250,
        ),
    )

    assert payload.symbol == "EURUSD"
    assert payload.timeframe == "M1"


def test_market_data_message_rejects_unknown_symbol() -> None:
    with pytest.raises(ValidationError) as excinfo:
        MarketDataMessage(
            symbol="UNKNOWN",
            timeframe="M1",
            data=TickData(datetime=dt.now(tz=datetime.UTC), bid=Decimal("1.0"), ask=Decimal("1.1")),
        )

    assert "Unsupported symbol" in str(excinfo.value)


def test_market_data_message_rejects_invalid_timeframe() -> None:
    with pytest.raises(ValidationError) as excinfo:
        MarketDataMessage(
            symbol="EURUSD",
            timeframe="BAD",
            data=TickData(datetime=dt.now(tz=datetime.UTC), bid=Decimal("1.0"), ask=Decimal("1.1")),
        )

    assert "Invalid timeframe" in str(excinfo.value)


# ============================================================================
# Pipstep Conversion Tests
# ============================================================================


def test_into_pip_forex_standard_5_decimals() -> None:
    """Test 5-decimal forex pairs using realistic prices.

    Examples from models.py:
    - EURUSD: 1.15370
    - GBPUSD: 1.31476
    - USDCAD: 1.40130
    """
    # EURUSD: 10 pips movement (0.00100)
    assert into_pip(Decimal("0.00100")) == 10

    # GBPUSD: 25 pips movement (0.00250)
    assert into_pip(Decimal("0.00250")) == 25

    # USDCAD: 50 pips movement (0.00500)
    assert into_pip(Decimal("0.00500")) == 50

    # Typical 1 pip movement (0.00010)
    assert into_pip(Decimal("0.00010")) == 1

    # Typical 100 pips movement (0.01000)
    assert into_pip(Decimal("0.01000")) == 100


def test_into_pip_forex_jpy_3_decimals() -> None:
    """Test 3-decimal JPY pairs using realistic prices.

    Examples from models.py:
    - USDJPY: 154.015
    - EURJPY: 177.616
    - GBPJPY: 202.336
    """
    # USDJPY: 10 pips movement (0.100)
    assert into_pip(Decimal("0.100")) == 10

    # EURJPY: 25 pips movement (0.250)
    assert into_pip(Decimal("0.250")) == 25

    # GBPJPY: 50 pips movement (0.500)
    assert into_pip(Decimal("0.500")) == 50

    # Typical 1 pip movement (0.010)
    assert into_pip(Decimal("0.010")) == 1

    # Large 100 pips movement (1.000)
    assert into_pip(Decimal("1.000")) == 100


def test_into_pip_crypto_2_decimals() -> None:
    """Test 2-decimal crypto using realistic prices.

    Example from models.py:
    - BTCUSD: 110043.50
    """
    # BTCUSD: 100 pips movement (10.00)
    assert into_pip(Decimal("10.00")) == 100

    # BTCUSD: 50 pips movement (5.00)
    assert into_pip(Decimal("5.00")) == 50

    # BTCUSD: 10 pips movement (1.00)
    assert into_pip(Decimal("1.00")) == 10

    # BTCUSD: 1 pip movement (0.10)
    assert into_pip(Decimal("0.10")) == 1

    # BTCUSD: Large 1000 pips movement (100.00)
    assert into_pip(Decimal("100.00")) == 1000


def test_into_pip_commodities_2_decimals() -> None:
    """Test commodities using realistic prices.

    Examples from models.py:
    - XAUUSD: 4002.25 (gold, 2 decimals)
    """
    # XAUUSD: 100 pips movement (10.00)
    assert into_pip(Decimal("10.00")) == 100

    # XAUUSD: 25 pips movement (2.50)
    assert into_pip(Decimal("2.50")) == 25

    # XAUUSD: 1 pip movement (0.10)
    assert into_pip(Decimal("0.10")) == 1


def test_into_pip_commodities_3_decimals() -> None:
    """Test commodities with 3 decimals.

    Examples from models.py:
    - USOUSD: 61.106 (oil, 3 decimals)
    """
    # USOUSD: 10 pips movement (0.100)
    assert into_pip(Decimal("0.100")) == 10

    # USOUSD: 50 pips movement (0.500)
    assert into_pip(Decimal("0.500")) == 50

    # USOUSD: 1 pip movement (0.010)
    assert into_pip(Decimal("0.010")) == 1


def test_into_pip_realistic_price_differences() -> None:
    """Test pip calculation with actual price movements."""
    # EURUSD: 1.15370 -> 1.15470 = 10 pips
    eurusd_diff = Decimal("1.15470") - Decimal("1.15370")
    assert into_pip(eurusd_diff) == 10

    # USDJPY: 154.015 -> 154.265 = 25 pips
    usdjpy_diff = Decimal("154.265") - Decimal("154.015")
    assert into_pip(usdjpy_diff) == 25

    # BTCUSD: 110043.50 -> 110053.50 = 100 pips
    btcusd_diff = Decimal("110053.50") - Decimal("110043.50")
    assert into_pip(btcusd_diff) == 100

    # XAUUSD: 4002.25 -> 4003.25 = 10 pips
    xauusd_diff = Decimal("4003.25") - Decimal("4002.25")
    assert into_pip(xauusd_diff) == 10


def test_into_pip_zero_difference() -> None:
    """Test pip calculation with zero price difference."""
    assert into_pip(Decimal("0")) == 0
    assert into_pip(Decimal("0.00000")) == 0
    assert into_pip(Decimal("0.000")) == 0
    assert into_pip(Decimal("0.00")) == 0


def test_into_pip_negative_differences() -> None:
    """Test pip calculation with negative price differences."""
    # EURUSD: -10 pips
    assert into_pip(Decimal("-0.00100")) == -10

    # USDJPY: -25 pips
    assert into_pip(Decimal("-0.250")) == -25

    # BTCUSD: -100 pips
    assert into_pip(Decimal("-10.00")) == -100


def test_into_pip_large_movements() -> None:
    """Test pip calculation with large price movements."""
    # EURUSD: 500 pips movement
    assert into_pip(Decimal("0.05000")) == 500

    # USDJPY: 1000 pips movement (10 yen)
    assert into_pip(Decimal("10.000")) == 1000

    # BTCUSD: 10000 pips movement ($1000)
    assert into_pip(Decimal("1000.00")) == 10000


def test_into_pip_integer_prices() -> None:
    """Test pip calculation for integer values (0 decimal places)."""
    # For whole numbers, second-to-last place is tens
    assert into_pip(Decimal("100")) == 10
    assert into_pip(Decimal("50")) == 5
    assert into_pip(Decimal("10")) == 1
