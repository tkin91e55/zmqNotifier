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
