"""Mock MT4 market data for testing."""

import pickle
from itertools import islice
from pathlib import Path

_PICKLE_PATH = Path(__file__).parent.parent.parent / "data" / "zmq_market_db.pickle"

with _PICKLE_PATH.open("rb") as pf:
    REAL_MARKET_DATA = pickle.load(pf)  # noqa: S301


def market_data_generator(
    symbol: str = "BTCUSD",
    timeframe: str | None = None,
    batch_size: int = 1,
):
    """Generate batches of market data from real pickle file."""
    channel = symbol if timeframe is None else f"{symbol}_{timeframe}"

    if channel not in REAL_MARKET_DATA:
        yield {channel: {}}
        return

    time_tuple = REAL_MARKET_DATA[channel]
    iterator = iter(time_tuple)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield {channel: {k: time_tuple[k] for k in batch}}


def mock_ohlc_data(symbol: str = "BTCUSD", timeframe: str = "M1", **kwargs):
    """Get single OHLC bar from real market data."""
    return next(market_data_generator(symbol=symbol, timeframe=timeframe, batch_size=1))


def mock_tick_data(symbol: str = "BTCUSD", **kwargs):
    """Get single tick from real market data."""
    return next(market_data_generator(symbol=symbol, timeframe=None, batch_size=1))


def mock_multiple_ohlc(symbol: str = "BTCUSD", timeframe: str = "M1", count: int = 5, **kwargs):
    """Get multiple OHLC bars from real market data."""
    return next(market_data_generator(symbol=symbol, timeframe=timeframe, batch_size=count))


def mock_multiple_ticks(symbol: str = "BTCUSD", count: int = 5, **kwargs):
    """Get multiple ticks from real market data."""
    return next(market_data_generator(symbol=symbol, timeframe=None, batch_size=count))
