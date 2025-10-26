"""
Mock MT4 market data for testing.

REAL_MARKET_DATA structure:
[('BTCUSD_M1', ('2025-09-15 09:55:58.893002', (1757940840, 114898.16, 114917.86, 114888.64, 114890.42, 66, 0, 0))...),
 ('BTCUSD_M5', ('2025-09-15 09:55:58.896449', (1757934900, 115489.69, 115710.88, 115435.11, 115658.48, 740, 0, 0))...),
 ('BTCUSD', ('2025-09-16 14:19:48.430725', (115006.13, 115023.24))...)]
"""

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
