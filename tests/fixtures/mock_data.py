"""Mock MT4 market data for testing."""

import pickle
from itertools import islice
from pathlib import Path

_PICKLE_PATH = Path(__file__).parent.parent.parent / "data" / "zmq_market_db.pickle"

with _PICKLE_PATH.open("rb") as pf:
    REAL_MARKET_DATA = pickle.load(pf)


def MarketDataGenerator(
    symbol: str = "BTCUSD",
    timeframe: str | None = None,
    batch_size: int = 1,
):
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


mock_ohlc_data = lambda symbol="BTCUSD", timeframe="M1", **kwargs: next(
    MarketDataGenerator(symbol=symbol, timeframe=timeframe, batch_size=1)
)

mock_tick_data = lambda symbol="BTCUSD", **kwargs: next(
    MarketDataGenerator(symbol=symbol, timeframe=None, batch_size=1)
)

mock_multiple_ohlc = lambda symbol="BTCUSD", timeframe="M1", count=5, **kwargs: next(
    MarketDataGenerator(symbol=symbol, timeframe=timeframe, batch_size=count)
)

mock_multiple_ticks = lambda symbol="BTCUSD", count=5, **kwargs: next(
    MarketDataGenerator(symbol=symbol, timeframe=None, batch_size=count)
)
