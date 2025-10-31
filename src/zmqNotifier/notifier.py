"""
Notification system for market alerts and monitoring.

Volatility measures ticks counts (activity of trades) and price fluctuations over
short window.
"""

import logging
from dataclasses import dataclass
from zmqNotifier.market_data import MarketDataHandler
from zmqNotifier.tick_agg import BucketedSlidingAggregator
from zmqNotifier.models import TickData

logger = logging.getLogger(__name__)


class VolatilityNotifier:
    """
    Detects unusual short-term price movement and trade activity for a symbol

    Add VolatilitySettings to config.py, this is tree-like:
    TODO using pydantic settings get symbols and their thresholds for highlow and vol in TFs
        thresholds configuration tree: symbol-> (volatility_threshold, activity_threshold)-> TFs
        {
            "BTCUSD": {
            "volatility_threshold": { "M1": 999999, "M5": 9120, "M30": 300 ... }, # in pipsteps
            "activity_threshold":   { "M1": 999, "M5": 999,"M30": 999...  }, # in volume count
            },
            "GBPUSD": {
            "volatility_threshold": { "M1": 50, "M5": 120, "M30": 300 ... }, # in pipsteps
            "activity_threshold":   { "M1": 999, "M5": 999, "M30": 999... }, # in volume count
            },
            ....
        }

    This Notifier as manager, stream the ticks of a given symbol to aggregators. Given a
    thresholds of max-min (volatility) and tick counts (activity),
    it notifies when the thresholds are exceeded. There is cooldown in the length of small window

    on_tick(): Ingests ticks and drive SymbolTracks
    Overview
    --------
    - Batches alert summaries so recipients are not flooded with individual tick events.

    Runtime State
    -------------
    - tick_deque keeps the latest ticks needed to evaluate the configured windows.
    - recent_ohlc caches aggregated OHLC snapshots per symbol and timeframe.
    - Thresholds, cooldowns, and window settings are injected per symbol during setup.

    Notification Flow
    -----------------
    1. Receive a tick via on_tick and update the underlying aggregators.
    2. Recompute volatility and activity scores for each watched timeframe.
    3. If any score crosses its trigger and is outside the cooldown window, format an alert payload.
    4. Send the batched payload through the attached async TelegramNotifier backend.

    Possible API Surface
    --------------------
    - configure_symbol(symbol, thresholds, windows) -> None
    - on_tick(symbol, bid, ask, timestamp) -> None
    - flush_pending(now) -> Optional[str]
    - reset(symbol=None) -> None
    """


    def __init__(self):
        """
        """
        # a symbol host a list of aggregators, if the xxxx_threshold is defined in settings
        self.config = {}
        # read from settings for each symbol and configure aggregators

    def on_tick(self, symbol, tick:TickData):
        # Further processing to be implemented
        # for tf in self._aggregators[symbol]:
        #    for aggregator in self._aggregators[symbol][tf]:
        #       aggregator.add(tick.datetime, (tick.bid) )
        pass

    def update_config(self, config):
        """
        Redefine all sybmols and respective TFs with thresholds to monitor.
        Allow add/delete aggregators per symbol/TF.
        But aggregators doesn't keep thresmod, it just asks from config
        """
        # CRUD aggregators for symbols, make idempotent update, not nested merging if not exist
        # therefore,
        # if a symbol/tf is removed from config, remove the aggregator
        # if a symbol/tf is added, create the aggregator
        # if a symbol/tf exist, update the thresholds
        #
        # read
        pass

    def _configure_symbol(self, symbol: str):
        # Further processing to be implemented
        # read
        pass

class SymbolTracker:
    """
    Per symbol, it book-keeps a few short window aggregators, e.g. M1, M5, M30
    Implement cooldown/scores tracking per symbol/timeframe

    1. It has a few small window pipelines: M1, M5, M30. Each pipeline employs an
       BucketedSlidingAggregator to keep track of min/max prices in that window.
       And each BucketedSlidingAggregator keep a long span (retention, a few months
       Heuristically, a few weeks there is a volcanic change.
    2. Compares current measures against master's thresholds
       About score and message:
       The algorithm that calculates a vector score based on how much the volatility and activity
       exceed and how significant since the past time-span. Using expoential for scoring.
       e.g. V= log_threshold(volatility_now) x log_2(past_timespan_that_80percentile(volatility_now))
       similarly for acitivity

       enqueue to notification manager
       if exceeded either volatility/activity exceed thresholds for escalating
       levels givin the small time window cooldown. e.g. V = 1 send once, V=1.5, not sent within
       trigger cooldown, but if within cooldown, V>=2, send again... and trigger cooldown again
       The short message title is like BTCUSD M1 V5 A3 + GBPUSD M5 V1 A2

    3 Applies scoring and cooldown logic to decide when to emit a notification burst.
    4. Forwards alert messages through a single NofitificationManager using Telegram as backend.
    5. Built aggregators over the small windows, then it measures the largest activities since
       the past time-span. It is like how the observatory or statisical approach report rarity of
       events.
    6. configuration: cooldown settings (integral base TF unit) and scoring parameters
    7. aggregators should store enough history buckets like 30 (configurable as min_bucket_trigger)
    """

    @dataclass
    class Scores:
        """
        score vector, each element own cooldown and escalation value respectively

        e.g. a tick comes, TF M1, volatility score (v) escalates to 1 (V), enqueue message
        after some ticks comes, activity score (a) escalates to 1 (A), enqueue message
        no message is enqueue for each esacled value of a/v. and cooldown of a/v is reset by
        each tick that less than 0<a/v
        if no further escalation, after cooldown period, the a/v score is reduced by 1, until 0
        """
        volatility_score: int
        activity_score: int

        def mangnitude(self) -> int:
            return self.volatility_score * (self.activity_score+1)


    DEFAULT_COOLDOWN_UNIT = 1  # per TF unit
    DEFAULT_BUCKET_RETENTION = {'M1':60*24*7*4,
                                'M5':60*24*7*4//5,
                                'M30':60*24*7*4//15}  # 4 weeks
    def __init__(self, symbol: str, master):
        self._symbol = symbol
        self._master = master
        self._aggregators: dict[str, BucketedSlidingAggregator] = {}

    def calculate(self):
        # calculate the volatility and activity scores from master's config thresholds
        pass

    def add_window(self, tf):
        # add a BucketedSlidingAggregator for the timeframe tf
        pass

    def remove_window(self, tf):
        # add a BucketedSlidingAggregator for the timeframe tf
        pass


class NotificationManager:
    """
    Itself is a thread that keeps cooldown/escalating state per symbol/timeframe
    members:
      1. TelegramNotifier backend instance
        * if pririty queue is not empty, send message batch every 15 seconds
      2. priority queue of pending messages, in priorty weight of (volatility_score +
      activity_score), and larger TF is higher priority, needs a M5, H1 parser struct to
      timedelta already has timespan order, larger timespan, larger priority weight
      3. message interval: 15 seconds, make configurable
    """
    pass
