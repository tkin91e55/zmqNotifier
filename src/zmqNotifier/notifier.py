"""
Notification system for market alerts and monitoring.

Volatility measures ticks counts (activity of trades) and price fluctuations over
short window.
"""

from datetime import timedelta
import logging
from typing import List

from dataclasses import dataclass
from zmqNotifier.tick_agg import BucketedSlidingAggregator
from zmqNotifier.models import TickData

logger = logging.getLogger(__name__)


class VolatilityNotifier:
    """
    Detects unusual short-term price movement and trade activity for a symbol

    This Notifier as manager, stream the ticks of a given symbol to aggregators. Given a
    thresholds of max-min (volatility) and tick counts (activity),
    it notifies when the thresholds are exceeded. There is cooldown in the length of small window

    on_tick(): Ingests ticks and drive SymbolTracks
    """


    def __init__(self):
        # a symbol host a list of aggregators, if the xxxx_threshold is defined in settings
        self.config = {}
        # read from settings for each symbol and create SymbolTrackers for them

    def on_tick(self, symbol, tick:TickData):
        # all symbol trackers on_tick(), which also calculate and eqnueue message
        # ask notify_manager to flush()
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
    symbol: str
    timeframe: str
    volatility_score: int
    activity_score: int
    direction: str  # "UP" or "DOWN"
    pip_change: int
    volume: int

    def mangnitude(self) -> int:
        return self.volatility_score * (self.activity_score+1)

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
    6. scoring per TF
    7. aggregators should store enough history buckets like 30
    """


    def __init__(self, symbol: str, master):
        self._symbol = symbol
        self._master = master
        self._aggregators: dict[str, BucketedSlidingAggregator] = {}

    def on_tick(self, tick:TickData):
        # feed tick to each aggregator
        pass

    def _calculate(self):
        # calculate the volatility and activity scores from master's config thresholds
        pass

    def add_agg(self, tf):
        # add a BucketedSlidingAggregator for the timeframe tf
        pass

    def remove_agg(self, tf):
        # add a BucketedSlidingAggregator for the timeframe tf
        pass


class NotificationManager:
    """
    Itself is a threaded work that keeps cooldown/escalating state per symbol/timeframe
    members:
      1. TelegramNotifier backend instance
        * if pririty queue is not empty, send message batch every 15 seconds
      2. priority queue of pending messages, in priorty weight of (volatility_score +
      activity_score), and larger TF is higher priority, needs a M5, H1 parser struct to
      timedelta already has timespan order, larger timespan, larger priority weight
      4. Batches alert summaries so recipients are not flooded with individual tick events.
    """
    FLUSH_INTERVAL = timedelta(seconds=15)

    def __init__(self):
        # initialize TelegramNotifier backend from settings
        # initialize priority queue
        # initialize time for flush
        pass

    def enqueue(self,score):
        pass

    def _format_batch_summary(self):
        """
        scores first, then an (ordered) dict add symbol and format as the message

        example:

        BTCUSD UP !! GBPUSD UP !!!

        # BTCUSD M1 UP
        Volatility: 120 pips since {V}, Activity: 300 ticks since {A}
        # BTCUSD M5 UP
        Volatility: 120 pips since {V}, Activity: 300 ticks since {A}
        ...
        """
        pass

    def _flush(self):
        # compile batch msg
        # telegram send batch
        # let _flush() be driven by tick events
        pass

notify_manager = NotificationManager()
