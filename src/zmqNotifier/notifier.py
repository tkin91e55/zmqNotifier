"""
Notification system for market alerts and monitoring.
Detects unusual short-term price movement and trade activity for a symbol

Volatility measures ticks counts (activity of trades) and price fluctuations over
short window.
"""

from datetime import datetime, timedelta
from decimal import Decimal
import logging
import math

from dataclasses import dataclass
from zmqNotifier.tick_agg import BucketedSlidingAggregator
from zmqNotifier.models import TickData, into_pip
from zmqNotifier.market_data import get_timeframe_minutes
from zmqNotifier.config import AppSettings, get_settings, SymbolTrackerConfig

logger = logging.getLogger(__name__)


class VolatilityNotifier:
    """
    This Notifier as manager, stream the ticks of a given symbol to aggregators. Given a
    thresholds of max-min (volatility) and tick counts (activity),
    it notifies when the thresholds are exceeded. There is cooldown in the length of small window

    on_tick(): Ingests ticks and drive SymbolTracks, notificiation manager's flush()
    update_config(): Update notifier configuration and sync trackers/aggregators.
    """

    def __init__(self, config: AppSettings | None = None):
        self.config = config or get_settings()
        self._trackers: dict[str, SymbolTracker] = {}
        # Initialize trackers for all configured symbols
        for symbol in self.config.notifier.symbols.keys():
            self._trackers[symbol] = SymbolTracker(symbol, self)

    def on_tick(self, symbol: str, tick: TickData):
        tracker = self._trackers.get(symbol)
        if tracker is None:
            logger.warning("No tracker configured for symbol %s", symbol)
            return

        tracker.on_tick(tick)
        notify_manager.flush()

    def update_config(self, config: AppSettings):
        """
        Update notifier configuration and sync trackers/aggregators.

        This is an idempotent operation that replaces the entire symbol configuration:
        - Removes trackers for symbols no longer in config
        - Creates trackers for newly added symbols
        - For existing symbols, syncs timeframes (add/remove aggregators as needed)
        - Threshold updates take effect immediately (no restart needed)

        Args:
            config: New AppSettings instance with updated notifier.symbols

        Design notes:
            - Aggregators don't store thresholds; they query config each time
            - Existing aggregator history is preserved when timeframes remain
            - Cooldown state is preserved for existing timeframes
            - New aggregators start with empty history and reset cooldown
        """
        self.config = config

        current_symbols = set(self._trackers.keys())
        new_symbols = set(self.config.notifier.symbols.keys())

        symbols_to_remove = self._remove_stale_trackers(current_symbols, new_symbols)
        symbols_to_add = self._add_new_trackers(current_symbols, new_symbols)
        symbols_to_update = current_symbols & new_symbols
        self._sync_existing_trackers(symbols_to_update)

        logger.info(
            "Config update complete: %d symbols tracked (%d added, %d removed, %d updated)",
            len(self._trackers),
            len(symbols_to_add),
            len(symbols_to_remove),
            len(symbols_to_update),
        )

    def _remove_stale_trackers(self, current_symbols: set[str], new_symbols: set[str]) -> set[str]:
        symbols_to_remove = current_symbols - new_symbols

        for symbol in symbols_to_remove:
            logger.info("Removing tracker for symbol %s (no longer in config)", symbol)
            del self._trackers[symbol]

        return symbols_to_remove

    def _add_new_trackers(self, current_symbols: set[str], new_symbols: set[str]) -> set[str]:
        symbols_to_add = new_symbols - current_symbols

        for symbol in symbols_to_add:
            logger.info("Adding tracker for new symbol %s", symbol)
            self._trackers[symbol] = SymbolTracker(symbol, self)

        return symbols_to_add

    def _sync_existing_trackers(self, symbols: set[str]) -> None:
        for symbol in symbols:
            tracker = self._trackers[symbol]
            thresholds = self.config.notifier.thresholds_for(symbol)

            if thresholds is None:
                logger.warning("Symbol %s has no thresholds, removing all aggregators", symbol)
                for tf in list(tracker._aggregators.keys()):
                    tracker.remove_agg(tf)
                continue

            desired_tfs = set(thresholds.keys())
            current_tfs = set(tracker._aggregators.keys())

            tfs_to_remove = current_tfs - desired_tfs
            for tf in tfs_to_remove:
                logger.info("Removing aggregator for %s/%s (no longer in config)", symbol, tf)
                tracker.remove_agg(tf)

            tfs_to_add = desired_tfs - current_tfs
            for tf in tfs_to_add:
                logger.info("Adding aggregator for %s/%s (new in config)", symbol, tf)
                tracker.add_agg(tf)

            # Existing timeframes keep their aggregator history and cooldown state
            # Threshold updates take effect immediately on next _calculate() call


@dataclass
class AggStates:
    """
    Aggregator state tracking for a symbol/timeframe combination.

    Tracks both current measurements and notification state:
    - Current volatility (pip_change) and activity (volume/tick count)
    - Calculated scores based on threshold exceedance
    - Cooldown timestamps to prevent notification spam
    - Escalation tracking for higher-severity alerts

    Notification Logic:
    - When a tick arrives and TF M1 volatility score (v) escalates to 1 (V), enqueue message
    - After more ticks, if activity score (a) escalates to 1 (A), enqueue message
    - No duplicate messages for same escalation level during cooldown
    - Cooldown of a/v is reset by each tick that brings score below threshold (0 < a/v)
    - If no further escalation after cooldown period, the a/v score reduces by 1 until 0

    The direction field indicates price movement direction ("UP" or "DOWN").
    """

    volatility_score: int = 0
    activity_score: int = 0
    last_volatility_notification: int = -99999  # Timestamp (seconds since epoch)
    last_activity_notification: int = -99999

    def magnitude(self) -> int:
        """
        Calculate combined magnitude of volatility and activity scores.

        Returns a weighted score where volatility is amplified by activity level.
        Higher magnitude indicates more urgent notification priority.
        """
        return self.volatility_score * (self.activity_score + 1)


class SymbolTracker:
    """
    Per symbol, it book-keeps a few short window aggregators, e.g. M1, M5, M30
    Implement cooldown/scores tracking per symbol/timeframe

    1. It has a few small window pipelines: M1, M5, M30. Each pipeline employs an
       BucketedSlidingAggregator to keep track of min/max prices in that window.
       And each BucketedSlidingAggregator keep a long span (retention, a few months
       Heuristically, a few weeks there is a volcanic change.
    5. Built aggregators over the small windows, then it measures the largest activities since
       the past time-span. It is like how the observatory or statisical approach report rarity of
       events.
    """

    def __init__(self, symbol: str, master: "VolatilityNotifier"):
        """
        Initialize a SymbolTracker for the given symbol.

        Args:
            symbol: Trading symbol (e.g., 'EURUSD', 'BTCUSD')
            master: Reference to VolatilityNotifier to access config and notification manager

        The initializer:
        1. Reads threshold and tracker config from master
        2. Creates BucketedSlidingAggregator for each configured timeframe
        3. Initializes per-timeframe cooldown tracking
        """
        self._symbol = symbol
        self._master = master
        self._aggregators: dict[str, BucketedSlidingAggregator] = {}
        self._agg_states: dict[str, AggStates] = {}

        # Get configuration from master
        thresholds = self.thresholds
        if thresholds is None:
            logger.warning("No thresholds configured for symbol %s", symbol)
            return

        # Create aggregators for each timeframe that has thresholds defined
        # thresholds is now a dict[str, tuple[int, int]] where key is timeframe
        configured_timeframes = set(thresholds.keys())

        for tf in configured_timeframes:
            self.add_agg(tf)

        logger.info(
            "SymbolTracker initialized for %s with timeframes: %s",
            symbol,
            list(self._aggregators.keys()),
        )

    def on_tick(self, tick: TickData):
        if not self._aggregators:
            return

        mid_price = (tick.bid + tick.ask) / Decimal(2)

        # Feed to all aggregators
        for agg in self._aggregators.values():
            agg.add(tick.datetime, mid_price)

        self._calculate(tick.datetime)

    def _calculate(self, now: datetime) -> None:
        """
        Calculate volatility and activity scores across all timeframes.

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
        6. aggregators should store enough history buckets like 30

        Compares current aggregator metrics against configured thresholds,
        applies cooldown logic, and returns AggStates if notification warranted.

        Args:
            timestamp: Current tick timestamp for cooldown checking

        Returns:
            AggStates object if any threshold exceeded, None otherwise

        Scoring Logic:
            - Volatility score: log2(pip_change / threshold) rounded up
            - Activity score:   log2(tick_count / threshold) rounded up
            - Both scores capped at 0 minimum
            - Cooldown prevents repeated notifications within cooldown_unit × timeframe
            - Escalation: Higher scores can break through active cooldown
        """

        thresholds = self.thresholds
        if not thresholds:
            return

        tracker_config = self.tracker
        current_timestamp = int(now.timestamp())
        cooldown_unit = tracker_config.cooldown_unit
        min_buckets = tracker_config.min_buckets_calculation

        for tf, agg in self._aggregators.items():
            state = self._agg_states[tf]
            vol_threshold, act_threshold = thresholds.get(tf, (None, None))
            if vol_threshold is None or act_threshold is None:
                continue
            if agg.buckets_count < min_buckets:
                continue

            # Get current measures
            _min, _max, tick_cnt = agg.query_min_max()
            price_change = _max - _min
            # Convert price difference to pips using symbol-specific multiplier
            pip_change = into_pip(price_change)

            # Calculate scores using log2
            # Score = floor(log2(current / threshold)), minimum 0
            volatility_score = (
                max(0, math.floor(math.log2(pip_change / vol_threshold)))
                if pip_change >= vol_threshold
                else 0
            )
            # Check if either score triggers notification
            if volatility_score == 0:
                continue

            # Cooldown logic with escalation
            cooldown_seconds = cooldown_unit * self._get_timeframe_seconds(tf)

            # Check volatility notification
            if volatility_score > 0:
                time_since_last = current_timestamp - state.last_volatility_notification
                # Notify if: no prior notification, cooldown expired, or score escalated
                if (
                    state.last_volatility_notification == -99999
                    or time_since_last >= cooldown_seconds
                    or volatility_score > state.volatility_score
                ):
                    state.volatility_score = volatility_score
                    state.last_volatility_notification = current_timestamp
                    # TODO: Enqueue to NotificationManager
                    logger.info(
                        "Volatility alert: %s %s V%d (pip_change=%d, threshold=%d)",
                        self._symbol,
                        tf,
                        volatility_score,
                        pip_change,
                        vol_threshold,
                    )

    def _get_timeframe_seconds(self, tf: str) -> float:
        """Convert timeframe code to seconds."""
        from zmqNotifier.market_data import get_timeframe_minutes

        return get_timeframe_minutes(tf) * 60

    def add_agg(self, tf: str):
        if tf in self._aggregators:
            logger.debug("Aggregator for %s/%s already exists", self._symbol, tf)
            return

        tracker_config = self.tracker

        # Convert timeframe to timedelta for bucket_span
        tf_minutes = get_timeframe_minutes(tf)
        bucket_span = timedelta(minutes=tf_minutes)

        # Determine max_window (number of buckets to retain)
        max_window = None
        if tracker_config.num_bucket_retention:
            max_window = tracker_config.num_bucket_retention.get(tf)

        self._aggregators[tf] = BucketedSlidingAggregator(
            bucket_span=bucket_span, max_window=max_window
        )
        self._agg_states[tf] = AggStates()

        logger.debug(
            "Added aggregator for %s/%s with bucket_span=%s, max_window=%s",
            self._symbol,
            tf,
            bucket_span,
            max_window,
        )

    def remove_agg(self, tf: str):
        """
        Remove the BucketedSlidingAggregator for the given timeframe.

        Args:
            tf: Timeframe code (e.g., 'M1', 'M5', 'M30')
        """
        if tf not in self._aggregators:
            logger.debug("No aggregator found for %s/%s to remove", self._symbol, tf)
            return

        del self._aggregators[tf]
        del self._agg_states[tf]

        logger.debug("Removed aggregator for %s/%s", self._symbol, tf)

    @property
    def thresholds(self) -> dict[str, tuple[int, int]] | None:
        return self._master.config.notifier.thresholds_for(self._symbol)

    @property
    def tracker(self) -> SymbolTrackerConfig:
        return self._master.config.notifier.resolve_tracker_config(self._symbol)


class NotificationManager:
    """
    Manages notification queuing, batching, and delivery for market alerts.

    Runs as a threaded worker that:
    1. Maintains a TelegramNotifier backend instance
       - Sends message batches every 15 seconds if priority queue is not empty
    2. Manages a priority queue of pending AggStates messages
       - Priority weight based on magnitude (volatility_score * (activity_score + 1))
       - Larger timeframes (H1 > M5 > M1) get higher priority
       - timedelta ordering provides natural priority: larger timespan = higher priority
    3. Batches alert summaries to prevent flooding recipients with individual tick events

    The manager ensures notifications are sent efficiently while respecting cooldown
    and escalation logic defined in AggStates.
    """

    FLUSH_INTERVAL = timedelta(seconds=15)

    def __init__(self):
        # initialize TelegramNotifier backend from settings
        # initialize priority queue
        # initialize time for flush
        pass

    def enqueue(self, state: AggStates):
        """
        Add an AggStates notification to the priority queue.

        Args:
            state: AggStates containing symbol, timeframe, scores, and notification timestamps
        """
        pass

    def _format_batch_summary(self):
        """
        Format queued AggStates into a batched summary message.

        Groups states by symbol, orders by priority (magnitude and timeframe),
        and formats as a concise alert message.

        Example output:

        BTCUSD UP !! GBPUSD UP !!!

        # BTCUSD M1 UP
        Volatility: 120 pips since {V}, Activity: 300 ticks since {A}
        # BTCUSD M5 UP
        Volatility: 120 pips since {V}, Activity: 300 ticks since {A}
        ...
        """
        pass

    def flush(self):
        # compile batch msg
        # telegram send batch
        pass


notify_manager = NotificationManager()
