"""
Notification system for market alerts and monitoring.

Detects unusual short-term price movement and trade activity using multi-dimensional scoring:

Scoring System:
    - Deep Score: Logarithmic threshold exceedance measuring immediate severity
      floor(log2(current_value / threshold))
    - Broad Score: Historical rarity via exponential lookback (1, 2, 4, 8... buckets)
      checking if current value exceeds 80% of historical data
    - Combined Scores: volatility_score = volatility_deep × volatility_broad
                      activity_score = activity_deep × activity_broad
                      overall_score = volatility_score × (activity_score + 1)

Alert Logic:
    - Monitors volatility (price fluctuations in pips) and activity (tick count)
    - Escalation: Higher scores break through active cooldowns
    - Stepdown: Scores reduce by 1 after cooldown expires without further escalation
    - Notifications sent only when scores escalate beyond previous levels

The system uses BucketedSlidingAggregator to track historical data across multiple
timeframes (M1, M5, M30, etc.) for each monitored symbol.
"""

from datetime import datetime, timedelta
from decimal import Decimal
import logging

from zmqNotifier.tick_agg import BucketedSlidingAggregator, AggStates, Message, init_msg_from_scores
from zmqNotifier.models import TickData
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


class SymbolTracker:
    """
    Per-symbol aggregator manager with multi-timeframe scoring and cooldown tracking.

    Manages BucketedSlidingAggregators for multiple timeframes (M1, M5, M30) to:
    1. Track min/max prices and tick counts across short windows
    2. Retain historical data in long-span buckets (configurable retention)
    3. Calculate multi-dimensional scores comparing current vs. historical data
    4. Apply escalation/cooldown logic to prevent notification spam

    The tracker maintains both aggregator state (price/tick data) and notification
    state (scores, cooldown timestamps) for each timeframe.

    Scoring Approach:
        - Each timeframe independently calculates volatility and activity scores
        - Scores are based on both magnitude (deep) and rarity (broad)
        - Higher scores represent more unusual market conditions
        - Escalation allows important alerts to break through cooldowns

    Example:
        M1 aggregator sees 10-pip movement exceeding threshold of 5 pips,
        and this movement exceeds 80% of historical data in last 4 buckets:
        - volatility_deep = floor(log2(10/5)) = 1
        - volatility_broad = 3 (spans checked: 1, 2, 4)
        - volatility_score = 1 × 3 = 3
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
        Calculate multi-dimensional scores and emit notifications for threshold breaches.

        For each timeframe aggregator:
        1. Verifies minimum bucket requirement is met
        2. Calculates Message with deep/broad scores via init_msg_from_scores()
        3. Checks if scores represent escalation via AggStates.should_notify()
        4. Updates cooldown state and enqueues notification if escalated

        Scoring Algorithm:
            Deep Score (immediate severity):
                volatility_deep = floor(log2(pip_change / threshold))
                activity_deep = floor(log2(tick_count / threshold))

            Broad Score (historical rarity):
                Exponential lookback checking 1, 2, 4, 8... buckets
                until finding span where current value DOES NOT exceed
                80% of historical maximum

            Combined:
                volatility_score = volatility_deep × volatility_broad
                activity_score = activity_deep × activity_broad
                overall_score = volatility_score × (activity_score + 1)

        Escalation Logic:
            - Notification sent when either volatility or activity score increases
            - Higher scores break through active cooldowns
            - State updated to new maximum scores
            - Well-formed messages logged and queued for NotificationManager

        Args:
            now: Current tick timestamp for scoring and cooldown checking
        """

        thresholds = self.thresholds
        if not thresholds:
            return

        tracker_config = self.tracker

        for tf, agg in self._aggregators.items():
            min_buckets_requirement = tracker_config.min_buckets_calculation
            if agg.buckets_count < min_buckets_requirement:
                continue

            state = self._agg_states[tf]
            msg = init_msg_from_scores(agg=agg, thresholds=thresholds.get(tf, (None, None)))

            state.stepdown(now)
            if not msg.is_significant():
                continue  # early leave

            state.trigger(now)  # msg is significant, postpone stepdown
            if not state.should_notify(msg):
                continue

            state.update(msg, now)

            msg.symbol = self._symbol
            msg.timeframe = tf
            msg.time = now
            msg.direction = "UP" if agg.get_active_direction() > 0 else "DOWN"

            if msg.is_well_formed():
                logger.info("Escalated alert for %s", msg)
                # TODO: Enqueue to NotificationManager

    def _get_timeframe_seconds(self, tf: str) -> int:
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
        cooldown_unit = tracker_config.cooldown_unit
        cooldown_seconds = cooldown_unit * self._get_timeframe_seconds(tf)
        self._agg_states[tf] = AggStates(cooldown_seconds=cooldown_seconds)

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

    def enqueue(self, msg: Message):
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
