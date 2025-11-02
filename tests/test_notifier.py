"""Tests for notifier module: VolatilityNotifier, SymbolTracker, NotificationManager."""

from datetime import datetime, timedelta, UTC
from decimal import Decimal

import pytest

from zmqNotifier.config import (
    AppSettings,
    NotifierSettings,
    SymbolNotifierConfig,
    SymbolTrackerConfig,
)
from zmqNotifier.models import TickData
from zmqNotifier.notifier import SymbolTracker, VolatilityNotifier, AggStates


@pytest.fixture
def minimal_config():
    """Minimal configuration for testing SymbolTracker."""
    return AppSettings(
        notifier=NotifierSettings(
            symbols={"EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 100), "M5": (20, 200)})}
        )
    )


@pytest.fixture
def full_config():
    """Full configuration with tracker overrides."""
    return AppSettings(
        notifier=NotifierSettings(
            symbols={
                "BTCUSD": SymbolNotifierConfig(
                    thresholds={"M1": (50, 500), "M30": (200, 1), "M5": (100, 1000)},
                    tracker=SymbolTrackerConfig(
                        cooldown_unit=2,
                        min_buckets_calculation=20,
                        num_bucket_retention={"M1": 120, "M5": 60, "M30": 30},
                    ),
                )
            },
            tracker_defaults=SymbolTrackerConfig(
                cooldown_unit=1,
                min_buckets_calculation=30,
                num_bucket_retention={"M1": 60, "M5": 30, "M30": 10},
            ),
        )
    )


class TestVolatilityNotifier:
    """Test cases for VolatilityNotifier."""

    def test_initialization_with_no_config(self):
        """VolatilityNotifier should initialize with default config if none provided."""
        notifier = VolatilityNotifier()
        assert notifier.config is not None
        assert isinstance(notifier.config, AppSettings)
        assert notifier._trackers == {}

    def test_initialization_with_config(self, minimal_config):
        """VolatilityNotifier should initialize trackers for configured symbols."""
        notifier = VolatilityNotifier(config=minimal_config)
        assert notifier.config == minimal_config
        assert "EURUSD" in notifier._trackers
        assert isinstance(notifier._trackers["EURUSD"], SymbolTracker)

    def test_initialization_multiple_symbols(self):
        """VolatilityNotifier should create trackers for all configured symbols."""
        config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 1)}),
                    "GBPUSD": SymbolNotifierConfig(thresholds={"M5": (20, 1)}),
                }
            )
        )
        notifier = VolatilityNotifier(config=config)
        assert len(notifier._trackers) == 2
        assert "EURUSD" in notifier._trackers
        assert "GBPUSD" in notifier._trackers

    def test_update_config_add_new_symbol(self, minimal_config):
        """update_config should create tracker for newly added symbol."""
        notifier = VolatilityNotifier(config=minimal_config)
        assert len(notifier._trackers) == 1
        assert "EURUSD" in notifier._trackers

        # Add GBPUSD to config
        new_config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 100), "M5": (20, 200)}),
                    "GBPUSD": SymbolNotifierConfig(thresholds={"M1": (15, 150)}),
                }
            )
        )
        notifier.update_config(new_config)

        assert len(notifier._trackers) == 2
        assert "EURUSD" in notifier._trackers
        assert "GBPUSD" in notifier._trackers
        assert notifier.config == new_config

        # Verify GBPUSD tracker was initialized properly
        gbpusd_tracker = notifier._trackers["GBPUSD"]
        assert "M1" in gbpusd_tracker._aggregators

    def test_update_config_remove_symbol(self, minimal_config):
        """update_config should remove tracker when symbol removed from config."""
        # Start with EURUSD and GBPUSD
        config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 1)}),
                    "GBPUSD": SymbolNotifierConfig(thresholds={"M5": (20, 1)}),
                }
            )
        )
        notifier = VolatilityNotifier(config=config)
        assert len(notifier._trackers) == 2

        # Remove GBPUSD from config
        new_config = AppSettings(
            notifier=NotifierSettings(
                symbols={"EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 1)})}
            )
        )
        notifier.update_config(new_config)

        assert len(notifier._trackers) == 1
        assert "EURUSD" in notifier._trackers
        assert "GBPUSD" not in notifier._trackers

    def test_update_config_add_timeframe(self, minimal_config):
        """update_config should add aggregator when new timeframe added to symbol."""
        notifier = VolatilityNotifier(config=minimal_config)
        tracker = notifier._trackers["EURUSD"]
        assert set(tracker._aggregators.keys()) == {"M1", "M5"}

        # Add M30 timeframe
        new_config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(
                        thresholds={"M1": (10, 100), "M30": (50, 300), "M5": (20, 200)}
                    )
                }
            )
        )
        notifier.update_config(new_config)

        assert set(tracker._aggregators.keys()) == {"M1", "M5", "M30"}

    def test_update_config_remove_timeframe(self, minimal_config):
        """update_config should remove aggregator when timeframe removed from symbol."""
        notifier = VolatilityNotifier(config=minimal_config)
        tracker = notifier._trackers["EURUSD"]
        assert set(tracker._aggregators.keys()) == {"M1", "M5"}

        # Remove M5 timeframe
        new_config = AppSettings(
            notifier=NotifierSettings(
                symbols={"EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 100)})}
            )
        )
        notifier.update_config(new_config)

        assert set(tracker._aggregators.keys()) == {"M1"}
        assert "M5" not in tracker._aggregators

    def test_update_config_preserve_history(self, minimal_config):
        """update_config should preserve aggregator history for unchanged timeframes."""
        notifier = VolatilityNotifier(config=minimal_config)
        tracker = notifier._trackers["EURUSD"]

        # Add some ticks to M1 aggregator
        tick = TickData(datetime=datetime.now(UTC), bid=Decimal("1.1000"), ask=Decimal("1.1002"))
        tracker.on_tick(tick)

        # Store reference to original M1 aggregator
        original_m1_agg = tracker._aggregators["M1"]

        # Update config but keep M1 timeframe (only add M30)
        new_config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(
                        thresholds={"M1": (10, 100), "M30": (50, 300), "M5": (20, 200)}
                    )
                }
            )
        )
        notifier.update_config(new_config)

        # M1 aggregator should be the same object (history preserved)
        assert tracker._aggregators["M1"] is original_m1_agg

    def test_update_config_threshold_updates(self, minimal_config):
        """update_config should allow threshold changes to take effect immediately."""
        notifier = VolatilityNotifier(config=minimal_config)

        # Update thresholds to higher values
        new_config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(thresholds={"M1": (50, 500), "M5": (100, 1000)})
                }
            )
        )
        notifier.update_config(new_config)

        # Verify new thresholds are in effect
        thresholds = notifier.config.notifier.thresholds_for("EURUSD")
        assert thresholds["M1"] == (50, 500)
        assert thresholds["M5"] == (100, 1000)

    def test_update_config_idempotent(self, minimal_config):
        """update_config should be idempotent when called with same config."""
        notifier = VolatilityNotifier(config=minimal_config)
        tracker_before = notifier._trackers["EURUSD"]
        agg_before = tracker_before._aggregators["M1"]

        # Call update_config with same config
        notifier.update_config(minimal_config)

        # Should have same tracker and aggregator objects
        tracker_after = notifier._trackers["EURUSD"]
        agg_after = tracker_after._aggregators["M1"]

        assert tracker_after is tracker_before
        assert agg_after is agg_before

    def test_update_config_symbol_with_empty_thresholds(self, caplog):
        """update_config should remove all aggregators when symbol has empty thresholds."""
        config = AppSettings(
            notifier=NotifierSettings(
                symbols={"EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 1)})}
            )
        )
        notifier = VolatilityNotifier(config=config)

        # Update config with symbol that has empty thresholds (no timeframes configured)
        new_config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(thresholds={})  # Empty
                }
            )
        )
        notifier.update_config(new_config)

        # Tracker should exist but have no aggregators (all were removed)
        assert "EURUSD" in notifier._trackers
        tracker = notifier._trackers["EURUSD"]
        assert len(tracker._aggregators) == 0
        # The M1 aggregator was removed because it's no longer in config
        assert "Removing aggregator for EURUSD/M1" in caplog.text

    def test_update_config_complete_replacement(self):
        """update_config should completely replace symbol set."""
        config1 = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 1)}),
                    "GBPUSD": SymbolNotifierConfig(thresholds={"M1": (15, 1)}),
                }
            )
        )
        notifier = VolatilityNotifier(config=config1)
        assert len(notifier._trackers) == 2

        # Replace with completely different symbols
        config2 = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "BTCUSD": SymbolNotifierConfig(thresholds={"M5": (50, 1)}),
                    "XAUUSD": SymbolNotifierConfig(thresholds={"M5": (30, 1)}),
                }
            )
        )
        notifier.update_config(config2)

        assert len(notifier._trackers) == 2
        assert "BTCUSD" in notifier._trackers
        assert "XAUUSD" in notifier._trackers
        assert "EURUSD" not in notifier._trackers
        assert "GBPUSD" not in notifier._trackers


class TestSymbolTracker:
    """Test cases for SymbolTracker."""

    def test_initialization_with_no_thresholds(self, caplog):
        """SymbolTracker should warn if no thresholds configured."""
        config = AppSettings()
        notifier = VolatilityNotifier(config=config)
        tracker = SymbolTracker("EURUSD", notifier)

        assert tracker._symbol == "EURUSD"
        assert tracker._master == notifier
        assert len(tracker._aggregators) == 0
        assert "No thresholds configured for symbol EURUSD" in caplog.text

    def test_initialization_with_minimal_config(self, minimal_config):
        """SymbolTracker should create aggregators for all configured timeframes."""
        notifier = VolatilityNotifier(config=minimal_config)
        tracker = SymbolTracker("EURUSD", notifier)

        # Should have aggregators for M1 and M5 (union of volatility and activity thresholds)
        assert len(tracker._aggregators) == 2
        assert "M1" in tracker._aggregators
        assert "M5" in tracker._aggregators

    def test_initialization_with_full_config(self, full_config):
        """SymbolTracker should respect tracker config overrides."""
        notifier = VolatilityNotifier(config=full_config)
        tracker = SymbolTracker("BTCUSD", notifier)

        # Should have aggregators for M1, M5, M30 (union of thresholds)
        assert len(tracker._aggregators) == 3
        assert "M1" in tracker._aggregators
        assert "M5" in tracker._aggregators
        assert "M30" in tracker._aggregators

        # Verify aggregator bucket spans
        assert tracker._aggregators["M1"]._bucket_span == timedelta(minutes=1)
        assert tracker._aggregators["M5"]._bucket_span == timedelta(minutes=5)
        assert tracker._aggregators["M30"]._bucket_span == timedelta(minutes=30)

        # Verify max_window settings from tracker overrides
        assert tracker._aggregators["M1"]._max_window == 120
        assert tracker._aggregators["M5"]._max_window == 60
        assert tracker._aggregators["M30"]._max_window == 30

    def test_add_agg_new_timeframe(self, minimal_config):
        """add_agg should create new aggregator for timeframe."""
        notifier = VolatilityNotifier(config=minimal_config)
        tracker = SymbolTracker("EURUSD", notifier)

        # Add new timeframe
        tracker.add_agg("H1")

        assert "H1" in tracker._aggregators
        assert tracker._aggregators["H1"]._bucket_span == timedelta(hours=1)

    def test_add_agg_existing_timeframe(self, minimal_config, caplog):
        """add_agg should not replace existing aggregator."""
        import logging

        caplog.set_level(logging.DEBUG)

        notifier = VolatilityNotifier(config=minimal_config)
        tracker = SymbolTracker("EURUSD", notifier)

        original_agg = tracker._aggregators["M1"]
        tracker.add_agg("M1")

        # Should be the same object (not replaced)
        assert tracker._aggregators["M1"] is original_agg
        assert "Aggregator for EURUSD/M1 already exists" in caplog.text

    def test_remove_agg_existing_timeframe(self, minimal_config):
        """remove_agg should delete aggregator."""
        notifier = VolatilityNotifier(config=minimal_config)
        tracker = SymbolTracker("EURUSD", notifier)

        assert "M1" in tracker._aggregators
        tracker.remove_agg("M1")

        assert "M1" not in tracker._aggregators

    def test_remove_agg_nonexistent_timeframe(self, minimal_config, caplog):
        """remove_agg should handle non-existent timeframe gracefully."""
        import logging

        caplog.set_level(logging.DEBUG)

        notifier = VolatilityNotifier(config=minimal_config)
        tracker = SymbolTracker("EURUSD", notifier)

        tracker.remove_agg("H4")
        assert "No aggregator found for EURUSD/H4 to remove" in caplog.text

    def test_aggregator_retention_with_defaults(self, minimal_config):
        """Aggregators should use default retention when no overrides specified."""
        # Use default tracker settings from config
        notifier = VolatilityNotifier(config=minimal_config)
        tracker = SymbolTracker("EURUSD", notifier)

        # M1 should have default retention from tracker_defaults
        # Default is 60*24*7*4 = 40320 for M1
        expected_m1_retention = 60 * 24 * 7 * 4
        assert tracker._aggregators["M1"]._max_window == expected_m1_retention

    def test_timeframe_from_volatility_only(self):
        """Tracker should create aggregators for timeframes with only volatility threshold."""
        config = AppSettings(
            notifier=NotifierSettings(
                symbols={"EURUSD": SymbolNotifierConfig(thresholds={"M1": (10, 1)})}
            )
        )
        notifier = VolatilityNotifier(config=config)
        tracker = SymbolTracker("EURUSD", notifier)

        assert "M1" in tracker._aggregators
        assert len(tracker._aggregators) == 1

    def test_timeframe_from_activity_only(self):
        """Tracker should create aggregators for timeframes with only activity threshold."""
        config = AppSettings(
            notifier=NotifierSettings(
                symbols={"EURUSD": SymbolNotifierConfig(thresholds={"M5": (1, 100)})}
            )
        )
        notifier = VolatilityNotifier(config=config)
        tracker = SymbolTracker("EURUSD", notifier)

        assert "M5" in tracker._aggregators
        assert len(tracker._aggregators) == 1

    def test_timeframe_union_of_thresholds(self):
        """Tracker should create aggregators for union of volatility and activity timeframes."""
        config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(
                        thresholds={"M1": (10, 1), "M30": (1, 200), "M5": (20, 100)}
                    )
                }
            )
        )
        notifier = VolatilityNotifier(config=config)
        tracker = SymbolTracker("EURUSD", notifier)

        # Should have M1, M5, M30
        assert len(tracker._aggregators) == 3
        assert "M1" in tracker._aggregators
        assert "M5" in tracker._aggregators
        assert "M30" in tracker._aggregators


class TestAggStates:
    """Test cases for AggStates dataclass."""

    def test_magnitude_calculation(self):
        """magnitude should correctly calculate combined score."""
        state = AggStates(
            volatility_score=2,
            activity_score=1,
        )
        # magnitude = volatility_score * (activity_score + 1) = 2 * (1 + 1) = 4
        assert state.magnitude() == 4

    def test_magnitude_zero_activity(self):
        """magnitude should handle zero activity score."""
        state = AggStates(
            volatility_score=3,
            activity_score=0,
        )
        # magnitude = 3 * (0 + 1) = 3
        assert state.magnitude() == 3


class TestSymbolTrackerOnTick:
    """Test cases for SymbolTracker.on_tick() and _calculate()."""

    @pytest.fixture
    def tracker_with_config(self):
        """Create tracker with known thresholds."""
        config = AppSettings(
            notifier=NotifierSettings(
                symbols={
                    "EURUSD": SymbolNotifierConfig(
                        thresholds={"M1": (10, 50)}, tracker=SymbolTrackerConfig(cooldown_unit=1)
                    )
                }
            )
        )
        notifier = VolatilityNotifier(config=config)
        return notifier._trackers["EURUSD"]

    def test_on_tick_feeds_aggregators(self, tracker_with_config):
        """on_tick should feed mid-price to all aggregators."""
        tick = TickData(
            datetime=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            bid=Decimal("1.1000"),
            ask=Decimal("1.1002"),
        )

        tracker_with_config.on_tick(tick)

        # Verify aggregator received the tick
        agg = tracker_with_config._aggregators["M1"]
        min_val, max_val, count = agg.query_min_max(num_buckets=0)

        assert min_val == Decimal("1.1001")  # Mid-price
        assert max_val == Decimal("1.1001")
        assert count == 1

    def test_on_tick_no_aggregators(self, minimal_config):
        """on_tick should handle empty aggregators gracefully."""
        notifier = VolatilityNotifier(config=minimal_config)
        tracker = SymbolTracker("UNKNOWN", notifier)

        # Should not raise
        tick = TickData(
            datetime=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            bid=Decimal("1.1000"),
            ask=Decimal("1.1002"),
        )
        tracker.on_tick(tick)

    def test_calculate_no_thresholds(self, minimal_config):
        """_calculate should return None if no thresholds configured."""
        notifier = VolatilityNotifier(config=AppSettings())
        tracker = SymbolTracker("EURUSD", notifier)

        score = tracker._calculate(datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC))
        assert score is None


class TestVolatilityNotifierOnTick:
    """Test cases for VolatilityNotifier.on_tick()."""

    def test_on_tick_routes_to_tracker(self, minimal_config):
        """on_tick should route tick to correct tracker."""
        notifier = VolatilityNotifier(config=minimal_config)

        tick = TickData(
            datetime=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            bid=Decimal("1.1000"),
            ask=Decimal("1.1002"),
        )

        notifier.on_tick("EURUSD", tick)

        # Verify tracker received the tick
        tracker = notifier._trackers["EURUSD"]
        agg = tracker._aggregators["M1"]
        min_val, max_val, count = agg.query_min_max(num_buckets=0)

        assert count == 1
        assert min_val == Decimal("1.1001")

    def test_on_tick_unknown_symbol(self, minimal_config, caplog):
        """on_tick should warn for unknown symbol."""
        notifier = VolatilityNotifier(config=minimal_config)

        tick = TickData(
            datetime=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            bid=Decimal("1.1000"),
            ask=Decimal("1.1002"),
        )

        notifier.on_tick("UNKNOWN", tick)
        assert "No tracker configured for symbol UNKNOWN" in caplog.text
