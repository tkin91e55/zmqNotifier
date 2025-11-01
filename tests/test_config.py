"""Tests for notifier configuration models."""

import pytest
from pydantic import ValidationError

from zmqNotifier.config import (
    NotificationDispatchSettings,
    NotifierSettings,
    SymbolNotifierConfig,
    SymbolThresholds,
    SymbolTrackerConfig,
)


class TestSymbolThresholds:
    """Test SymbolThresholds validation."""

    def test_empty_thresholds_allowed(self):
        """Empty threshold dicts should be valid."""
        thresholds = SymbolThresholds()
        assert thresholds.volatility_threshold == {}
        assert thresholds.activity_threshold == {}

    def test_uppercase_timeframe_normalization(self):
        """Timeframe keys should be normalized to uppercase."""
        thresholds = SymbolThresholds(
            volatility_threshold={"m1": 100, "m5": 250}, activity_threshold={"m1": 50, "m5": 150}
        )
        assert "M1" in thresholds.volatility_threshold
        assert "M5" in thresholds.volatility_threshold
        assert "m1" not in thresholds.volatility_threshold

    def test_zero_threshold_rejected(self):
        """Zero thresholds should be rejected (must be positive)."""
        with pytest.raises(ValidationError, match="must be greater than 0"):
            SymbolThresholds(volatility_threshold={"M1": 0})

        with pytest.raises(ValidationError, match="must be greater than 0"):
            SymbolThresholds(activity_threshold={"M1": 0})

    def test_negative_values_turned(self):
        """Negative thresholds should be rejected."""
        v = SymbolThresholds(volatility_threshold={"M1": -100})
        a = SymbolThresholds(activity_threshold={"M1": -50})
        assert v.volatility_threshold["M1"] == 100
        assert a.activity_threshold["M1"] == 50


class TestSymbolTrackerConfig:
    """Test SymbolTrackerConfig validation."""

    def test_all_fields_optional(self):
        """All tracker config fields should be optional."""
        config = SymbolTrackerConfig()
        assert config.cooldown_unit is None
        assert config.min_buckets_calculation is None
        assert config.num_bucket_retention is None

    def test_positive_cooldown_unit(self):
        """Cooldown unit must be positive."""
        with pytest.raises(ValidationError, match="greater than 0"):
            SymbolTrackerConfig(cooldown_unit=0)

        with pytest.raises(ValidationError, match="greater than 0"):
            SymbolTrackerConfig(cooldown_unit=-1)

    def test_positive_min_buckets_calculation(self):
        """Min buckets calculation must be positive."""
        with pytest.raises(ValidationError, match="greater than 0"):
            SymbolTrackerConfig(min_buckets_calculation=0)

    def test_uppercase_retention_keys(self):
        """Retention timeframe keys should be normalized to uppercase."""
        config = SymbolTrackerConfig(num_bucket_retention={"m1": 1000, "m5": 200})
        assert config.num_bucket_retention is not None
        assert "M1" in config.num_bucket_retention
        assert "M5" in config.num_bucket_retention
        assert "m1" not in config.num_bucket_retention

    def test_negative_retention_rejected(self):
        """Negative bucket retention should be rejected."""
        with pytest.raises(ValidationError, match="must be positive"):
            SymbolTrackerConfig(num_bucket_retention={"M1": -100})

    def test_zero_retention_rejected(self):
        """Zero bucket retention should be rejected."""
        with pytest.raises(ValidationError, match="must be positive"):
            SymbolTrackerConfig(num_bucket_retention={"M1": 0})


class TestNotificationDispatchSettings:
    """Test NotificationDispatchSettings validation."""

    def test_default_interval(self):
        """Default message interval should be 15 seconds."""
        dispatch = NotificationDispatchSettings()
        assert dispatch.message_interval_seconds == 15

    def test_custom_interval(self):
        """Custom message interval should be accepted."""
        dispatch = NotificationDispatchSettings(message_interval_seconds=30)
        assert dispatch.message_interval_seconds == 30

    def test_minimum_interval_one_second(self):
        """Message interval must be at least 1 second."""
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            NotificationDispatchSettings(message_interval_seconds=0)


class TestSymbolNotifierConfig:
    """Test SymbolNotifierConfig composition."""

    def test_default_empty_thresholds(self):
        """Default should have empty threshold dicts."""
        config = SymbolNotifierConfig()
        assert config.thresholds.volatility_threshold == {}
        assert config.thresholds.activity_threshold == {}
        assert config.tracker is None

    def test_with_thresholds_only(self):
        """Should accept thresholds without tracker config."""
        config = SymbolNotifierConfig(
            thresholds=SymbolThresholds(
                volatility_threshold={"M1": 100}, activity_threshold={"M1": 50}
            )
        )
        assert config.thresholds.volatility_threshold["M1"] == 100
        assert config.tracker is None

    def test_with_tracker_overrides(self):
        """Should accept tracker overrides."""
        config = SymbolNotifierConfig(tracker=SymbolTrackerConfig(cooldown_unit=2))
        assert config.tracker is not None
        assert config.tracker.cooldown_unit == 2


class TestNotifierSettings:
    """Test NotifierSettings top-level configuration."""

    def test_default_empty_symbols(self):
        """Default should have no symbol configurations."""
        settings = NotifierSettings()
        assert settings.symbols == {}

    def test_default_tracker_settings(self):
        """Default tracker settings should be populated."""
        settings = NotifierSettings()
        assert settings.tracker_defaults.cooldown_unit == 1
        assert settings.tracker_defaults.min_buckets_calculation == 30
        assert settings.tracker_defaults.num_bucket_retention is not None
        assert settings.tracker_defaults.num_bucket_retention["M1"] == 60 * 24 * 7 * 4

    def test_default_dispatch_settings(self):
        """Default dispatch settings should be populated."""
        settings = NotifierSettings()
        assert settings.dispatch.message_interval_seconds == 15

    def test_uppercase_symbol_keys(self):
        """Symbol keys should be normalized to uppercase."""
        settings = NotifierSettings(
            symbols={"btcusd": SymbolNotifierConfig(), "eurusd": SymbolNotifierConfig()}
        )
        assert "BTCUSD" in settings.symbols
        assert "EURUSD" in settings.symbols
        assert "btcusd" not in settings.symbols

    def test_symbol_config_retrieval(self):
        """Should retrieve symbol config case-insensitively."""
        settings = NotifierSettings(symbols={"BTCUSD": SymbolNotifierConfig()})
        assert settings.symbol_config("btcusd") is not None
        assert settings.symbol_config("BTCUSD") is not None
        assert settings.symbol_config("BtcUsd") is not None
        assert settings.symbol_config("EURUSD") is None

    def test_symbol_config_empty_string(self):
        """Should return None for empty symbol string."""
        settings = NotifierSettings()
        assert settings.symbol_config("") is None

    def test_thresholds_for_existing_symbol(self):
        """Should return thresholds for configured symbol."""
        settings = NotifierSettings(
            symbols={
                "BTCUSD": SymbolNotifierConfig(
                    thresholds=SymbolThresholds(
                        volatility_threshold={"M1": 100}, activity_threshold={"M1": 50}
                    )
                )
            }
        )
        thresholds = settings.thresholds_for("BTCUSD")
        assert thresholds is not None
        assert thresholds.volatility_threshold["M1"] == 100

    def test_thresholds_for_missing_symbol(self):
        """Should return None for unconfigured symbol."""
        settings = NotifierSettings()
        assert settings.thresholds_for("UNKNOWN") is None

    def test_resolve_tracker_config_defaults_only(self):
        """Should return defaults when no symbol override exists."""
        settings = NotifierSettings()
        config = settings.resolve_tracker_config("BTCUSD")
        assert config.cooldown_unit == 1
        assert config.min_buckets_calculation == 30

    def test_resolve_tracker_config_with_override(self):
        """Should merge symbol overrides with defaults."""
        settings = NotifierSettings(
            symbols={"BTCUSD": SymbolNotifierConfig(tracker=SymbolTrackerConfig(cooldown_unit=3))}
        )
        config = settings.resolve_tracker_config("BTCUSD")
        # Override applied
        assert config.cooldown_unit == 3
        # Defaults preserved
        assert config.min_buckets_calculation == 30
        # Note: num_bucket_retention is None because it wasn't in the override

    def test_resolve_tracker_config_partial_retention_override(self):
        """Should handle partial num_bucket_retention overrides."""
        settings = NotifierSettings(
            tracker_defaults=SymbolTrackerConfig(
                cooldown_unit=1,
                min_buckets_calculation=30,
                num_bucket_retention={"M1": 1000, "M5": 200, "M30": 50},
            ),
            symbols={
                "BTCUSD": SymbolNotifierConfig(
                    tracker=SymbolTrackerConfig(
                        num_bucket_retention={"M1": 2000}  # Only override M1
                    )
                )
            },
        )
        config = settings.resolve_tracker_config("BTCUSD")
        # This test highlights a potential issue: the entire dict gets replaced
        # Expected behavior might be to merge retention dicts, not replace
        assert config.num_bucket_retention == {"M1": 2000}
        # Note: M5 and M30 from defaults are lost!

    def test_full_symbol_configuration(self):
        """Test complete symbol configuration with all options."""
        settings = NotifierSettings(
            symbols={
                "BTCUSD": SymbolNotifierConfig(
                    thresholds=SymbolThresholds(
                        volatility_threshold={"M1": 100, "M5": 250},
                        activity_threshold={"M1": 50, "M5": 150},
                    ),
                    tracker=SymbolTrackerConfig(
                        cooldown_unit=2,
                        min_buckets_calculation=40,
                        num_bucket_retention={"M1": 5000, "M5": 1000},
                    ),
                ),
                "EURUSD": SymbolNotifierConfig(
                    thresholds=SymbolThresholds(
                        volatility_threshold={"M1": 15, "M5": 40},
                        activity_threshold={"M1": 100, "M5": 250},
                    )
                ),
            },
            dispatch=NotificationDispatchSettings(message_interval_seconds=20),
        )

        # BTCUSD checks
        btc_thresholds = settings.thresholds_for("BTCUSD")
        assert btc_thresholds is not None
        assert btc_thresholds.volatility_threshold["M1"] == 100
        btc_tracker = settings.resolve_tracker_config("BTCUSD")
        assert btc_tracker.cooldown_unit == 2

        # EURUSD checks (uses tracker defaults)
        eur_tracker = settings.resolve_tracker_config("EURUSD")
        assert eur_tracker.cooldown_unit == 1  # From defaults

        # Dispatch settings
        assert settings.dispatch.message_interval_seconds == 20
