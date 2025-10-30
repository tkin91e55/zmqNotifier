"""Configuration management for zmqNotifier using Pydantic Settings."""

from __future__ import annotations

import logging
import sys
from enum import Enum
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class StorageBackend(str, Enum):
    """Supported storage backends for market data."""

    CSV = "csv"
    SQLITE = "sqlite"


class BrokerSettings(BaseModel):
    """Broker-specific configuration."""

    model_config = ConfigDict(extra="ignore")

    brokertime_tz: int = Field(
        default=0,
        ge=-12,
        le=14,
        description="Broker timezone offset from UTC in hours (e.g., 11 for UTC+11).",
    )


class ZmqSettings(BaseModel):
    """ZeroMQ connection configuration."""

    model_config = ConfigDict(extra="ignore")

    host: str = Field("localhost", description="Hostname or IP address for the ZMQ server.")
    push_port: int = Field(32768, ge=1, le=65535, description="Port for PUSH socket.")
    pull_port: int = Field(32769, ge=1, le=65535, description="Port for PULL socket.")
    sub_port: int = Field(32770, ge=1, le=65535, description="Port for SUB socket.")
    client_id: str = Field("zmq-notifier", min_length=1, description="Client identifier.")


class StorageSettings(BaseModel):
    """Market data storage configuration."""

    model_config = ConfigDict(extra="ignore")

    data_path: Path = Field(
        default=Path("./data"), description="Root path for persisted market data."
    )
    backend: StorageBackend = Field(
        default=StorageBackend.CSV, description="Active storage backend."
    )
    compression_enabled: bool = Field(
        default=True, description="Enable monthly compression for stored data."
    )
    retention_days: int = Field(
        default=180, ge=1, description="Number of days to retain market data before cleanup."
    )
    flush_interval_minutes: int = Field(
        default=5, ge=1, description="Minutes between data buffer flushes to storage."
    )


class DataValidationSettings(BaseModel):
    """Validation thresholds for incoming market data."""

    model_config = ConfigDict(extra="ignore")

    supported_symbols: tuple[str, ...] = Field(
        (
            "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
            "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "GBPAUD",
            "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "AUDCAD", "AUDCHF", "AUDJPY",
            "AUDNZD", "NZDCAD", "NZDCHF", "NZDJPY", "CADCHF", "CADJPY", "CHFJPY",
            "BTCUSD", "XAUUSD", "USOUSD",
        ),
        description="MT4 Symbols allowed for incoming data.",
    )
    supported_timeframes: tuple[str, ...] = Field(
        ("M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN"),
        description="Supported chart timeframes.",
    )

    @field_validator("supported_timeframes")
    @classmethod
    def _normalize_timeframes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Ensure timeframe codes are uppercase."""
        return tuple(item.upper() for item in value)


class NotificationSettings(BaseModel):
    """Notification channel configuration."""

    model_config = ConfigDict(extra="ignore")

    enabled: bool = Field(default=False, description="Enable/disable all notifications globally.")
    telegram_bot_token: str | None = Field(
        default=None, description="Telegram bot token (if Telegram notifications are used)."
    )
    telegram_chat_id: str | None = Field(
        default=None, description="Telegram chat to receive notifications."
    )


class LoggingSettings(BaseModel):
    """Standard logging configuration exposed via settings."""

    model_config = ConfigDict(extra="ignore")

    level: str = Field(default="INFO", description="Root logger level.")
    fmt: str = Field(
        default="%(asctime)s %(levelname)s %(name)s: %(message)s",
        description="Logging format string.",
    )
    datefmt: str = Field(default="%Y-%m-%d %H:%M:%S", description="Datetime format used in logs.")
    log_dir: Path = Field(
        default=Path("./logs"),
        description="Directory for log files (relative paths resolved at runtime).",
    )
    file_name: str = Field(
        default="runtime.log", description="Filename for the rotating file handler."
    )
    max_bytes: int = Field(
        default=10 * 1024 * 1024, description="Maximum size per log file before rotation (bytes)."
    )
    backup_count: int = Field(default=5, description="Number of rotated log files to retain.")
    console_enabled: bool = Field(
        default=True, description="Emit logs to stdout in addition to file output."
    )
    console_level: str | None = Field(
        default=None, description="Optional override for console handler level."
    )
    file_level: str | None = Field(
        default=None, description="Optional override for file handler level."
    )
    propagate: bool = Field(
        default=True, description="Allow package loggers to propagate to root handlers."
    )
    loggers: dict[str, str] = Field(
        default_factory=lambda: {"zmqNotifier.market_data": "INFO", "zmqNotifier.zmq_cli": "INFO"},
        description="Per-logger level overrides (name -> level).",
    )


class AppSettings(BaseSettings):
    """Application settings with environment variable support."""

    model_config = SettingsConfigDict(
        env_prefix="ZMQ_NOTIFIER_",
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
        validate_assignment=True,
    )

    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    zmq: ZmqSettings = Field(default_factory=ZmqSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    validation: DataValidationSettings = Field(default_factory=DataValidationSettings)
    notifications: NotificationSettings = Field(default_factory=NotificationSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    auto_create_dirs: bool = Field(
        default=True, description="Create required directories automatically on settings load."
    )

    @model_validator(mode="after")
    def _materialise_directories(self) -> AppSettings:
        """Resolve and optionally create configured directories."""
        if not self.auto_create_dirs:
            return self

        storage_path = _ensure_directory(self.storage.data_path)
        object.__setattr__(
            self, "storage", self.storage.model_copy(update={"data_path": storage_path})
        )

        return self


def _ensure_directory(path: Path) -> Path:
    """Convert to an absolute path and ensure the directory exists."""
    resolved = _resolve_path(path)
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _resolve_path(path: Path) -> Path:
    """Convert a path to an absolute representation without touching the filesystem."""
    return path if path.is_absolute() else path.resolve()


class StdoutStreamHandler(logging.StreamHandler):
    """Stream handler that keeps stdout binding fresh for testing environments."""

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - thin wrapper
        self.stream = sys.stdout
        super().emit(record)


_LOGGING_CONFIGURED = False
_ACTIVE_LOGGING_SETTINGS: LoggingSettings | None = None


def configure_logging(logging_settings: LoggingSettings | None = None) -> LoggingSettings:
    """
    Initialise stdlib logging using values from :class:`LoggingSettings`.

    The function is idempotent—subsequent calls return the already-applied settings without
    reconfiguring handlers. When called with ``None`` (default) it obtains settings from
    :func:`get_settings`, allowing environment variables to drive configuration:

    - File logging is always configured using a rotating file handler rooted at
      ``logging.log_dir`` / ``logging.file_name``.
    - Console logging is optional and can be toggled or level-adjusted independently.
    - Package-specific levels are applied for any loggers named in ``logging.loggers``.

    Parameters
    ----------
    logging_settings:
        Optional explicit :class:`LoggingSettings` instance. If omitted, cached application
        settings are used.

    Returns
    -------
    LoggingSettings
        The active logging configuration instance applied to the process.

    """
    global _LOGGING_CONFIGURED
    global _ACTIVE_LOGGING_SETTINGS

    if _LOGGING_CONFIGURED:
        assert _ACTIVE_LOGGING_SETTINGS is not None
        return _ACTIVE_LOGGING_SETTINGS

    if logging_settings is None:
        logging_settings = get_settings().logging

    log_dir = logging_settings.log_dir
    if not log_dir.is_absolute():
        log_dir = (Path.cwd() / log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / logging_settings.file_name

    formatter = logging.Formatter(logging_settings.fmt, logging_settings.datefmt)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging_settings.level.upper())

    file_handler = RotatingFileHandler(
        str(log_file),
        maxBytes=logging_settings.max_bytes,
        backupCount=logging_settings.backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel((logging_settings.file_level or logging_settings.level).upper())
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    if logging_settings.console_enabled:
        console_handler = StdoutStreamHandler()
        console_handler.setLevel((logging_settings.console_level or logging_settings.level).upper())
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    for name, level in logging_settings.loggers.items():
        package_logger = logging.getLogger(name)
        package_logger.setLevel(level.upper())
        package_logger.propagate = logging_settings.propagate

    _ACTIVE_LOGGING_SETTINGS = logging_settings
    _LOGGING_CONFIGURED = True
    return logging_settings


@lru_cache
def get_settings(**overrides: object) -> AppSettings:
    """Return a cached instance of application settings."""
    return AppSettings(**overrides)


# Global settings instance (preserved for backwards compatibility)
settings = get_settings()
