"""Configuration management for zmqNotifier using Pydantic Settings."""

from __future__ import annotations

from enum import Enum
from functools import lru_cache
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
        default=Path("./data"), description="Root path for persisted market data.",
    )
    backend: StorageBackend = Field(
        default=StorageBackend.CSV, description="Active storage backend.",
    )
    compression_enabled: bool = Field(
        default=True, description="Enable compression for stored data.",
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

    telegram_bot_token: str | None = Field(
        default=None,
        description="Telegram bot token (if Telegram notifications are used).",
    )
    telegram_chat_id: str | None = Field(
        default=None,
        description="Telegram chat to receive notifications.",
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

    zmq: ZmqSettings = Field(default_factory=ZmqSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    validation: DataValidationSettings = Field(default_factory=DataValidationSettings)
    notifications: NotificationSettings = Field(default_factory=NotificationSettings)
    auto_create_dirs: bool = Field(
        default=True,
        description="Create required directories automatically on settings load.",
    )

    @model_validator(mode="after")
    def _materialise_directories(self) -> AppSettings:
        """Resolve and optionally create configured directories."""
        if not self.auto_create_dirs:
            return self

        storage_path = _ensure_directory(self.storage.data_path)
        storage = self.storage.model_copy(update={"data_path": storage_path})

        return self.model_copy(update={"storage": storage})


def _ensure_directory(path: Path) -> Path:
    """Convert to an absolute path and ensure the directory exists."""
    resolved = _resolve_path(path)
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _resolve_path(path: Path) -> Path:
    """Convert a path to an absolute representation without touching the filesystem."""
    return path if path.is_absolute() else path.resolve()


@lru_cache
def get_settings(**overrides: object) -> AppSettings:
    """Return a cached instance of application settings."""
    return AppSettings(**overrides)


# Global settings instance (preserved for backwards compatibility)
settings = get_settings()
