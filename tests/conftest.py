"""Shared pytest fixtures."""
import pytest
from zmqNotifier import config as app_config


@pytest.fixture(autouse=True)
def reset_app_settings():
    """Ensure tests operate on a fresh settings instance."""
    app_config.get_settings.cache_clear()
    app_config.settings = app_config.get_settings()
    yield
    app_config.get_settings.cache_clear()
    app_config.settings = app_config.get_settings()
