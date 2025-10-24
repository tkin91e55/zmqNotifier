"""Tests for application configuration management."""

from __future__ import annotations

from zmqNotifier import config


def test_get_settings_returns_cached_instance() -> None:
    first = config.get_settings()
    second = config.get_settings()

    assert first is second


# def test_get_settings_creates_storage_directory(tmp_path) -> None:
#     storage_dir = tmp_path / "market_data"
#     assert not storage_dir.exists()

#     settings = config.get_settings(storage={"data_path": storage_dir})

#     assert settings.storage.data_path == storage_dir
#     assert storage_dir.exists()


# def test_get_settings_respects_auto_create_dirs_flag(tmp_path) -> None:
#     storage_dir = tmp_path / "no_create"
#     assert not storage_dir.exists()

#     settings = config.get_settings(storage={"data_path": storage_dir}, auto_create_dirs=False)

#     assert settings.storage.data_path == storage_dir
#     assert not storage_dir.exists()
