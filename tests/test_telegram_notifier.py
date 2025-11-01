"""Tests for Telegram notification backend."""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from zmqNotifier.config import TelegramSettings
from zmqNotifier.notification.backends import TelegramNotifier


class TestTelegramNotifierInit:
    """Test TelegramNotifier initialization."""

    def test_init_with_valid_credentials(self):
        """Test initialization with valid bot token and chat ID."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")
        assert notifier.bot_token == "123:ABC"
        assert notifier.chat_id == "456"
        assert notifier.timeout == 10.0
        assert notifier.parse_mode == "HTML"
        assert notifier._api_url == "https://api.telegram.org/bot123:ABC/sendMessage"

    def test_init_with_custom_timeout(self):
        """Test initialization with custom timeout value."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456", timeout=30.0)
        assert notifier.timeout == 30.0

    def test_init_with_custom_parse_mode(self):
        """Test initialization with custom parse mode."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456", parse_mode="Markdown")
        assert notifier.parse_mode == "Markdown"

    def test_init_with_empty_bot_token(self):
        """Test initialization fails with empty bot token."""
        with pytest.raises(ValueError, match="bot_token cannot be empty"):
            TelegramNotifier(bot_token="", chat_id="456")

    def test_init_with_empty_chat_id(self):
        """Test initialization fails with empty chat ID."""
        with pytest.raises(ValueError, match="chat_id cannot be empty"):
            TelegramNotifier(bot_token="123:ABC", chat_id="")


class TestTelegramNotifierSendMessage:
    """Test TelegramNotifier.send_message method."""

    @pytest.mark.asyncio
    async def test_send_message_success(self):
        """Test successful message sending."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")

        mock_response = MagicMock()
        mock_response.json.return_value = {"ok": True}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context

            result = await notifier.send_message("Test message")

        assert result is True
        mock_context.__aenter__.return_value.post.assert_called_once()
        call_args = mock_context.__aenter__.return_value.post.call_args
        assert call_args[0][0] == notifier._api_url
        assert call_args[1]["json"] == {
            "chat_id": "456",
            "text": "Test message",
            "parse_mode": "HTML",
        }

    @pytest.mark.asyncio
    async def test_send_message_empty_string(self):
        """Test sending empty message returns False without API call."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")

        with patch("httpx.AsyncClient") as mock_client:
            result = await notifier.send_message("")

        assert result is False
        mock_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_message_truncates_long_messages(self):
        """Test messages longer than 4096 chars are truncated."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")

        long_message = "A" * 5000
        mock_response = MagicMock()
        mock_response.json.return_value = {"ok": True}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context

            result = await notifier.send_message(long_message)

        assert result is True
        call_args = mock_context.__aenter__.return_value.post.call_args
        sent_text = call_args[1]["json"]["text"]
        assert len(sent_text) == 4096
        assert sent_text.endswith("...")

    @pytest.mark.asyncio
    async def test_send_message_api_returns_ok_false(self):
        """Test handling when API returns ok=false."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "ok": False,
            "description": "Bad Request: chat not found",
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context

            result = await notifier.send_message("Test message")

        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_timeout(self):
        """Test handling of timeout errors."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")

        with patch("httpx.AsyncClient") as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(
                side_effect=httpx.TimeoutException("Timeout")
            )
            mock_client.return_value = mock_context

            result = await notifier.send_message("Test message")

        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_http_error(self):
        """Test handling of HTTP error responses."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"

        with patch("httpx.AsyncClient") as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(
                side_effect=httpx.HTTPStatusError(
                    "403 Forbidden", request=MagicMock(), response=mock_response
                )
            )
            mock_client.return_value = mock_context

            result = await notifier.send_message("Test message")

        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_network_error(self):
        """Test handling of network errors."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")

        with patch("httpx.AsyncClient") as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(
                side_effect=httpx.RequestError("Network error")
            )
            mock_client.return_value = mock_context

            result = await notifier.send_message("Test message")

        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_unexpected_error(self):
        """Test handling of unexpected exceptions."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")

        with patch("httpx.AsyncClient") as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(
                side_effect=RuntimeError("Unexpected error")
            )
            mock_client.return_value = mock_context

            result = await notifier.send_message("Test message")

        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_with_markdown(self):
        """Test sending message with Markdown parse mode."""
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456", parse_mode="Markdown")

        mock_response = MagicMock()
        mock_response.json.return_value = {"ok": True}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context

            result = await notifier.send_message("*Bold* _Italic_")

        assert result is True
        call_args = mock_context.__aenter__.return_value.post.call_args
        assert call_args[1]["json"]["parse_mode"] == "Markdown"


class TestTelegramNotifierFromConfig:
    """Test TelegramNotifier.from_config factory method."""

    def test_from_config_with_valid_settings(self):
        """Test creating notifier from valid config."""
        config = TelegramSettings(telegram_bot_token="123:ABC", telegram_chat_id="456")

        notifier = TelegramNotifier.from_config(config)

        assert notifier.bot_token == "123:ABC"
        assert notifier.chat_id == "456"

    def test_from_config_with_missing_token(self):
        """Test from_config fails when token is missing."""
        config = TelegramSettings(telegram_bot_token=None, telegram_chat_id="456")

        with pytest.raises(ValueError, match="telegram_bot_token not configured"):
            TelegramNotifier.from_config(config)

    def test_from_config_with_missing_chat_id(self):
        """Test from_config fails when chat_id is missing."""
        config = TelegramSettings(telegram_bot_token="123:ABC", telegram_chat_id=None)

        with pytest.raises(ValueError, match="telegram_chat_id not configured"):
            TelegramNotifier.from_config(config)

    def test_from_config_with_empty_token(self):
        """Test from_config fails when token is empty string."""
        config = TelegramSettings(telegram_bot_token="", telegram_chat_id="456")

        with pytest.raises(ValueError, match="telegram_bot_token not configured"):
            TelegramNotifier.from_config(config)

    def test_from_config_with_empty_chat_id(self):
        """Test from_config fails when chat_id is empty string."""
        config = TelegramSettings(telegram_bot_token="123:ABC", telegram_chat_id="")

        with pytest.raises(ValueError, match="telegram_chat_id not configured"):
            TelegramNotifier.from_config(config)
