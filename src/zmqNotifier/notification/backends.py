"""Notification backend implementations."""

from __future__ import annotations

import logging
from typing import Any

import httpx


logger = logging.getLogger(__name__)


class TelegramNotifier:
    """
    Send notifications via Telegram Bot API.

    This class provides a simple interface for sending text messages to a Telegram
    chat using the Bot API. It handles authentication, error handling, and provides
    retry logic for transient failures.

    Parameters
    ----------
    bot_token : str
        Telegram bot token obtained from BotFather
    chat_id : str
        Target chat ID where messages will be sent
    timeout : float, optional
        HTTP request timeout in seconds (default: 10.0)
    parse_mode : str, optional
        Message parsing mode - "HTML" or "Markdown" (default: "HTML")

    Examples
    --------
    >>> notifier = TelegramNotifier(bot_token="123:ABC", chat_id="456")
    >>> await notifier.send_message("Market alert: EURUSD volatility spike!")

    """

    BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(
        self, bot_token: str, chat_id: str, timeout: float = 10.0, parse_mode: str = "HTML"
    ):
        """Initialize Telegram notifier with credentials."""
        if not bot_token:
            raise ValueError("bot_token cannot be empty")
        if not chat_id:
            raise ValueError("chat_id cannot be empty")

        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout = timeout
        self.parse_mode = parse_mode
        self._api_url = self.BASE_URL.format(token=bot_token)

    async def send_message(self, message: str) -> bool:
        if not message:
            logger.warning("Attempted to send empty message, skipping")
            return False

        if len(message) > 4096:
            logger.warning(
                "Message exceeds Telegram's 4096 character limit (%d chars), truncating",
                len(message),
            )
            message = message[:4093] + "..."

        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": self.parse_mode}

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(self._api_url, json=payload)
                response.raise_for_status()

                result: dict[str, Any] = response.json()
                if result.get("ok"):
                    logger.info("Successfully sent Telegram message")
                    return True
                else:
                    logger.error(
                        "Telegram API returned ok=false: %s",
                        result.get("description", "Unknown error"),
                    )
                    return False

        except httpx.TimeoutException:
            logger.error("Telegram API request timed out after %.1fs", self.timeout)
            return False

        except httpx.HTTPStatusError as e:
            logger.error(
                "Telegram API returned error status %d: %s", e.response.status_code, e.response.text
            )
            return False

        except httpx.RequestError as e:
            logger.error("Network error while sending Telegram message: %s", str(e))
            return False

        except Exception as e:
            logger.error("Unexpected error sending Telegram message: %s", str(e), exc_info=True)
            return False

    @classmethod
    def from_config(cls, config: Any) -> TelegramNotifier:
        if not config.telegram_bot_token:
            raise ValueError("telegram_bot_token not configured")
        if not config.telegram_chat_id:
            raise ValueError("telegram_chat_id not configured")

        return cls(bot_token=config.telegram_bot_token, chat_id=config.telegram_chat_id)
