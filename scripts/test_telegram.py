#!/usr/bin/env python3
"""Manual test script for Telegram notifications.

This script loads credentials from .env and sends a test message.
Run with: poetry run python scripts/test_telegram.py
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for direct module import
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from zmqNotifier.config import get_settings
from zmqNotifier.notification.backends import TelegramNotifier


async def main():
    """Send a test message to Telegram."""
    print("Loading configuration from .env...")
    settings = get_settings()

    if not settings.notifications.telegram_bot_token:
        print("ERROR: TELEGRAM_BOT_TOKEN not found in .env")
        print("Please set: ZMQ_NOTIFIER_NOTIFICATIONS__TELEGRAM_BOT_TOKEN=your_token")
        return 1

    if not settings.notifications.telegram_chat_id:
        print("ERROR: TELEGRAM_CHAT_ID not found in .env")
        print("Please set: ZMQ_NOTIFIER_NOTIFICATIONS__TELEGRAM_CHAT_ID=your_chat_id")
        return 1

    print(f"Bot token: {settings.notifications.telegram_bot_token[:10]}...")
    print(f"Chat ID: {settings.notifications.telegram_chat_id}")

    print("\nCreating TelegramNotifier...")
    notifier = TelegramNotifier.from_config(settings.notifications)

    print("Sending test message...")
    success = await notifier.send_message(
        "<b>Test Message from zmqNotifier</b>\n\n"
        "This is a test notification to verify Telegram integration is working.\n\n"
        "Features:\n"
        "✓ HTML formatting support\n"
        "✓ Error handling\n"
        "✓ Config integration"
    )

    if success:
        print("✅ Message sent successfully!")
        return 0
    else:
        print("❌ Failed to send message. Check logs above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
