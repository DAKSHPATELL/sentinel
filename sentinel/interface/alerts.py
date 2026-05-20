"""
SENTINEL Alert System.
Multi-channel alerting: desktop notifications, Telegram, Slack, email.
Rate-limited to prevent alert fatigue.
"""
from __future__ import annotations

import asyncio
import subprocess
from collections import deque
from datetime import datetime, timedelta
from typing import Optional

import httpx
import structlog

from sentinel.config import get_config
from sentinel.models import AlertPriority, Signal

logger = structlog.get_logger(__name__)

# Priority to emoji mapping for Telegram/Slack
PRIORITY_EMOJI = {
    AlertPriority.CRITICAL: "\u2757\u2757",  # ‼️
    AlertPriority.HIGH: "\u26a0\ufe0f",  # ⚠️
    AlertPriority.MEDIUM: "\U0001f535",  # 🔵
    AlertPriority.LOW: "\u2b55",  # ⭕
}


class AlertManager:
    """
    Sends alerts through configured channels with rate limiting.

    Supports:
    - Desktop notifications (macOS native via osascript)
    - Telegram bot messages
    - Slack webhooks
    - Email (SMTP)
    """

    def __init__(self) -> None:
        self._config = get_config().interface.alerts
        self._recent_alerts: deque[datetime] = deque(maxlen=200)

    def _should_alert(self, signal: Signal) -> bool:
        """Check rate limits and priority filter."""
        # Priority filter
        priority_order = [AlertPriority.LOW, AlertPriority.MEDIUM, AlertPriority.HIGH, AlertPriority.CRITICAL]
        min_idx = next(
            (i for i, p in enumerate(priority_order) if p.value == self._config.min_priority),
            1,
        )
        signal_idx = next(
            (i for i, p in enumerate(priority_order) if p == signal.priority),
            0,
        )
        if signal_idx < min_idx:
            return False

        # Rate limit
        now = datetime.utcnow()
        cutoff = now - timedelta(hours=1)
        while self._recent_alerts and self._recent_alerts[0] < cutoff:
            self._recent_alerts.popleft()

        if len(self._recent_alerts) >= self._config.max_alerts_per_hour:
            logger.warning("alert_rate_limited", count=len(self._recent_alerts))
            return False

        return True

    async def send_alert(self, signal: Signal) -> None:
        """Send alert through all configured channels."""
        if not self._config.enabled:
            return

        if not self._should_alert(signal):
            return

        self._recent_alerts.append(datetime.utcnow())
        channels = self._config.channels

        tasks = []
        if "desktop" in channels:
            tasks.append(self._desktop_notify(signal))
        if "telegram" in channels and self._config.telegram.bot_token:
            tasks.append(self._telegram_notify(signal))
        if "slack" in channels and self._config.slack.webhook_url:
            tasks.append(self._slack_notify(signal))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _desktop_notify(self, signal: Signal) -> None:
        """macOS native desktop notification via osascript."""
        try:
            emoji = PRIORITY_EMOJI.get(signal.priority, "")
            title = f"SENTINEL {emoji} {signal.priority.value.upper()}"
            message = signal.title[:200]

            cmd = [
                "osascript", "-e",
                f'display notification "{message}" with title "{title}" sound name "Glass"'
            ]
            subprocess.run(cmd, timeout=5, capture_output=True)
            logger.debug("desktop_alert_sent", signal_id=str(signal.id))
        except Exception as e:
            logger.error("desktop_alert_failed", error=str(e))

    async def _telegram_notify(self, signal: Signal) -> None:
        """Send alert via Telegram bot."""
        tg = self._config.telegram
        if not tg.bot_token or not tg.chat_id:
            return

        emoji = PRIORITY_EMOJI.get(signal.priority, "")
        text = (
            f"{emoji} *SENTINEL — {signal.signal_type.value.upper()}*\n\n"
            f"*{signal.title}*\n\n"
            f"{signal.description[:500]}\n\n"
            f"Confidence: {signal.confidence:.0%} | Priority: {signal.priority.value}\n"
            f"Entities: {', '.join(signal.entities[:5])}"
        )

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"https://api.telegram.org/bot{tg.bot_token}/sendMessage",
                    json={
                        "chat_id": tg.chat_id,
                        "text": text,
                        "parse_mode": "Markdown",
                    },
                )
            logger.debug("telegram_alert_sent", signal_id=str(signal.id))
        except Exception as e:
            logger.error("telegram_alert_failed", error=str(e))

    async def _slack_notify(self, signal: Signal) -> None:
        """Send alert via Slack webhook."""
        webhook = self._config.slack.webhook_url
        if not webhook:
            return

        emoji = PRIORITY_EMOJI.get(signal.priority, "")
        text = (
            f"{emoji} *SENTINEL — {signal.signal_type.value.upper()}*\n"
            f">{signal.title}\n"
            f">{signal.description[:300]}\n"
            f"Confidence: {signal.confidence:.0%} | Entities: {', '.join(signal.entities[:5])}"
        )

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(webhook, json={"text": text})
            logger.debug("slack_alert_sent", signal_id=str(signal.id))
        except Exception as e:
            logger.error("slack_alert_failed", error=str(e))
