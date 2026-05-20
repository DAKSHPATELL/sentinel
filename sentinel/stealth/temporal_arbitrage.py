"""
SENTINEL Temporal Arbitrage Scheduler.
Off-peak crawl scheduling based on per-domain hourly success histograms.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient

logger = structlog.get_logger(__name__)


class TemporalArbitrageScheduler:
    """
    Schedule retries at domains' off-peak hours.

    For each domain, tracks HTTP response success rates by hour-of-day.
    When all strategies fail, schedules retries at the hour with the
    highest historical success rate.
    """

    def __init__(self, duckdb_client: DuckDBClient) -> None:
        self._duckdb = duckdb_client
        self._config = get_config()
        self._retry_counts: dict[str, int] = {}  # url -> retry count

    def initialize(self) -> None:
        """Create DuckDB table for hourly domain stats."""
        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS domain_hourly_stats (
                domain VARCHAR NOT NULL,
                hour_of_day INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                attempts INTEGER DEFAULT 0,
                successes INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (domain, hour_of_day, day_of_week)
            )
        """)

    def record_attempt(
        self, domain: str, success: bool, timestamp: Optional[datetime] = None
    ) -> None:
        """
        Record a crawl attempt for hourly histogram.

        Args:
            domain: Target domain.
            success: Whether the attempt succeeded.
            timestamp: When the attempt occurred (defaults to now).
        """
        ts = timestamp or datetime.utcnow()
        hour = ts.hour
        dow = ts.weekday()

        try:
            self._duckdb.execute(
                """
                INSERT INTO domain_hourly_stats (domain, hour_of_day, day_of_week, attempts, successes, updated_at)
                VALUES (?, ?, ?, 1, ?, now())
                ON CONFLICT (domain, hour_of_day, day_of_week) DO UPDATE SET
                    attempts = domain_hourly_stats.attempts + 1,
                    successes = domain_hourly_stats.successes + excluded.successes,
                    updated_at = now()
                """,
                (domain, hour, dow, int(success)),
            )
        except Exception as e:
            logger.debug("temporal_record_failed", domain=domain, error=str(e))

    def get_best_hour(self, domain: str) -> Optional[int]:
        """
        Get the hour with highest success rate for a domain.

        Args:
            domain: Target domain.

        Returns:
            Best hour (0-23), or None if insufficient data.
        """
        try:
            rows = self._duckdb.query(
                """
                SELECT hour_of_day, SUM(attempts) AS total_attempts, SUM(successes) AS total_successes
                FROM domain_hourly_stats
                WHERE domain = ?
                GROUP BY hour_of_day
                HAVING total_attempts >= ?
                ORDER BY (total_successes * 1.0 / total_attempts) DESC
                LIMIT 1
                """,
                (domain, self._config.stealth.temporal_arbitrage.min_success_data_points),
            )
            if rows:
                return rows[0]["hour_of_day"]
        except Exception as e:
            logger.debug("get_best_hour_failed", domain=domain, error=str(e))
        return None

    def get_hourly_histogram(self, domain: str) -> list[dict]:
        """
        Get full 24-hour success rate histogram for a domain.

        Returns:
            List of {hour, attempts, successes, rate} dicts.
        """
        try:
            rows = self._duckdb.query(
                """
                SELECT hour_of_day, SUM(attempts) AS attempts, SUM(successes) AS successes
                FROM domain_hourly_stats
                WHERE domain = ?
                GROUP BY hour_of_day
                ORDER BY hour_of_day
                """,
                (domain,),
            )
            return [
                {
                    "hour": row["hour_of_day"],
                    "attempts": row["attempts"],
                    "successes": row["successes"],
                    "rate": row["successes"] / max(row["attempts"], 1),
                }
                for row in rows
            ]
        except Exception:
            return []

    def compute_next_retry_time(self, domain: str) -> Optional[datetime]:
        """
        Compute the next optimal retry time for a domain.

        Uses hourly success histogram to find best hour,
        then calculates next occurrence of that hour.

        Args:
            domain: Target domain.

        Returns:
            Next retry datetime, or None if insufficient data.
        """
        best_hour = self.get_best_hour(domain)
        if best_hour is None:
            # No data — retry in 4 hours (default off-peak guess)
            return datetime.utcnow() + timedelta(hours=4)

        now = datetime.utcnow()
        # Find next occurrence of best_hour
        next_time = now.replace(hour=best_hour, minute=0, second=0, microsecond=0)
        if next_time <= now:
            next_time += timedelta(days=1)

        return next_time

    def should_retry(self, url: str) -> bool:
        """
        Check if URL should be retried (under max retry limit).

        Args:
            url: URL to check.

        Returns:
            True if retry allowed.
        """
        max_retries = self._config.stealth.temporal_arbitrage.max_retries_per_url
        return self._retry_counts.get(url, 0) < max_retries

    def record_retry(self, url: str) -> None:
        """Increment retry count for a URL."""
        self._retry_counts[url] = self._retry_counts.get(url, 0) + 1

    def get_retry_count(self, url: str) -> int:
        """Get current retry count for URL."""
        return self._retry_counts.get(url, 0)
