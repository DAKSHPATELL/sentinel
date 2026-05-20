"""
SENTINEL rate limiter.
Token bucket per domain with adaptive rate limiting.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)


class DomainBucket:
    """Token bucket for a single domain."""

    def __init__(self, rate_per_minute: float, burst_multiplier: float = 2.0) -> None:
        """
        Initialize a token bucket.

        Args:
            rate_per_minute: Tokens added per minute.
            burst_multiplier: Maximum burst capacity as multiplier of rate.
        """
        self.rate_per_minute = rate_per_minute
        self.max_tokens = rate_per_minute * burst_multiplier
        self.tokens = self.max_tokens
        self.last_refill = time.monotonic()
        self.last_429: Optional[float] = None
        self.original_rate = rate_per_minute

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed_minutes = (now - self.last_refill) / 60.0
        self.tokens = min(self.max_tokens, self.tokens + elapsed_minutes * self.rate_per_minute)
        self.last_refill = now

    def try_acquire(self) -> bool:
        """
        Try to acquire a token.

        Returns:
            True if token acquired, False if rate limited.
        """
        self._refill()
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False

    def report_429(self) -> None:
        """Handle a 429 response: halve rate and record time."""
        self.rate_per_minute = max(1.0, self.rate_per_minute / 2)
        self.max_tokens = self.rate_per_minute * 2.0
        self.last_429 = time.monotonic()
        logger.warning(
            "rate_reduced",
            new_rate=self.rate_per_minute,
            bucket_info="429 received",
        )

    def report_success(self) -> None:
        """Handle successful request: gradually restore rate."""
        if self.last_429 is not None:
            elapsed = time.monotonic() - self.last_429
            if elapsed > 3600:  # >1 hour since last 429
                new_rate = min(self.original_rate, self.rate_per_minute * 1.1)
                if new_rate > self.rate_per_minute:
                    self.rate_per_minute = new_rate
                    self.max_tokens = new_rate * 2.0
                    logger.debug("rate_restored", new_rate=self.rate_per_minute)


class RateLimiter:
    """
    Per-domain adaptive rate limiter.

    Implements token bucket per domain with:
    - Adaptive rate reduction on 429 responses
    - Gradual rate recovery after no 429s
    - Global concurrent domain limit
    """

    def __init__(self) -> None:
        """Initialize the rate limiter."""
        config = get_config()
        self._default_rate = config.stealth.rate_limiting.default_requests_per_minute
        self._aggressive_domains = config.stealth.rate_limiting.aggressive_domains
        self._backoff_on_429 = config.stealth.rate_limiting.backoff_on_429
        self._max_concurrent = config.ingestion.max_concurrent_domains
        self._buckets: dict[str, DomainBucket] = {}
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        self._lock = asyncio.Lock()

    def _get_bucket(self, domain: str) -> DomainBucket:
        """Get or create a token bucket for a domain."""
        if domain not in self._buckets:
            rate = self._aggressive_domains.get(domain, self._default_rate)
            self._buckets[domain] = DomainBucket(rate)
        return self._buckets[domain]

    async def acquire(self, domain: str) -> bool:
        """
        Acquire permission to make a request to a domain.

        Blocks if the global concurrent domain limit is reached.
        Returns True if request is allowed, False if rate limited.

        Args:
            domain: Target domain.

        Returns:
            True if request is allowed.
        """
        async with self._lock:
            bucket = self._get_bucket(domain)
            if bucket.try_acquire():
                return True

        # Rate limited — wait and retry
        wait_time = 60.0 / max(bucket.rate_per_minute, 1.0)
        logger.debug("rate_limited", domain=domain, wait_seconds=round(wait_time, 2))
        await asyncio.sleep(wait_time)

        async with self._lock:
            bucket = self._get_bucket(domain)
            return bucket.try_acquire()

    async def report_429(self, domain: str) -> None:
        """Handle a 429 response for a domain."""
        if self._backoff_on_429:
            async with self._lock:
                bucket = self._get_bucket(domain)
                bucket.report_429()

    async def report_success(self, domain: str) -> None:
        """Handle successful request to a domain."""
        async with self._lock:
            bucket = self._get_bucket(domain)
            bucket.report_success()

    def set_domain_rate(self, domain: str, rate_per_minute: float) -> None:
        """Manually set the rate for a specific domain (e.g., from robots.txt Crawl-Delay)."""
        bucket = self._get_bucket(domain)
        bucket.rate_per_minute = rate_per_minute
        bucket.original_rate = rate_per_minute
        bucket.max_tokens = rate_per_minute * 2.0
        logger.info("domain_rate_set", domain=domain, rate=rate_per_minute)

    def get_stats(self) -> dict[str, dict]:
        """Get rate limiter statistics."""
        return {
            domain: {
                "rate_per_minute": bucket.rate_per_minute,
                "tokens": round(bucket.tokens, 2),
                "last_429": bucket.last_429,
            }
            for domain, bucket in self._buckets.items()
        }
