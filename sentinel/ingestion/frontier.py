"""
SENTINEL URL frontier.
Priority queue with bloom filter dedup, URL normalization, and adaptive revisiting.
"""
from __future__ import annotations

import hashlib
import math
import time
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import structlog

from sentinel.config import get_config
from sentinel.core.sqlite_client import SQLiteClient

logger = structlog.get_logger(__name__)


def normalize_url(url: str) -> str:
    """
    Normalize a URL for deduplication.

    - Lowercase scheme and host
    - Strip fragment
    - Sort query parameters
    - Strip trailing slash (except root)
    - Strip common tracking parameters

    Args:
        url: Raw URL to normalize.

    Returns:
        Normalized URL string.
    """
    try:
        parsed = urlparse(url)

        # Lowercase scheme and host
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Sort query parameters and remove tracking params
        tracking_params = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "ref", "fbclid", "gclid"}
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        filtered = {k: v for k, v in sorted(query_params.items()) if k not in tracking_params}
        query = urlencode(filtered, doseq=True)

        # Strip fragment
        fragment = ""

        # Reconstruct path, strip trailing slash (keep root /)
        path = parsed.path
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        if not path:
            path = "/"

        return urlunparse((scheme, netloc, path, parsed.params, query, fragment))
    except Exception:
        return url


def extract_domain(url: str) -> str:
    """Extract domain from a URL."""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def calculate_priority(
    base_priority: float,
    source_weight: float = 1.0,
    hours_since_created: float = 0.0,
    depth: int = 0,
    domain_reputation: float = 1.0,
    priority_decay: float = 0.95,
) -> float:
    """
    Calculate final URL priority per PRD Appendix A.1.

    priority = base_priority × source_weight × freshness_decay × depth_penalty × domain_reputation

    Args:
        base_priority: Base priority from source (0.0-10.0).
        source_weight: Weight from CrawlStrategy (0.1-10.0).
        hours_since_created: Hours since URL was created.
        depth: Depth from seed URL.
        domain_reputation: Historical signal yield (0.1-5.0).
        priority_decay: Decay factor per hour (default 0.95).

    Returns:
        Calculated priority score.
    """
    freshness_decay = priority_decay ** hours_since_created
    depth_penalty = 0.9 ** depth
    return base_priority * source_weight * freshness_decay * depth_penalty * domain_reputation


class URLFrontier:
    """
    URL frontier with priority queue, bloom filter dedup, and adaptive revisiting.

    Manages the crawl queue in SQLite with bloom filter for fast dedup checks.
    """

    def __init__(self, sqlite_client: SQLiteClient) -> None:
        """
        Initialize URL frontier.

        Args:
            sqlite_client: SQLite client for frontier storage.
        """
        self._sqlite = sqlite_client
        self._config = get_config()
        self._seen_urls: set[str] = set()  # In-memory bloom filter substitute
        self._seen_count = 0

    async def add_url(
        self,
        url: str,
        priority: float = 1.0,
        depth: int = 0,
        parent_url: Optional[str] = None,
        source_id: Optional[str] = None,
        source_weight: float = 1.0,
    ) -> bool:
        """
        Add a URL to the frontier.

        Normalizes URL, checks bloom filter, calculates priority, inserts into SQLite.

        Args:
            url: URL to add.
            priority: Base priority.
            depth: Depth from seed URL.
            parent_url: URL that linked to this.
            source_id: Source that discovered this.
            source_weight: Source weight for priority calculation.

        Returns:
            True if URL was added, False if duplicate or max depth exceeded.
        """
        # Check depth limit
        max_depth = self._config.ingestion.frontier.max_depth
        if depth > max_depth:
            return False

        # Normalize
        normalized = normalize_url(url)

        # Quick bloom filter check
        if normalized in self._seen_urls:
            return False

        # Calculate priority
        final_priority = calculate_priority(
            base_priority=priority,
            source_weight=source_weight,
            hours_since_created=0.0,
            depth=depth,
            priority_decay=self._config.ingestion.frontier.priority_decay,
        )

        # Insert into SQLite
        domain = extract_domain(normalized)
        added = await self._sqlite.add_to_frontier(
            url=normalized,
            domain=domain,
            priority=final_priority,
            depth=depth,
            parent_url=parent_url,
            source_id=source_id,
        )

        if added:
            self._seen_urls.add(normalized)
            self._seen_count += 1
            logger.debug(
                "url_added_to_frontier",
                url=normalized[:100],
                priority=round(final_priority, 3),
                depth=depth,
            )

        return added

    async def get_next_batch(self, n: int = 10) -> list[dict]:
        """
        Get next N highest-priority URLs for crawling.

        Args:
            n: Number of URLs to retrieve.

        Returns:
            List of frontier row dicts.
        """
        urls = await self._sqlite.get_next_urls(n)

        # Mark as in_progress
        for row in urls:
            await self._sqlite.update_status(row["url"], "in_progress")

        if urls:
            logger.debug("frontier_batch_retrieved", count=len(urls))

        return urls

    async def mark_completed(self, url: str, content_hash: str) -> None:
        """
        Mark a URL as completed.

        Sets next_crawl_at based on content change detection:
        - If content_hash matches previous: double revisit interval
        - If new content: use min_revisit_hours

        Args:
            url: Completed URL.
            content_hash: SHA-256 of crawled content.
        """
        # Check previous content hash
        results = await self._sqlite.query(
            "SELECT content_hash, last_crawled_at FROM frontier WHERE url = ?", (url,)
        )

        min_revisit = self._config.ingestion.frontier.min_revisit_hours
        max_revisit = self._config.ingestion.frontier.max_revisit_hours

        if results and results[0].get("content_hash") == content_hash:
            # Content unchanged — double revisit interval
            revisit_hours = min(min_revisit * 2, max_revisit)
        else:
            revisit_hours = min_revisit

        next_crawl = (datetime.utcnow() + timedelta(hours=revisit_hours)).isoformat()

        await self._sqlite.update_status(
            url=url,
            status="completed",
            content_hash=content_hash,
            next_crawl_at=next_crawl,
        )

    async def mark_failed(self, url: str, error: str) -> None:
        """
        Mark a URL as failed.

        If attempts < max_attempts: reset to pending with backoff.
        If attempts >= max_attempts: mark as permanently failed.

        Args:
            url: Failed URL.
            error: Error message.
        """
        results = await self._sqlite.query(
            "SELECT attempt_count FROM frontier WHERE url = ?", (url,)
        )
        attempts = (results[0]["attempt_count"] if results else 0) + 1

        if attempts >= 3:  # max_attempts
            await self._sqlite.update_status(url=url, status="failed", error_message=error)
            logger.warning("url_permanently_failed", url=url[:100], attempts=attempts, error=error)
        else:
            # Retry with backoff
            backoff_hours = 2 ** attempts
            next_crawl = (datetime.utcnow() + timedelta(hours=backoff_hours)).isoformat()
            await self._sqlite.update_status(
                url=url, status="pending", error_message=error, next_crawl_at=next_crawl
            )

    async def get_stats(self) -> dict:
        """Get frontier statistics."""
        return await self._sqlite.get_frontier_stats()
