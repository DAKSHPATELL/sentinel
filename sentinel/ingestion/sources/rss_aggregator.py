"""
SENTINEL RSS aggregator.
Generic RSS/Atom feed monitor for multiple feeds.
"""
from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional
from uuid import uuid4

import feedparser
import httpx
import structlog

from sentinel.config import get_config
from sentinel.constants import SEED_RSS_FEEDS
from sentinel.ingestion.base import BaseSource
from sentinel.models import (
    CrawlJob, CrawlResult, ContentType, ExtractedContent,
    SourceHealth, SourceType,
)

logger = structlog.get_logger(__name__)


class RSSAggregator(BaseSource):
    """
    Generic RSS/Atom feed aggregator.

    Monitors configured feeds plus seed feeds.
    Tracks seen entry IDs per feed.
    """

    def __init__(self) -> None:
        """Initialize RSS aggregator."""
        self._config = get_config()
        self._seen_entries: dict[str, set[str]] = {}  # feed_url -> set of entry IDs
        self._source_id = uuid4()
        self._last_check: Optional[datetime] = None
        self._total_items = 0
        self._consecutive_failures = 0

    def _get_feeds(self) -> list[str]:
        """Get all configured feeds plus seed feeds."""
        config_feeds = self._config.ingestion.sources.rss.feeds
        all_feeds = list(set(config_feeds + SEED_RSS_FEEDS))
        return all_feeds

    def _get_entry_id(self, entry: dict) -> str:
        """Get a unique ID for a feed entry."""
        return entry.get("id", entry.get("link", entry.get("title", "")))

    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """Poll all RSS feeds for new entries."""
        rss_config = self._config.ingestion.sources.rss
        if not rss_config.enabled:
            return

        feeds = self._get_feeds()

        async with httpx.AsyncClient(timeout=15.0) as client:
            for feed_url in feeds:
                try:
                    resp = await client.get(
                        feed_url,
                        headers={"User-Agent": self._config.ingestion.user_agent},
                        follow_redirects=True,
                    )
                    resp.raise_for_status()

                    feed = feedparser.parse(resp.text)

                    if feed_url not in self._seen_entries:
                        self._seen_entries[feed_url] = set()

                    seen = self._seen_entries[feed_url]

                    for entry in feed.entries:
                        entry_id = self._get_entry_id(entry)
                        if entry_id in seen:
                            continue

                        link = entry.get("link")
                        if not link:
                            continue

                        yield CrawlJob(
                            url=link,
                            source_id=self._source_id,
                            priority=1.0,
                            depth=0,
                            parent_url=feed_url,
                        )

                        seen.add(entry_id)
                        self._total_items += 1

                        # Limit entries per feed per check
                        if len(seen) > 10000:
                            # Trim oldest entries to prevent unbounded growth
                            self._seen_entries[feed_url] = set(list(seen)[-5000:])

                except Exception as e:
                    logger.debug("rss_feed_failed", feed=feed_url[:80], error=str(e))

        self._last_check = datetime.utcnow()
        self._consecutive_failures = 0
        logger.info("rss_check_completed", feeds=len(feeds), total_items=self._total_items)

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        """Extract RSS-specific data."""
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=SourceType.RSS,
            content_type=ContentType.ARTICLE,
            title=crawl_result.title or "",
            full_text=crawl_result.cleaned_text or "",
        )

    def get_health(self) -> SourceHealth:
        """Return source health."""
        return SourceHealth(
            source_id=self._source_id,
            source_name="RSS Aggregator",
            source_type=SourceType.RSS,
            status="healthy" if self._consecutive_failures < 3 else "degraded",
            last_successful_check=self._last_check,
            consecutive_failures=self._consecutive_failures,
            total_items_collected=self._total_items,
        )
