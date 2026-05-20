"""
SENTINEL Hacker News monitor.
Monitors HN Firebase API for new, top, and best stories.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import AsyncGenerator, Optional
from uuid import uuid4

import httpx
import structlog

from sentinel.config import get_config
from sentinel.ingestion.base import BaseSource
from sentinel.models import (
    CrawlJob, CrawlResult, ContentType, ExtractedContent,
    SourceHealth, SourceType,
)

logger = structlog.get_logger(__name__)

HN_API_BASE = "https://hacker-news.firebaseio.com/v0"


class HackerNewsMonitor(BaseSource):
    """
    Hacker News Firebase API monitor.

    Tracks new, top, and best stories. Calculates score velocity
    for priority assignment. Persists seen IDs across checks.
    """

    def __init__(self) -> None:
        """Initialize HN monitor."""
        self._config = get_config()
        self._seen_ids: set[int] = set()
        self._score_cache: dict[int, tuple[int, float]] = {}  # id -> (score, timestamp)
        self._source_id = uuid4()
        self._last_check: Optional[datetime] = None
        self._total_items = 0
        self._consecutive_failures = 0

    async def _fetch_story_ids(self, category: str, client: httpx.AsyncClient) -> list[int]:
        """Fetch story IDs for a category."""
        url = f"{HN_API_BASE}/{category}stories.json"
        resp = await client.get(url, timeout=15.0)
        resp.raise_for_status()
        return resp.json()[:500]  # Max 500 per endpoint

    async def _fetch_item(self, item_id: int, client: httpx.AsyncClient) -> Optional[dict]:
        """Fetch a single HN item."""
        url = f"{HN_API_BASE}/item/{item_id}.json"
        try:
            resp = await client.get(url, timeout=10.0)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    def _calculate_score_velocity(self, item_id: int, current_score: int) -> float:
        """Calculate score velocity (score increase per hour)."""
        if item_id in self._score_cache:
            prev_score, prev_time = self._score_cache[item_id]
            hours = (time.time() - prev_time) / 3600
            if hours > 0:
                return (current_score - prev_score) / hours
        self._score_cache[item_id] = (current_score, time.time())
        return 0.0

    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """Poll HN for new stories and yield CrawlJobs."""
        hn_config = self._config.ingestion.sources.hackernews
        if not hn_config.enabled:
            return

        async with httpx.AsyncClient() as client:
            all_ids: set[int] = set()

            for category in hn_config.categories:
                try:
                    ids = await self._fetch_story_ids(category, client)
                    all_ids.update(ids)
                except Exception as e:
                    logger.error("hn_fetch_ids_failed", category=category, error=str(e))

            new_ids = all_ids - self._seen_ids

            for item_id in new_ids:
                item = await self._fetch_item(item_id, client)
                if not item or item.get("type") != "story":
                    continue

                score = item.get("score", 0)
                if score < hn_config.min_score_threshold:
                    continue

                url = item.get("url")
                if not url:
                    # Self-posts — crawl the HN discussion
                    url = f"https://news.ycombinator.com/item?id={item_id}"

                velocity = self._calculate_score_velocity(item_id, score)

                # Priority: high-score or fast-rising stories get higher priority
                priority = min(10.0, 1.0 + (score / 100) + (velocity / 10))

                yield CrawlJob(
                    url=url,
                    source_id=self._source_id,
                    priority=priority,
                    depth=0,
                    parent_url=f"https://news.ycombinator.com/item?id={item_id}",
                )

                self._seen_ids.add(item_id)
                self._total_items += 1

        self._last_check = datetime.utcnow()
        self._consecutive_failures = 0
        logger.info("hn_check_completed", new_stories=len(new_ids))

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        """Extract HN-specific data from crawl result."""
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=SourceType.HACKERNEWS,
            content_type=ContentType.ARTICLE,
            title=crawl_result.title or "",
            full_text=crawl_result.cleaned_text or "",
        )

    def get_health(self) -> SourceHealth:
        """Return source health."""
        return SourceHealth(
            source_id=self._source_id,
            source_name="Hacker News",
            source_type=SourceType.HACKERNEWS,
            status="healthy" if self._consecutive_failures < 3 else "degraded",
            last_successful_check=self._last_check,
            consecutive_failures=self._consecutive_failures,
            total_items_collected=self._total_items,
        )
