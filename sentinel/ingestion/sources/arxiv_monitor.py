"""
SENTINEL arXiv monitor.
Monitors arXiv Atom feed for new papers by category.
"""
from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional
from uuid import uuid4

import feedparser
import httpx
import structlog

from sentinel.config import get_config
from sentinel.ingestion.base import BaseSource
from sentinel.models import (
    CrawlJob, CrawlResult, ContentType, ExtractedContent,
    SourceHealth, SourceType,
)

logger = structlog.get_logger(__name__)

ARXIV_API = "http://export.arxiv.org/api/query"


class ArxivMonitor(BaseSource):
    """
    arXiv Atom feed monitor.

    Fetches latest papers per configured category.
    Cross-category papers get priority boost.
    """

    def __init__(self) -> None:
        """Initialize arXiv monitor."""
        self._config = get_config()
        self._seen_ids: set[str] = set()
        self._source_id = uuid4()
        self._last_check: Optional[datetime] = None
        self._total_items = 0
        self._consecutive_failures = 0

    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """Poll arXiv for new papers."""
        arxiv_config = self._config.ingestion.sources.arxiv
        if not arxiv_config.enabled:
            return

        async with httpx.AsyncClient(timeout=30.0) as client:
            for category in arxiv_config.categories:
                try:
                    resp = await client.get(
                        ARXIV_API,
                        params={
                            "search_query": f"cat:{category}",
                            "sortBy": "submittedDate",
                            "sortOrder": "descending",
                            "max_results": arxiv_config.max_results_per_query,
                        },
                    )
                    resp.raise_for_status()

                    feed = feedparser.parse(resp.text)

                    for entry in feed.entries:
                        arxiv_id = entry.get("id", "").split("/abs/")[-1]
                        if not arxiv_id or arxiv_id in self._seen_ids:
                            continue

                        # Check for cross-category papers
                        categories = [t.get("term", "") for t in entry.get("tags", [])]
                        tracked_cats = set(arxiv_config.categories)
                        cross_category_count = len(set(categories) & tracked_cats)

                        # Priority boost for cross-category papers
                        priority = 1.0 + (cross_category_count - 1) * 0.5

                        url = entry.get("link", entry.get("id", ""))

                        yield CrawlJob(
                            url=url,
                            source_id=self._source_id,
                            priority=priority,
                            depth=0,
                        )

                        self._seen_ids.add(arxiv_id)
                        self._total_items += 1

                except Exception as e:
                    logger.error("arxiv_fetch_failed", category=category, error=str(e))

        self._last_check = datetime.utcnow()
        self._consecutive_failures = 0
        logger.info("arxiv_check_completed", total_seen=len(self._seen_ids))

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        """Extract arXiv-specific data."""
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=SourceType.ARXIV,
            content_type=ContentType.PAPER,
            title=crawl_result.title or "",
            full_text=crawl_result.cleaned_text or "",
        )

    def get_health(self) -> SourceHealth:
        """Return source health."""
        return SourceHealth(
            source_id=self._source_id,
            source_name="arXiv",
            source_type=SourceType.ARXIV,
            status="healthy" if self._consecutive_failures < 3 else "degraded",
            last_successful_check=self._last_check,
            consecutive_failures=self._consecutive_failures,
            total_items_collected=self._total_items,
        )
