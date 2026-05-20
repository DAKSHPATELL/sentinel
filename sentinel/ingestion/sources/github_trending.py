"""
SENTINEL GitHub trending monitor.
Monitors GitHub events + search API for trending repos.
"""
from __future__ import annotations

from datetime import datetime, timedelta
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

GITHUB_API = "https://api.github.com"


class GitHubTrendingMonitor(BaseSource):
    """
    GitHub events and trending repository monitor.

    Fetches public events stream and searches for newly created
    repos with >10 stars. Calculates star velocity.
    """

    def __init__(self) -> None:
        """Initialize GitHub monitor."""
        self._config = get_config()
        self._seen_repos: set[str] = set()
        self._star_cache: dict[str, tuple[int, float]] = {}
        self._source_id = uuid4()
        self._last_check: Optional[datetime] = None
        self._total_items = 0
        self._consecutive_failures = 0

    def _get_headers(self) -> dict[str, str]:
        """Get API headers with optional auth token."""
        headers = {"Accept": "application/vnd.github+json"}
        token = self._config.ingestion.sources.github.token
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """Poll GitHub for trending repos and events."""
        gh_config = self._config.ingestion.sources.github
        if not gh_config.enabled:
            return

        headers = self._get_headers()

        async with httpx.AsyncClient(headers=headers, timeout=15.0) as client:
            # Search for trending repos
            yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
            for lang in gh_config.trending_languages:
                try:
                    resp = await client.get(
                        f"{GITHUB_API}/search/repositories",
                        params={
                            "q": f"created:>{yesterday} stars:>10 language:{lang}",
                            "sort": "stars",
                            "order": "desc",
                            "per_page": 30,
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    for repo in data.get("items", []):
                        full_name = repo["full_name"]
                        if full_name in self._seen_repos:
                            continue

                        stars = repo.get("stargazers_count", 0)
                        # Calculate star velocity
                        import time
                        velocity = 0.0
                        if full_name in self._star_cache:
                            prev_stars, prev_time = self._star_cache[full_name]
                            hours = (time.time() - prev_time) / 3600
                            if hours > 0:
                                velocity = (stars - prev_stars) / hours
                        self._star_cache[full_name] = (stars, time.time())

                        # Priority based on stars and velocity
                        priority = min(10.0, 1.0 + (stars / 50) + (velocity / 5))
                        if velocity > 100:
                            priority = 10.0  # CRITICAL priority for viral repos

                        # Yield README URL
                        readme_url = f"https://github.com/{full_name}"
                        yield CrawlJob(
                            url=readme_url,
                            source_id=self._source_id,
                            priority=priority,
                            depth=0,
                        )

                        self._seen_repos.add(full_name)
                        self._total_items += 1

                except Exception as e:
                    logger.error("github_search_failed", language=lang, error=str(e))

            # Fetch public events
            try:
                resp = await client.get(f"{GITHUB_API}/events", params={"per_page": 100})
                resp.raise_for_status()
                events = resp.json()

                for event in events:
                    if event.get("type") not in gh_config.track_events:
                        continue
                    repo = event.get("repo", {})
                    full_name = repo.get("name", "")
                    if full_name and full_name not in self._seen_repos:
                        yield CrawlJob(
                            url=f"https://github.com/{full_name}",
                            source_id=self._source_id,
                            priority=1.5,
                            depth=0,
                        )
                        self._seen_repos.add(full_name)
                        self._total_items += 1

            except Exception as e:
                logger.error("github_events_failed", error=str(e))

        self._last_check = datetime.utcnow()
        self._consecutive_failures = 0

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        """Extract GitHub-specific data."""
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=SourceType.GITHUB,
            content_type=ContentType.CODE_REPO,
            title=crawl_result.title or "",
            full_text=crawl_result.cleaned_text or "",
        )

    def get_health(self) -> SourceHealth:
        """Return source health."""
        return SourceHealth(
            source_id=self._source_id,
            source_name="GitHub Trending",
            source_type=SourceType.GITHUB,
            status="healthy" if self._consecutive_failures < 3 else "degraded",
            last_successful_check=self._last_check,
            consecutive_failures=self._consecutive_failures,
            total_items_collected=self._total_items,
        )
