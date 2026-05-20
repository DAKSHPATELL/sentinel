"""
SENTINEL Common Crawl source.
Queries CC-INDEX API to discover URLs from the world's largest web archive
(3+ billion pages/month) — gives SENTINEL internet-scale coverage for free.
"""
from __future__ import annotations

import gzip
import io
import json
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

# CC-INDEX API base — free, no auth required
CC_INDEX_API = "https://index.commoncrawl.org"

# High-value domains to prioritize from Common Crawl
# These are domains where signals are most likely to originate
HIGH_VALUE_DOMAINS = [
    # Tech & AI
    "openai.com", "anthropic.com", "deepmind.google", "ai.meta.com",
    "huggingface.co", "arxiv.org", "techcrunch.com", "theverge.com",
    "arstechnica.com", "wired.com",
    # Finance & Business
    "sec.gov", "bloomberg.com", "reuters.com", "ft.com",
    "wsj.com", "cnbc.com", "crunchbase.com",
    # Patents & Research
    "patents.google.com", "patft.uspto.gov", "lens.org",
    "scholar.google.com", "nature.com", "science.org",
    # Government & Policy
    "whitehouse.gov", "europa.eu", "who.int",
    "federalregister.gov",
    # Startup ecosystem
    "ycombinator.com", "producthunt.com", "angel.co",
    "techstars.com", "sequoiacap.com", "a16z.com",
]

# Keywords that boost priority when found in URLs
SIGNAL_KEYWORDS = [
    "artificial-intelligence", "machine-learning", "neural-network",
    "quantum-computing", "blockchain", "patent", "funding",
    "acquisition", "merger", "ipo", "sec-filing", "fda-approval",
    "breakthrough", "launch", "release", "announce",
    "robotics", "biotech", "clean-energy", "semiconductor",
]


class CommonCrawlSource(BaseSource):
    """
    Common Crawl ingestion source.

    Queries the CC-INDEX API to find pages from the world's largest
    web archive. Processes domain lists in round-robin, fetching
    WARC records for pages matching our intelligence interests.

    This is the "breadth" engine — it provides coverage of the
    entire indexed web without needing to crawl it ourselves.
    """

    def __init__(self) -> None:
        self._config = get_config()
        self._source_id = uuid4()
        self._last_check: Optional[datetime] = None
        self._total_items = 0
        self._consecutive_failures = 0
        self._domain_index = 0
        self._latest_crawl_id: Optional[str] = None

    @property
    def name(self) -> str:
        return "commoncrawl"

    async def _get_latest_crawl_id(self, client: httpx.AsyncClient) -> Optional[str]:
        """Get the most recent Common Crawl index ID."""
        try:
            resp = await client.get(f"{CC_INDEX_API}/collinfo.json", timeout=30)
            resp.raise_for_status()
            collections = resp.json()
            if collections:
                crawl_id = collections[0]["id"]
                logger.info("cc_latest_crawl", crawl_id=crawl_id)
                return crawl_id
        except Exception as e:
            logger.error("cc_crawl_id_failed", error=str(e))
        return None

    async def _query_index(
        self,
        client: httpx.AsyncClient,
        crawl_id: str,
        domain: str,
        max_pages: int = 50,
    ) -> list[dict]:
        """
        Query CC-INDEX for pages from a specific domain.

        Returns list of index records with url, filename, offset, length.
        """
        results = []
        try:
            params = {
                "url": f"*.{domain}/*",
                "output": "json",
                "limit": max_pages,
                "filter": "=status:200",
                "fl": "url,filename,offset,length,timestamp,mime,status",
            }
            resp = await client.get(
                f"{CC_INDEX_API}/{crawl_id}-index",
                params=params,
                timeout=60,
            )
            resp.raise_for_status()

            # CC-INDEX returns NDJSON (one JSON object per line)
            for line in resp.text.strip().split("\n"):
                if line.strip():
                    try:
                        record = json.loads(line)
                        results.append(record)
                    except json.JSONDecodeError:
                        continue

            logger.info("cc_index_query", domain=domain, results=len(results))

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug("cc_no_results", domain=domain)
            else:
                logger.error("cc_index_error", domain=domain, status=e.response.status_code)
        except Exception as e:
            logger.error("cc_index_failed", domain=domain, error=str(e))

        return results

    def _calculate_priority(self, record: dict, domain: str) -> float:
        """Calculate priority for a CC record based on domain and URL signals."""
        base = 3.0  # CC records are historical, lower base than real-time sources

        url = record.get("url", "").lower()

        # Domain boost
        if domain in HIGH_VALUE_DOMAINS:
            base += 2.0

        # Keyword boost
        for kw in SIGNAL_KEYWORDS:
            if kw in url:
                base += 1.0
                break  # Only one keyword boost

        # Recency boost — prefer newer crawls
        try:
            ts = record.get("timestamp", "")
            if ts:
                crawl_date = datetime.strptime(ts[:8], "%Y%m%d")
                days_old = (datetime.utcnow() - crawl_date).days
                if days_old < 30:
                    base += 2.0
                elif days_old < 90:
                    base += 1.0
        except (ValueError, TypeError):
            pass

        return min(base, 10.0)

    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """
        Poll Common Crawl index for new URLs.

        Rotates through high-value domains, querying CC-INDEX
        for matching pages and yielding them as CrawlJobs.
        """
        cc_config = self._config.ingestion.sources.commoncrawl

        if not cc_config.enabled:
            return

        async with httpx.AsyncClient(
            headers={"User-Agent": self._config.ingestion.user_agent},
        ) as client:
            # Get latest crawl ID (cache it)
            if not self._latest_crawl_id:
                self._latest_crawl_id = await self._get_latest_crawl_id(client)
                if not self._latest_crawl_id:
                    self._consecutive_failures += 1
                    return

            # Process a batch of domains per check cycle
            domains = cc_config.domains or HIGH_VALUE_DOMAINS
            batch_size = cc_config.domains_per_cycle
            start = self._domain_index
            end = min(start + batch_size, len(domains))

            for i in range(start, end):
                domain = domains[i % len(domains)]

                records = await self._query_index(
                    client,
                    self._latest_crawl_id,
                    domain,
                    max_pages=cc_config.max_pages_per_domain,
                )

                for record in records:
                    url = record.get("url", "")
                    if not url:
                        continue

                    priority = self._calculate_priority(record, domain)

                    yield CrawlJob(
                        url=url,
                        priority=priority,
                        depth=0,
                        parent_url=None,
                        source_id=self._source_id,
                    )
                    self._total_items += 1

            # Advance domain index (wraps around)
            self._domain_index = end % len(domains)
            self._last_check = datetime.utcnow()
            self._consecutive_failures = 0

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        """Extract content from a Common Crawl sourced page."""
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=SourceType.COMMON_CRAWL,
            content_type=ContentType.ARTICLE,
            title=crawl_result.title or "",
            full_text=crawl_result.cleaned_text or "",
        )

    def get_health(self) -> SourceHealth:
        return SourceHealth(
            source_id=self._source_id,
            source_name="Common Crawl",
            source_type=SourceType.COMMON_CRAWL,
            status="healthy" if self._consecutive_failures < 3 else "degraded",
            last_successful_check=self._last_check,
            consecutive_failures=self._consecutive_failures,
            total_items_collected=self._total_items,
        )
