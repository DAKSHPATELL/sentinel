"""
SENTINEL web crawler.
Main crawl loop: fetch pages, store HTML, extract links, emit events.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse
from uuid import uuid4

import httpx
import structlog
from bs4 import BeautifulSoup

from sentinel.config import get_config
from sentinel.core.storage import StorageManager
from sentinel.events import STREAM_CRAWL_RESULTS, EventBus
from sentinel.ingestion.base import BaseSource
from sentinel.ingestion.frontier import URLFrontier, extract_domain
from sentinel.models import (
    CrawlJob, CrawlResult, ContentType, CrawlStatus, ExtractedContent,
    SourceHealth, SourceType,
)
from sentinel.stealth.rate_limiter import RateLimiter
from sentinel.stealth.robots_parser import RobotsParser
from sentinel.stealth.annihilator import AcquisitionOrchestrator
from sentinel.stealth.temporal_arbitrage import TemporalArbitrageScheduler

logger = structlog.get_logger(__name__)


class WebCrawler(BaseSource):
    """
    General-purpose web crawler.

    Pulls URLs from frontier, fetches with httpx, stores HTML,
    extracts outgoing links, and emits CrawlResult events.
    """

    def __init__(
        self,
        frontier: URLFrontier,
        rate_limiter: RateLimiter,
        robots_parser: RobotsParser,
        storage: StorageManager,
        orchestrator: AcquisitionOrchestrator,
        temporal_scheduler: TemporalArbitrageScheduler,
        event_bus: Optional[EventBus] = None,
    ) -> None:
        """
        Initialize web crawler.

        Args:
            frontier: URL frontier for batch retrieval.
            rate_limiter: Per-domain rate limiter.
            robots_parser: Robots.txt compliance checker.
            storage: HTML storage manager.
            orchestrator: Multi-strategy content acquisition engine.
            temporal_scheduler: Off-peak retry scheduler.
            event_bus: Event bus for emitting crawl results.
        """
        self._frontier = frontier
        self._rate_limiter = rate_limiter
        self._robots = robots_parser
        self._storage = storage
        self._orchestrator = orchestrator
        self._temporal_scheduler = temporal_scheduler
        self._event_bus = event_bus
        self._config = get_config()
        self._source_id = uuid4()
        self._last_check: Optional[datetime] = None
        self._total_items = 0
        self._consecutive_failures = 0
        self._running = False

    def _extract_links(self, html: str, base_url: str) -> list[str]:
        """Extract outgoing links from HTML."""
        links = []
        try:
            soup = BeautifulSoup(html, "lxml")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                absolute = urljoin(base_url, href)
                parsed = urlparse(absolute)
                # Only HTTP(S) links
                if parsed.scheme in ("http", "https"):
                    links.append(absolute)
        except Exception as e:
            logger.debug("link_extraction_failed", url=base_url[:80], error=str(e))
        return links[:100]  # Cap at 100 links per page

    async def _fetch_page(
        self, url: str
    ) -> Optional[CrawlResult]:
        """Fetch a single page."""
        domain = extract_domain(url)
        start_time = time.perf_counter()

        try:
            # Check robots.txt
            if not await self._robots.is_allowed(url):
                return CrawlResult(
                    job_id=uuid4(), url=url, status_code=0,
                    content_type="", content_hash="",
                    blocked=True,
                )

            # Check temporal arbitrage
            if not self._temporal_scheduler.should_retry(url):
                logger.debug("temporal_arbitrage_skipped_max_retries", url=url[:80])
                return CrawlResult(
                    job_id=uuid4(), url=url, status_code=0,
                    content_type="", content_hash="",
                    blocked=False, download_time_ms=0,
                )

            # Acquire rate limiter token
            allowed = await self._rate_limiter.acquire(domain)
            if not allowed:
                return None  # Skip, will retry later

            # Fetch via orchestrator
            acq_result = await self._orchestrator.acquire(url)

            # Temporal scheduling
            self._temporal_scheduler.record_attempt(domain, acq_result.success)

            if not acq_result.success:
                self._temporal_scheduler.record_retry(url)
                return CrawlResult(
                    job_id=uuid4(), url=url, status_code=0,
                    content_type="", content_hash="",
                    blocked=False, download_time_ms=acq_result.acquisition_time_ms,
                )

            await self._rate_limiter.report_success(domain)

            content = acq_result.content or ""
            download_ms = acq_result.acquisition_time_ms
            
            # Reconstructor might return very short fragments, but assume it gave good enough text.
            if len(content) > self._config.ingestion.max_page_size_bytes:
                content = content[: self._config.ingestion.max_page_size_bytes]

            content_hash = hashlib.sha256(content.encode()).hexdigest()

            # Store raw HTML
            raw_path = self._storage.store_raw_html(url, content)

            # Extract links
            links = self._extract_links(content, url)

            # Extract title
            title = None
            try:
                soup = BeautifulSoup(content[:10000], "lxml")
                title_tag = soup.find("title")
                if title_tag:
                    title = title_tag.get_text(strip=True)[:500]
            except Exception:
                pass

            return CrawlResult(
                job_id=uuid4(),
                url=url,
                status_code=200,
                content_type="text/html",
                content_hash=content_hash,
                raw_html_path=raw_path,
                title=title,
                outgoing_links=links,
                download_time_ms=download_ms,
                text_length=len(content),
            )

        except httpx.TimeoutException:
            return CrawlResult(
                job_id=uuid4(), url=url, status_code=0,
                content_type="", content_hash="",
                download_time_ms=int((time.perf_counter() - start_time) * 1000),
            )
        except Exception as e:
            logger.error("crawl_failed", url=url[:100], error=str(e))
            return None

    async def check(self) -> None:
        """
        Main crawl loop — run as async generator for scheduler compatibility.

        In practice, the web crawler runs its own continuous loop via run_workers().
        This method exists for BaseSource interface compliance.
        """
        # The web crawler runs via run_workers(), not through the standard check() pattern
        return
        yield  # Make it a generator # type: ignore

    async def run_workers(self, num_workers: int = 8) -> None:
        """
        Run crawl workers that continuously process the frontier.

        Args:
            num_workers: Number of concurrent crawl workers.
        """
        self._running = True
        tasks = [asyncio.create_task(self._worker(i)) for i in range(num_workers)]
        logger.info("crawl_workers_started", count=num_workers)

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False

    async def _worker(self, worker_id: int) -> None:
        """Single crawl worker loop."""
        async with httpx.AsyncClient(
            headers={"User-Agent": self._config.ingestion.user_agent},
            follow_redirects=True,
            timeout=self._config.ingestion.request_timeout,
        ) as client:
            while self._running:
                try:
                    # Get batch from frontier
                    batch = await self._frontier.get_next_batch(1)
                    if not batch:
                        await asyncio.sleep(2)
                        continue

                    for row in batch:
                        url = row["url"]
                        result = await self._fetch_page(url)

                        if result is None:
                            await self._frontier.mark_failed(url, "fetch returned None")
                            continue

                        if result.blocked:
                            await self._frontier.mark_failed(url, "blocked or robots denied")
                            continue

                        if result.status_code >= 400:
                            await self._frontier.mark_failed(url, f"HTTP {result.status_code}")
                            continue

                        # Success
                        await self._frontier.mark_completed(url, result.content_hash)
                        self._total_items += 1

                        # Add outgoing links to frontier
                        for link in result.outgoing_links[:50]:
                            await self._frontier.add_url(
                                url=link,
                                priority=row.get("priority", 1.0) * 0.8,
                                depth=row.get("depth", 0) + 1,
                                parent_url=url,
                            )

                        # Emit crawl result event
                        if self._event_bus:
                            try:
                                await self._event_bus.emit(
                                    STREAM_CRAWL_RESULTS,
                                    {
                                        "job_id": str(result.job_id),
                                        "url": result.url,
                                        "status_code": result.status_code,
                                        "content_hash": result.content_hash,
                                        "raw_html_path": result.raw_html_path or "",
                                        "title": result.title or "",
                                        "text_length": result.text_length,
                                    },
                                )
                            except Exception as e:
                                logger.error("event_emit_failed", error=str(e))

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("worker_error", worker=worker_id, error=str(e))
                    await asyncio.sleep(5)

    def stop(self) -> None:
        """Signal workers to stop."""
        self._running = False

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        """Extract web crawl data."""
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=SourceType.WEB_CRAWL,
            content_type=ContentType.ARTICLE,
            title=crawl_result.title or "",
            full_text=crawl_result.cleaned_text or "",
        )

    def get_health(self) -> SourceHealth:
        """Return source health."""
        return SourceHealth(
            source_id=self._source_id,
            source_name="Web Crawler",
            source_type=SourceType.WEB_CRAWL,
            status="healthy" if self._running else "stopped",
            last_successful_check=self._last_check,
            consecutive_failures=self._consecutive_failures,
            total_items_collected=self._total_items,
        )
