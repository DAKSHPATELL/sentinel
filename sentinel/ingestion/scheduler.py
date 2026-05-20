"""
SENTINEL source scheduler.
APScheduler-based source scheduling with jitter and failure handling.
"""
from __future__ import annotations

import asyncio
import random
from typing import Any, Optional

import structlog

from sentinel.config import get_config
from sentinel.ingestion.base import BaseSource
from sentinel.ingestion.frontier import URLFrontier

logger = structlog.get_logger(__name__)


class SourceScheduler:
    """
    APScheduler-based source scheduling.

    Schedules each enabled source's check() method at its configured interval
    with random jitter. Handles failures and auto-disabling.
    """

    def __init__(self, frontier: URLFrontier) -> None:
        """
        Initialize the source scheduler.

        Args:
            frontier: URL frontier for adding discovered URLs.
        """
        self._frontier = frontier
        self._sources: dict[str, BaseSource] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._failures: dict[str, int] = {}
        self._running = False

    def register_source(self, name: str, source: BaseSource) -> None:
        """
        Register a source monitor.

        Args:
            name: Unique name for the source.
            source: Source monitor instance.
        """
        self._sources[name] = source
        self._failures[name] = 0
        logger.info("source_registered", source=name)

    async def _run_source(self, name: str, source: BaseSource, interval: int) -> None:
        """Run a source monitor in a loop with jitter."""
        while self._running:
            try:
                # Add 0-10% jitter
                jitter = random.uniform(0, interval * 0.1)
                await asyncio.sleep(interval + jitter)

                logger.info("source_check_started", source=name)
                count = 0

                async for crawl_job in source.check():
                    added = await self._frontier.add_url(
                        url=crawl_job.url,
                        priority=crawl_job.priority,
                        depth=crawl_job.depth,
                        parent_url=crawl_job.parent_url,
                        source_id=str(crawl_job.source_id) if crawl_job.source_id else None,
                    )
                    if added:
                        count += 1

                self._failures[name] = 0
                logger.info("source_check_completed", source=name, urls_added=count)

            except asyncio.CancelledError:
                logger.info("source_cancelled", source=name)
                break
            except Exception as e:
                self._failures[name] = self._failures.get(name, 0) + 1
                logger.error(
                    "source_check_failed",
                    source=name,
                    error=str(e),
                    consecutive_failures=self._failures[name],
                )

                # Disable after 5 consecutive failures
                if self._failures[name] >= 5:
                    logger.warning("source_disabled", source=name, reason="5 consecutive failures")
                    break

                # Exponential backoff on failure
                backoff = min(interval * (2 ** self._failures[name]), 3600)
                await asyncio.sleep(backoff)

    async def start(self) -> None:
        """Start all registered source monitors."""
        self._running = True
        config = get_config()

        # Map source names to their poll intervals
        intervals = {
            "hackernews": config.ingestion.sources.hackernews.poll_interval_seconds,
            "github": config.ingestion.sources.github.poll_interval_seconds,
            "arxiv": config.ingestion.sources.arxiv.poll_interval_seconds,
            "rss": config.ingestion.sources.rss.poll_interval_seconds,
            "reddit": config.ingestion.sources.reddit.poll_interval_seconds,
            "web_crawler": 30,  # Web crawler runs its own loop
            "ct_monitor": 3600,
            "patents": config.ingestion.sources.patents.poll_interval_seconds,
            "commoncrawl": config.ingestion.sources.commoncrawl.poll_interval_seconds,
            "sitemap": config.ingestion.sources.sitemap.poll_interval_seconds,
        }

        for name, source in self._sources.items():
            interval = intervals.get(name, 3600)
            task = asyncio.create_task(self._run_source(name, source, interval))
            self._tasks[name] = task
            logger.info("source_scheduled", source=name, interval_seconds=interval)

    async def stop(self) -> None:
        """Stop all source monitors."""
        self._running = False
        for name, task in self._tasks.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        logger.info("scheduler_stopped", sources=list(self._sources.keys()))

    def get_status(self) -> dict[str, dict]:
        """Get status of all registered sources."""
        return {
            name: {
                "running": name in self._tasks and not self._tasks[name].done(),
                "consecutive_failures": self._failures.get(name, 0),
            }
            for name in self._sources
        }
