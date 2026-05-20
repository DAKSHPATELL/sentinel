"""
SENTINEL ingestion base classes.
Abstract base class for all source monitors.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncGenerator

from sentinel.models import CrawlJob, CrawlResult, ExtractedContent, SourceHealth


class BaseSource(ABC):
    """
    Abstract base for every source monitor.

    The scheduler calls check() at the configured interval.
    check() yields CrawlJob objects that enter the frontier.
    """

    @abstractmethod
    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """
        Poll the source for new items.

        Yields CrawlJob for each discovered URL.
        Must be idempotent — calling twice with no new data yields nothing.
        Must handle its own rate limiting and error recovery.
        Must update source.last_checked on success.
        """
        ...  # pragma: no cover
        yield  # type: ignore

    @abstractmethod
    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        """
        Source-specific extraction from crawled content.

        Called after general crawling completes.
        Returns structured ExtractedContent with source-specific fields
        in the structured_data dict.

        Args:
            crawl_result: The result of crawling this source's URL.

        Returns:
            Extracted and structured content.
        """
        ...  # pragma: no cover

    @abstractmethod
    def get_health(self) -> SourceHealth:
        """
        Return current source health metrics.

        Returns:
            SourceHealth with current status and metrics.
        """
        ...  # pragma: no cover
