"""
SENTINEL robots.txt parser.
Async robots.txt checking with domain-level caching.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
import structlog

from sentinel.config import get_config
from sentinel.core.sqlite_client import SQLiteClient

logger = structlog.get_logger(__name__)


class RobotsParser:
    """
    Robots.txt compliance engine.

    Caches robots.txt per domain in SQLite.
    Refreshes if cache is >24h old.
    Parses Crawl-Delay directive and updates rate limiter.
    """

    def __init__(self, sqlite_client: SQLiteClient) -> None:
        """
        Initialize robots parser.

        Args:
            sqlite_client: SQLite client for domain_stats caching.
        """
        self._sqlite = sqlite_client
        self._parsers: dict[str, RobotFileParser] = {}
        self._config = get_config()

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        return parsed.netloc

    def _get_robots_url(self, url: str) -> str:
        """Get robots.txt URL for a given URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    async def _fetch_robots(self, domain: str, robots_url: str) -> Optional[str]:
        """Fetch robots.txt from a domain."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    robots_url,
                    headers={"User-Agent": self._config.ingestion.user_agent},
                    follow_redirects=True,
                )
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code in (403, 404):
                    return ""  # No robots.txt or access denied — allow all
                else:
                    logger.debug("robots_fetch_unexpected_status", domain=domain, status=resp.status_code)
                    return ""
        except Exception as e:
            logger.debug("robots_fetch_failed", domain=domain, error=str(e))
            return ""

    async def _get_parser(self, domain: str, url: str) -> RobotFileParser:
        """Get or create a RobotFileParser for a domain."""
        # Check memory cache
        if domain in self._parsers:
            return self._parsers[domain]

        # Check SQLite cache
        stats = await self._sqlite.get_domain_stats(domain)
        robots_text = None

        if stats and stats.get("robots_txt") is not None and stats.get("robots_fetched_at"):
            fetched_at = datetime.fromisoformat(stats["robots_fetched_at"])
            if datetime.utcnow() - fetched_at < timedelta(hours=24):
                robots_text = stats["robots_txt"]

        # Fetch if not cached or expired
        if robots_text is None:
            robots_url = self._get_robots_url(url)
            robots_text = await self._fetch_robots(domain, robots_url)
            if robots_text is not None:
                await self._sqlite.update_domain_robots(domain, robots_text)

        # Parse
        parser = RobotFileParser()
        parser.parse((robots_text or "").splitlines())
        self._parsers[domain] = parser

        return parser

    async def is_allowed(self, url: str) -> bool:
        """
        Check if a URL is allowed by robots.txt.

        Args:
            url: URL to check.

        Returns:
            True if crawling is allowed.
        """
        if not self._config.ingestion.sources.web_crawler.respect_robots_txt:
            return True

        domain = self._extract_domain(url)
        try:
            parser = await self._get_parser(domain, url)
            allowed = parser.can_fetch(self._config.ingestion.user_agent, url)
            if not allowed:
                logger.debug("robots_denied", url=url[:100])
            return allowed
        except Exception as e:
            logger.error("robots_check_failed", url=url[:100], error=str(e))
            return True  # Allow on error (permissive)

    async def get_crawl_delay(self, url: str) -> Optional[float]:
        """
        Get the Crawl-Delay directive for a URL's domain.

        Args:
            url: URL to check.

        Returns:
            Crawl delay in seconds, or None if not specified.
        """
        domain = self._extract_domain(url)
        try:
            parser = await self._get_parser(domain, url)
            delay = parser.crawl_delay(self._config.ingestion.user_agent)
            return float(delay) if delay is not None else None
        except Exception:
            return None
