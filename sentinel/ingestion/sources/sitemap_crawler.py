"""
SENTINEL Sitemap discovery engine.
Discovers and parses sitemap.xml / sitemap_index.xml from every known domain
to find every URL without needing to crawl link-by-link.
"""
from __future__ import annotations

import gzip
import io
import re
from datetime import datetime
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin, urlparse
from uuid import uuid4
from xml.etree import ElementTree

import httpx
import structlog

from sentinel.config import get_config
from sentinel.core.sqlite_client import SQLiteClient
from sentinel.ingestion.base import BaseSource
from sentinel.models import (
    CrawlJob, CrawlResult, ContentType, ExtractedContent,
    SourceHealth, SourceType,
)

logger = structlog.get_logger(__name__)

# XML namespace used in sitemaps
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Common sitemap locations to probe
SITEMAP_PATHS = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap/sitemap.xml",
    "/sitemaps/sitemap.xml",
    "/sitemap1.xml",
    "/sitemap-index.xml",
    "/wp-sitemap.xml",          # WordPress
    "/sitemap_news.xml",        # Google News sitemaps
    "/sitemap_video.xml",
]

# robots.txt Sitemap: directive regex
ROBOTS_SITEMAP_RE = re.compile(r"^Sitemap:\s*(.+)$", re.IGNORECASE | re.MULTILINE)


class SitemapCrawler(BaseSource):
    """
    Sitemap discovery and parsing engine.

    For every domain discovered by SENTINEL's other sources, this engine:
    1. Checks robots.txt for Sitemap: directives
    2. Probes common sitemap paths
    3. Recursively parses sitemap index files
    4. Extracts all URLs with lastmod timestamps
    5. Yields them as CrawlJobs with appropriate priority

    This provides *complete* URL coverage of any domain without
    needing to follow links page by page.
    """

    def __init__(self, sqlite: SQLiteClient) -> None:
        self._sqlite = sqlite
        self._config = get_config()
        self._source_id = uuid4()
        self._last_check: Optional[datetime] = None
        self._total_items = 0
        self._consecutive_failures = 0
        # Track which domains we've already discovered sitemaps for
        self._discovered_domains: set[str] = set()

    @property
    def name(self) -> str:
        return "sitemap"

    async def _get_domains_from_frontier(self, limit: int = 100) -> list[str]:
        """Get unique domains from the frontier that we haven't sitemapped yet."""
        try:
            rows = await self._sqlite.query(
                """
                SELECT DISTINCT domain FROM frontier
                WHERE status IN ('completed', 'pending')
                ORDER BY RANDOM()
                LIMIT ?
                """,
                (limit * 3,),  # Fetch extra to filter
            )
            domains = []
            for row in rows:
                domain = row["domain"]
                if domain and domain not in self._discovered_domains:
                    domains.append(domain)
                    if len(domains) >= limit:
                        break
            return domains
        except Exception as e:
            logger.error("sitemap_get_domains_failed", error=str(e))
            return []

    async def _fetch_robots_sitemaps(
        self, client: httpx.AsyncClient, domain: str
    ) -> list[str]:
        """Extract Sitemap: URLs from robots.txt."""
        sitemaps = []
        try:
            resp = await client.get(
                f"https://{domain}/robots.txt", timeout=15
            )
            if resp.status_code == 200:
                matches = ROBOTS_SITEMAP_RE.findall(resp.text)
                sitemaps.extend(url.strip() for url in matches)
        except Exception:
            pass
        return sitemaps

    async def _fetch_sitemap(
        self, client: httpx.AsyncClient, url: str
    ) -> Optional[str]:
        """Fetch and decompress a sitemap URL."""
        try:
            resp = await client.get(url, timeout=30)
            if resp.status_code != 200:
                return None

            content = resp.content

            # Handle gzipped sitemaps (.xml.gz)
            if url.endswith(".gz") or resp.headers.get("content-encoding") == "gzip":
                try:
                    content = gzip.decompress(content)
                except Exception:
                    pass

            if isinstance(content, bytes):
                return content.decode("utf-8", errors="replace")
            return content

        except Exception as e:
            logger.debug("sitemap_fetch_failed", url=url[:80], error=str(e))
            return None

    def _parse_sitemap_index(self, xml_text: str) -> list[str]:
        """Parse a sitemap index file to get child sitemap URLs."""
        child_sitemaps = []
        try:
            root = ElementTree.fromstring(xml_text)
            # Handle both namespaced and non-namespaced
            for sitemap in root.findall("sm:sitemap", SITEMAP_NS):
                loc = sitemap.find("sm:loc", SITEMAP_NS)
                if loc is not None and loc.text:
                    child_sitemaps.append(loc.text.strip())

            # Try without namespace
            if not child_sitemaps:
                for sitemap in root.findall("sitemap"):
                    loc = sitemap.find("loc")
                    if loc is not None and loc.text:
                        child_sitemaps.append(loc.text.strip())

        except ElementTree.ParseError:
            pass
        return child_sitemaps

    def _parse_sitemap_urls(self, xml_text: str) -> list[dict]:
        """Parse a sitemap file to extract URLs with metadata."""
        urls = []
        try:
            root = ElementTree.fromstring(xml_text)

            # Try namespaced first
            url_elements = root.findall("sm:url", SITEMAP_NS)
            if not url_elements:
                url_elements = root.findall("url")

            for url_elem in url_elements:
                loc = url_elem.find("sm:loc", SITEMAP_NS)
                if loc is None:
                    loc = url_elem.find("loc")
                if loc is None or not loc.text:
                    continue

                entry = {"url": loc.text.strip()}

                # Extract lastmod
                lastmod = url_elem.find("sm:lastmod", SITEMAP_NS)
                if lastmod is None:
                    lastmod = url_elem.find("lastmod")
                if lastmod is not None and lastmod.text:
                    entry["lastmod"] = lastmod.text.strip()

                # Extract changefreq
                changefreq = url_elem.find("sm:changefreq", SITEMAP_NS)
                if changefreq is None:
                    changefreq = url_elem.find("changefreq")
                if changefreq is not None and changefreq.text:
                    entry["changefreq"] = changefreq.text.strip()

                # Extract priority
                priority = url_elem.find("sm:priority", SITEMAP_NS)
                if priority is None:
                    priority = url_elem.find("priority")
                if priority is not None and priority.text:
                    try:
                        entry["priority"] = float(priority.text.strip())
                    except ValueError:
                        pass

                urls.append(entry)

        except ElementTree.ParseError:
            pass
        return urls

    def _calculate_priority(self, entry: dict) -> float:
        """Calculate crawl priority from sitemap metadata."""
        base = entry.get("priority", 0.5) * 5.0  # Sitemap priority 0-1 → 0-5

        # Recency boost from lastmod
        lastmod = entry.get("lastmod")
        if lastmod:
            try:
                # Handle various date formats
                for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                    try:
                        mod_date = datetime.strptime(lastmod[:19], fmt[:len(lastmod)])
                        break
                    except ValueError:
                        continue
                else:
                    mod_date = None

                if mod_date:
                    days_old = (datetime.utcnow() - mod_date.replace(tzinfo=None)).days
                    if days_old < 1:
                        base += 4.0   # Updated today
                    elif days_old < 7:
                        base += 3.0   # This week
                    elif days_old < 30:
                        base += 2.0   # This month
                    elif days_old < 90:
                        base += 1.0
            except Exception:
                pass

        # Changefreq boost
        freq_boost = {
            "always": 3.0, "hourly": 2.5, "daily": 2.0,
            "weekly": 1.0, "monthly": 0.5, "yearly": 0.0, "never": -1.0,
        }
        changefreq = entry.get("changefreq", "").lower()
        base += freq_boost.get(changefreq, 0.0)

        return max(0.1, min(base, 10.0))

    async def _discover_sitemaps(
        self, client: httpx.AsyncClient, domain: str
    ) -> list[str]:
        """Discover all sitemap URLs for a domain."""
        found = set()

        # 1. Check robots.txt
        robots_sitemaps = await self._fetch_robots_sitemaps(client, domain)
        found.update(robots_sitemaps)

        # 2. Probe common paths
        for path in SITEMAP_PATHS:
            url = f"https://{domain}{path}"
            if url not in found:
                try:
                    resp = await client.head(url, timeout=10)
                    if resp.status_code == 200:
                        found.add(url)
                except Exception:
                    continue

        if not found:
            logger.debug("sitemap_none_found", domain=domain)

        return list(found)

    async def _process_domain(
        self, client: httpx.AsyncClient, domain: str, max_urls: int = 500
    ) -> list[dict]:
        """Process all sitemaps for a domain, return URL entries."""
        all_urls = []

        sitemap_urls = await self._discover_sitemaps(client, domain)
        if not sitemap_urls:
            return []

        processed_sitemaps = set()
        queue = list(sitemap_urls)

        while queue and len(all_urls) < max_urls:
            sitemap_url = queue.pop(0)
            if sitemap_url in processed_sitemaps:
                continue
            processed_sitemaps.add(sitemap_url)

            xml_text = await self._fetch_sitemap(client, sitemap_url)
            if not xml_text:
                continue

            # Try as sitemap index first
            children = self._parse_sitemap_index(xml_text)
            if children:
                queue.extend(c for c in children if c not in processed_sitemaps)
                logger.debug("sitemap_index_found", url=sitemap_url[:80], children=len(children))
                continue

            # Parse as regular sitemap
            urls = self._parse_sitemap_urls(xml_text)
            all_urls.extend(urls)
            logger.debug("sitemap_parsed", url=sitemap_url[:80], urls=len(urls))

        self._discovered_domains.add(domain)
        return all_urls[:max_urls]

    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """
        Discover sitemaps from known domains and yield URLs as CrawlJobs.
        """
        sm_config = self._config.ingestion.sources.sitemap

        if not sm_config.enabled:
            return

        domains = await self._get_domains_from_frontier(
            limit=sm_config.domains_per_cycle
        )

        if not domains:
            logger.debug("sitemap_no_new_domains")
            return

        async with httpx.AsyncClient(
            headers={"User-Agent": self._config.ingestion.user_agent},
            follow_redirects=True,
        ) as client:
            for domain in domains:
                try:
                    entries = await self._process_domain(
                        client, domain, max_urls=sm_config.max_urls_per_domain
                    )

                    for entry in entries:
                        url = entry.get("url", "")
                        if not url:
                            continue

                        priority = self._calculate_priority(entry)

                        yield CrawlJob(
                            url=url,
                            priority=priority,
                            depth=0,
                            parent_url=None,
                            source_id=self._source_id,
                        )
                        self._total_items += 1

                    if entries:
                        logger.info(
                            "sitemap_domain_processed",
                            domain=domain,
                            urls=len(entries),
                        )

                except Exception as e:
                    logger.error("sitemap_domain_failed", domain=domain, error=str(e))
                    continue

        self._last_check = datetime.utcnow()
        self._consecutive_failures = 0

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=SourceType.WEB_CRAWL,
            content_type=ContentType.ARTICLE,
            title=crawl_result.title or "",
            full_text=crawl_result.cleaned_text or "",
        )

    def get_health(self) -> SourceHealth:
        return SourceHealth(
            source_id=self._source_id,
            source_name="Sitemap Crawler",
            source_type=SourceType.WEB_CRAWL,
            status="healthy" if self._consecutive_failures < 3 else "degraded",
            last_successful_check=self._last_check,
            consecutive_failures=self._consecutive_failures,
            total_items_collected=self._total_items,
        )
