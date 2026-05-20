"""
SENTINEL Obstacle Annihilation Engine.
12-strategy multi-path content acquisition with domain profile learning.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Optional
from urllib.parse import urlparse, urljoin

import httpx
import structlog

from sentinel.config import get_config
from sentinel.core.sqlite_client import SQLiteClient
from sentinel.models import AcquisitionResult

logger = structlog.get_logger(__name__)


# ─── 12 ACQUISITION STRATEGIES ──────────────────────────────────


async def direct_http(url: str, timeout: float = 15.0) -> Optional[str]:
    """Strategy 1: Standard httpx.get with default headers."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(url, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        resp.raise_for_status()
        return resp.text


async def tls_impersonate(url: str, timeout: float = 15.0) -> Optional[str]:
    """Strategy 2: TLS impersonation with curl_cffi (Chrome/Safari/Firefox profiles)."""
    try:
        from curl_cffi.requests import AsyncSession
        profiles = ["chrome120", "safari17_0", "firefox120"]
        import random
        profile = random.choice(profiles)
        async with AsyncSession(impersonate=profile) as session:
            resp = await session.get(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 200:
                return resp.text
    except ImportError:
        # curl_cffi not installed — fall back to httpx with custom TLS
        async with httpx.AsyncClient(timeout=timeout, http2=True) as client:
            resp = await client.get(url, follow_redirects=True, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-Mode": "navigate",
            })
            resp.raise_for_status()
            return resp.text
    return None


async def stealth_browser(url: str, timeout: float = 30.0) -> Optional[str]:
    """Strategy 3: Full browser render via Camoufox (or Playwright fallback)."""
    try:
        # Try Camoufox first
        from camoufox.async_api import AsyncCamoufox
        async with AsyncCamoufox(headless=True) as browser:
            page = await browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout * 1000))
            content = await page.content()
            await page.close()
            return content
    except (ImportError, Exception):
        pass

    try:
        # Fall back to Playwright
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout * 1000))
            content = await page.content()
            await browser.close()
            return content
    except (ImportError, Exception):
        pass
    return None


async def google_cache(url: str, timeout: float = 15.0) -> Optional[str]:
    """Strategy 4: Fetch from Google Cache."""
    cache_url = f"https://webcache.googleusercontent.com/search?q=cache:{url}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(cache_url, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        })
        if resp.status_code == 200 and len(resp.text) > 500:
            return resp.text
    return None


async def wayback_machine(url: str, timeout: float = 20.0) -> Optional[str]:
    """Strategy 5: Fetch latest capture from Wayback Machine."""
    wayback_url = f"https://web.archive.org/web/2/{url}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(wayback_url, follow_redirects=True)
        if resp.status_code == 200 and len(resp.text) > 500:
            return resp.text
    return None


async def api_discovery(url: str, timeout: float = 15.0) -> Optional[str]:
    """Strategy 6: Check for API endpoints, sitemap.xml, GraphQL."""
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Check sitemap.xml for alternate URLs
        for endpoint in [f"{base}/sitemap.xml", f"{base}/api/", f"{base}/graphql"]:
            try:
                resp = await client.get(endpoint, follow_redirects=True)
                if resp.status_code == 200 and len(resp.text) > 100:
                    return resp.text
            except Exception:
                continue
    return None


async def rss_fallback(url: str, timeout: float = 15.0) -> Optional[str]:
    """Strategy 7: Find RSS/Atom feed via link tags from any accessible version."""
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Try common feed paths
        feed_paths = ["/rss", "/feed", "/atom.xml", "/rss.xml", "/feeds/posts/default"]
        for path in feed_paths:
            try:
                resp = await client.get(f"{base}{path}", follow_redirects=True)
                if resp.status_code == 200 and ("rss" in resp.text[:500].lower() or "atom" in resp.text[:500].lower()):
                    # Parse feed and find matching URL content
                    import feedparser
                    feed = feedparser.parse(resp.text)
                    for entry in feed.entries:
                        if entry.get("link") == url or url in entry.get("link", ""):
                            return entry.get("summary", "") or entry.get("content", [{}])[0].get("value", "")
            except Exception:
                continue
    return None


async def google_amp(url: str, timeout: float = 15.0) -> Optional[str]:
    """Strategy 8: Try Google AMP version."""
    parsed = urlparse(url)
    domain = parsed.netloc
    path = parsed.path
    amp_url = f"https://ampproject.org/c/s/{domain}{path}"

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.get(amp_url, follow_redirects=True)
            if resp.status_code == 200 and len(resp.text) > 500:
                return resp.text
        except Exception:
            pass
    return None


async def social_extraction(url: str, timeout: float = 15.0) -> Optional[str]:
    """Strategy 9: Search Reddit/HN for quotes of this URL."""
    fragments = []

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Search HN Algolia API
        try:
            resp = await client.get(
                "https://hn.algolia.com/api/v1/search",
                params={"query": url, "tags": "story"},
            )
            if resp.status_code == 200:
                data = resp.json()
                for hit in data.get("hits", [])[:3]:
                    title = hit.get("title", "")
                    if title:
                        fragments.append(f"[HN] {title}")
                    # Also get comments for quoted content
                    story_id = hit.get("objectID")
                    if story_id:
                        comments_resp = await client.get(
                            f"https://hn.algolia.com/api/v1/items/{story_id}"
                        )
                        if comments_resp.status_code == 200:
                            story = comments_resp.json()
                            for child in (story.get("children") or [])[:5]:
                                text = child.get("text", "")
                                if text and len(text) > 50:
                                    fragments.append(text[:500])
        except Exception:
            pass

        # Search Reddit
        try:
            resp = await client.get(
                f"https://www.reddit.com/search.json",
                params={"q": f'url:"{url}"', "sort": "relevance", "limit": 5},
                headers={"User-Agent": "SentinelBot/1.0"},
            )
            if resp.status_code == 200:
                data = resp.json()
                for post in data.get("data", {}).get("children", []):
                    d = post.get("data", {})
                    title = d.get("title", "")
                    selftext = d.get("selftext", "")
                    if title:
                        fragments.append(f"[Reddit] {title}")
                    if selftext and len(selftext) > 50:
                        fragments.append(selftext[:500])
        except Exception:
            pass

    if fragments:
        return "\n\n".join(fragments)
    return None


async def dns_alternate(url: str, timeout: float = 10.0) -> Optional[str]:
    """Strategy 10: Try www/non-www and alternate TLDs."""
    parsed = urlparse(url)
    domain = parsed.netloc

    alternates = []
    if domain.startswith("www."):
        alternates.append(domain[4:])
    else:
        alternates.append(f"www.{domain}")

    async with httpx.AsyncClient(timeout=timeout) as client:
        for alt_domain in alternates:
            alt_url = f"{parsed.scheme}://{alt_domain}{parsed.path}"
            try:
                resp = await client.get(alt_url, follow_redirects=True)
                if resp.status_code == 200 and len(resp.text) > 500:
                    return resp.text
            except Exception:
                continue
    return None


async def ct_subdomains(url: str, timeout: float = 20.0) -> Optional[str]:
    """Strategy 11: Query crt.sh for alternate subdomains."""
    parsed = urlparse(url)
    # Get base domain (strip subdomain)
    parts = parsed.netloc.split(".")
    base_domain = ".".join(parts[-2:]) if len(parts) > 2 else parsed.netloc

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.get(
                f"https://crt.sh/?q=%.{base_domain}&output=json"
            )
            if resp.status_code == 200:
                certs = resp.json()
                subdomains = set()
                for cert in certs[:50]:
                    name = cert.get("name_value", "")
                    for subdomain in name.split("\n"):
                        subdomain = subdomain.strip()
                        if subdomain and subdomain != parsed.netloc and not subdomain.startswith("*"):
                            subdomains.add(subdomain)

                for subdomain in list(subdomains)[:5]:
                    alt_url = f"{parsed.scheme}://{subdomain}{parsed.path}"
                    try:
                        r = await client.get(alt_url, follow_redirects=True, timeout=5.0)
                        if r.status_code == 200 and len(r.text) > 500:
                            return r.text
                    except Exception:
                        continue
        except Exception:
            pass
    return None


async def aggregator_search(url: str, timeout: float = 15.0) -> Optional[str]:
    """Strategy 12: Search news aggregators for the same content."""
    parsed = urlparse(url)
    # Extract a search query from the URL path
    path_parts = parsed.path.strip("/").split("/")
    search_query = " ".join(
        p.replace("-", " ").replace("_", " ")
        for p in path_parts[-2:]
        if len(p) > 3 and not p.isdigit()
    )

    if not search_query or len(search_query) < 5:
        return None

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Try DuckDuckGo instant answer API
        try:
            resp = await client.get(
                "https://api.duckduckgo.com/",
                params={"q": search_query, "format": "json", "no_redirect": 1},
            )
            if resp.status_code == 200:
                data = resp.json()
                abstract = data.get("AbstractText", "")
                if abstract and len(abstract) > 100:
                    return abstract
        except Exception:
            pass
    return None


# ─── STRATEGY REGISTRY ──────────────────────────────────────────


ALL_STRATEGIES = [
    ("direct_http", direct_http),
    ("tls_impersonate", tls_impersonate),
    ("stealth_browser", stealth_browser),
    ("google_cache", google_cache),
    ("wayback_machine", wayback_machine),
    ("api_discovery", api_discovery),
    ("rss_fallback", rss_fallback),
    ("google_amp", google_amp),
    ("social_extraction", social_extraction),
    ("dns_alternate", dns_alternate),
    ("ct_subdomains", ct_subdomains),
    ("aggregator_search", aggregator_search),
]


# ─── ACQUISITION ORCHESTRATOR ───────────────────────────────────


class AcquisitionOrchestrator:
    """
    Multi-strategy content acquisition with domain profile learning.

    For each URL:
    1. Load domain profile (historical success rates per strategy)
    2. Launch top N strategies in parallel
    3. If any succeeds: return content, update profile
    4. If all N fail: launch remaining strategies
    5. If all fail: schedule for temporal arbitrage
    """

    def __init__(self, sqlite_client: SQLiteClient) -> None:
        self._sqlite = sqlite_client
        self._config = get_config()
        self._domain_profiles: dict[str, dict[str, dict]] = {}

    async def initialize(self) -> None:
        """Initialize domain_strategy_stats table."""
        await self._sqlite.execute("""
            CREATE TABLE IF NOT EXISTS domain_strategy_stats (
                domain TEXT NOT NULL,
                strategy TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                successes INTEGER DEFAULT 0,
                avg_latency_ms REAL DEFAULT 0,
                last_used_at TEXT,
                PRIMARY KEY (domain, strategy)
            )
        """)

    async def _get_domain_profile(self, domain: str) -> dict[str, dict]:
        """Load domain strategy success rates from SQLite."""
        if domain in self._domain_profiles:
            return self._domain_profiles[domain]

        rows = await self._sqlite.query(
            "SELECT strategy, attempts, successes, avg_latency_ms FROM domain_strategy_stats WHERE domain = ?",
            (domain,),
        )
        profile = {}
        for row in rows:
            attempts = row["attempts"]
            successes = row["successes"]
            rate = successes / max(attempts, 1)
            profile[row["strategy"]] = {
                "attempts": attempts,
                "successes": successes,
                "success_rate": rate,
                "avg_latency_ms": row["avg_latency_ms"],
            }
        self._domain_profiles[domain] = profile
        return profile

    async def _update_domain_profile(
        self, domain: str, strategy: str, success: bool, latency_ms: int
    ) -> None:
        """Update strategy stats for a domain."""
        await self._sqlite.execute(
            """
            INSERT INTO domain_strategy_stats (domain, strategy, attempts, successes, avg_latency_ms, last_used_at)
            VALUES (?, ?, 1, ?, ?, datetime('now'))
            ON CONFLICT(domain, strategy) DO UPDATE SET
                attempts = attempts + 1,
                successes = successes + ?,
                avg_latency_ms = (avg_latency_ms * attempts + ?) / (attempts + 1),
                last_used_at = datetime('now')
            """,
            (domain, strategy, int(success), latency_ms, int(success), latency_ms),
        )
        # Invalidate cache
        self._domain_profiles.pop(domain, None)

    def _sort_strategies_by_profile(
        self, profile: dict[str, dict]
    ) -> list[tuple[str, callable]]:
        """Sort strategies by domain-specific success rate (descending)."""
        def sort_key(item: tuple[str, callable]) -> float:
            name = item[0]
            stats = profile.get(name, {})
            return stats.get("success_rate", 0.5)  # Default 50% for unknown

        return sorted(ALL_STRATEGIES, key=sort_key, reverse=True)

    async def acquire(self, url: str) -> AcquisitionResult:
        """
        Acquire content using multi-strategy approach.

        Args:
            url: URL to acquire content from.

        Returns:
            AcquisitionResult with content or failure info.
        """
        start_time = time.perf_counter()
        domain = urlparse(url).netloc
        config = self._config.stealth.annihilator

        if not config.enabled:
            # Fallback to direct HTTP only
            try:
                content = await direct_http(url)
                if content:
                    return AcquisitionResult(
                        success=True, strategy_used="direct_http",
                        strategies_attempted=1, content=content,
                        acquisition_time_ms=int((time.perf_counter() - start_time) * 1000),
                    )
            except Exception:
                pass
            return AcquisitionResult(success=False, strategies_attempted=1)

        profile = await self._get_domain_profile(domain)
        sorted_strategies = self._sort_strategies_by_profile(profile)

        max_first_wave = config.max_parallel_strategies
        total_attempted = 0

        # Wave 1: Top N strategies in parallel
        first_wave = sorted_strategies[:max_first_wave]
        result = await self._execute_wave(url, domain, first_wave)
        total_attempted += len(first_wave)

        if result:
            result.strategies_attempted = total_attempted
            result.acquisition_time_ms = int((time.perf_counter() - start_time) * 1000)
            return result

        # Wave 2: Remaining strategies
        remaining = sorted_strategies[max_first_wave:]
        if remaining:
            result = await self._execute_wave(url, domain, remaining)
            total_attempted += len(remaining)

            if result:
                result.strategies_attempted = total_attempted
                result.acquisition_time_ms = int((time.perf_counter() - start_time) * 1000)
                return result

        logger.warning(
            "all_strategies_failed",
            url=url[:100],
            strategies_attempted=total_attempted,
        )

        return AcquisitionResult(
            success=False,
            strategies_attempted=total_attempted,
            acquisition_time_ms=int((time.perf_counter() - start_time) * 1000),
        )

    async def _execute_wave(
        self, url: str, domain: str, strategies: list[tuple[str, callable]]
    ) -> Optional[AcquisitionResult]:
        """Execute a wave of strategies in parallel, return first success."""
        tasks = []
        names = []
        for name, func in strategies:
            tasks.append(self._timed_strategy(name, func, url, domain))
            names.append(name)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for name, result in zip(names, results):
            if isinstance(result, tuple):
                content, latency_ms = result
                if content and len(content) > 100:
                    await self._update_domain_profile(domain, name, True, latency_ms)
                    logger.info(
                        "strategy_success",
                        strategy=name,
                        url=url[:80],
                        latency_ms=latency_ms,
                        content_len=len(content),
                    )
                    return AcquisitionResult(
                        success=True,
                        strategy_used=name,
                        content=content,
                        domain_profile_updated=True,
                    )
                else:
                    await self._update_domain_profile(domain, name, False, latency_ms)
            elif isinstance(result, Exception):
                logger.debug("strategy_failed", strategy=name, url=url[:80], error=str(result))
                await self._update_domain_profile(domain, name, False, 0)

        return None

    async def _timed_strategy(
        self, name: str, func: callable, url: str, domain: str
    ) -> tuple[Optional[str], int]:
        """Run a strategy and return (content, latency_ms)."""
        start = time.perf_counter()
        try:
            content = await asyncio.wait_for(func(url), timeout=30.0)
            latency_ms = int((time.perf_counter() - start) * 1000)
            return content, latency_ms
        except asyncio.TimeoutError:
            return None, int((time.perf_counter() - start) * 1000)
        except Exception as e:
            latency_ms = int((time.perf_counter() - start) * 1000)
            raise

    def get_stats(self) -> dict:
        """Get cached domain profiles."""
        return {
            domain: {
                name: stats.get("success_rate", 0)
                for name, stats in profile.items()
            }
            for domain, profile in self._domain_profiles.items()
        }
