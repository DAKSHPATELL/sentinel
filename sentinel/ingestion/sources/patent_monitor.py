"""
SENTINEL Patent Monitor.
Monitors multiple patent databases for new filings in tracked CPC codes.

Sources:
  1. Google Patents (via SerpAPI-like scraping of patents.google.com)
  2. USPTO PatentsView API (free, structured, US patents)
  3. EPO Open Patent Services (free with registration, European + international)
  4. WIPO PATENTSCOPE (international PCT applications)
  5. Lens.org (aggregated patent + scholarly data)

Patents are the single most valuable early warning signal — filed 12-18 months
before products launch, they reveal strategic intent before any press release.
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional
from urllib.parse import quote_plus, urlencode
from uuid import uuid4

import httpx
import structlog

from sentinel.config import get_config
from sentinel.ingestion.base import BaseSource
from sentinel.models import (
    CrawlJob,
    CrawlResult,
    ContentType,
    ExtractedContent,
    SourceHealth,
    SourceType,
)

logger = structlog.get_logger(__name__)

# CPC code descriptions for context enrichment
CPC_DESCRIPTIONS = {
    "G06N": "AI, Machine Learning & Neural Networks",
    "G06F": "Computing & Data Processing",
    "H04L": "Telecommunications & Networking",
    "G16H": "Healthcare Informatics",
    "Y02E": "Clean Energy Technologies",
    "G06Q": "Business Methods & Fintech",
    "G16B": "Bioinformatics",
    "G06V": "Computer Vision & Image Recognition",
    "H04W": "Wireless Communication",
    "G06T": "Image Data Processing & 3D",
    "A61B": "Medical Diagnostics",
    "H01L": "Semiconductor Devices",
    "B25J": "Robotics & Manipulators",
    "G01N": "Material Analysis & Sensors",
    "H02J": "Power Distribution & Battery Systems",
}

# Major patent assignees to boost priority when seen
HIGH_VALUE_ASSIGNEES = {
    "apple", "google", "alphabet", "microsoft", "amazon", "meta", "nvidia",
    "openai", "anthropic", "deepmind", "tesla", "spacex", "ibm", "intel",
    "samsung", "tsmc", "asml", "arm", "qualcomm", "broadcom", "amd",
    "palantir", "databricks", "snowflake", "stripe", "coinbase",
    "moderna", "biontech", "illumina", "crispr",
}


class PatentMonitor(BaseSource):
    """
    Multi-source patent monitoring system.

    Checks USPTO, Google Patents, EPO, and WIPO for new filings
    matching configured CPC codes and keywords.
    """

    def __init__(self) -> None:
        self._config = get_config()
        self._patent_config = self._config.ingestion.sources.patents
        self._source_id = uuid4()
        self._last_check: Optional[datetime] = None
        self._consecutive_failures = 0
        self._total_collected = 0

    @property
    def name(self) -> str:
        return "patents"

    @property
    def source_type(self) -> SourceType:
        return SourceType.PATENT

    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """
        Poll all patent sources for new filings.
        Yields CrawlJobs for each discovered patent.
        """
        if not self._patent_config.enabled:
            return

        timeout = self._config.ingestion.request_timeout
        rate_delay = 60.0 / self._config.stealth.rate_limiting.default_requests_per_minute

        async with httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": self._config.ingestion.user_agent},
            follow_redirects=True,
        ) as client:
            # Source 1: USPTO PatentsView API (most reliable, free, structured)
            async for job in self._check_uspto(client, rate_delay):
                yield job

            # Source 2: Google Patents search
            async for job in self._check_google_patents(client, rate_delay):
                yield job

            # Source 3: EPO Open Patent Services
            async for job in self._check_epo(client, rate_delay):
                yield job

            # Source 4: WIPO PATENTSCOPE
            async for job in self._check_wipo(client, rate_delay):
                yield job

            # Source 5: Lens.org (aggregated)
            async for job in self._check_lens(client, rate_delay):
                yield job

        self._last_check = datetime.utcnow()

    # ── USPTO PatentsView API ─────────────────────────────────────

    async def _check_uspto(
        self, client: httpx.AsyncClient, rate_delay: float
    ) -> AsyncGenerator[CrawlJob, None]:
        """
        Query USPTO PatentsView API for recent patents by CPC code.

        PatentsView is free, no auth required, returns structured JSON.
        Endpoint: https://api.patentsview.org/patents/query
        """
        # Calculate date window: patents published since last check (or last 7 days)
        since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

        for cpc in self._patent_config.cpc_codes:
            try:
                # PatentsView query format
                query = {
                    "q": f'{{"_and":[{{"_gte":{{"patent_date":"{since}"}}}},{{"_begins":{{"cpc_subgroup_id":"{cpc}"}}}}]}}',
                    "f": '["patent_number","patent_title","patent_date","patent_abstract","assignee_organization","cpc_subgroup_id","inventor_last_name","patent_type"]',
                    "o": '{"page":1,"per_page":50}',
                }

                resp = await client.get(
                    "https://api.patentsview.org/patents/query",
                    params=query,
                )

                if resp.status_code == 200:
                    data = resp.json()
                    patents = data.get("patents", [])

                    for patent in patents:
                        patent_num = patent.get("patent_number", "")
                        title = patent.get("patent_title", "")
                        assignees = patent.get("assignees", [])

                        # Calculate priority
                        priority = self._calculate_patent_priority(
                            title=title,
                            cpc_code=cpc,
                            assignees=assignees,
                        )

                        # Patent detail URL
                        url = f"https://patents.google.com/patent/US{patent_num}"

                        yield CrawlJob(
                            url=url,
                            source_id=self._source_id,
                            priority=priority,
                            depth=0,
                        )
                        self._total_collected += 1

                    logger.debug(
                        "uspto_check_complete",
                        cpc=cpc,
                        patents_found=len(patents),
                    )
                else:
                    logger.warning("uspto_api_error", status=resp.status_code, cpc=cpc)

                await asyncio.sleep(rate_delay)

            except Exception as e:
                logger.error("uspto_check_failed", cpc=cpc, error=str(e))
                self._consecutive_failures += 1

    # ── Google Patents Search ─────────────────────────────────────

    async def _check_google_patents(
        self, client: httpx.AsyncClient, rate_delay: float
    ) -> AsyncGenerator[CrawlJob, None]:
        """
        Search Google Patents for recent filings by CPC code.

        Google Patents covers 100+ jurisdictions — the widest coverage
        of any free patent search. We scrape the search results page.
        """
        since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y%m%d")

        for cpc in self._patent_config.cpc_codes:
            for jurisdiction in self._patent_config.jurisdictions:
                try:
                    # Google Patents search URL
                    search_query = f"cpc={cpc} country={jurisdiction} after={since}"
                    url = f"https://patents.google.com/?q={quote_plus(search_query)}&oq={quote_plus(search_query)}"

                    resp = await client.get(url)

                    if resp.status_code == 200:
                        # Extract patent links from the search results page
                        patent_urls = self._extract_google_patent_links(resp.text)

                        for patent_url in patent_urls[:20]:  # Cap per query
                            priority = self._calculate_patent_priority(
                                title="",
                                cpc_code=cpc,
                                assignees=[],
                            )

                            yield CrawlJob(
                                url=patent_url,
                                source_id=self._source_id,
                                priority=priority,
                                depth=0,
                            )
                            self._total_collected += 1

                        logger.debug(
                            "google_patents_check",
                            cpc=cpc,
                            jurisdiction=jurisdiction,
                            found=len(patent_urls),
                        )

                    await asyncio.sleep(rate_delay * 2)  # Extra polite to Google

                except Exception as e:
                    logger.error(
                        "google_patents_failed",
                        cpc=cpc,
                        jurisdiction=jurisdiction,
                        error=str(e),
                    )

    def _extract_google_patent_links(self, html: str) -> list[str]:
        """Extract patent detail URLs from Google Patents search results."""
        # Google Patents uses /patent/XX... links
        pattern = r'href="(/patent/[A-Z]{2}\d+[A-Z]?\d*)"'
        matches = re.findall(pattern, html)
        # Deduplicate and make absolute
        seen = set()
        urls = []
        for match in matches:
            if match not in seen:
                seen.add(match)
                urls.append(f"https://patents.google.com{match}")
        return urls

    # ── EPO Open Patent Services ──────────────────────────────────

    async def _check_epo(
        self, client: httpx.AsyncClient, rate_delay: float
    ) -> AsyncGenerator[CrawlJob, None]:
        """
        Query EPO Open Patent Services for recent European and international patents.

        OPS is free (up to ~2000 requests/week without registration, more with free API key).
        Uses the published-data/search endpoint.
        """
        since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y%m%d")

        for cpc in self._patent_config.cpc_codes:
            try:
                # EPO OPS search query (CQL syntax)
                cql_query = f'cpc="{cpc}" and pd>={since}'
                params = {
                    "q": cql_query,
                    "Range": "1-50",
                }

                resp = await client.get(
                    "https://ops.epo.org/3.2/rest-services/published-data/search",
                    params=params,
                    headers={"Accept": "application/json"},
                )

                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        results = (
                            data.get("ops:world-patent-data", {})
                            .get("ops:biblio-search", {})
                            .get("ops:search-result", {})
                            .get("ops:publication-reference", [])
                        )

                        if isinstance(results, dict):
                            results = [results]

                        for pub_ref in results:
                            doc_id = pub_ref.get("document-id", {})
                            country = doc_id.get("country", {}).get("$", "")
                            doc_number = doc_id.get("doc-number", {}).get("$", "")

                            if country and doc_number:
                                url = f"https://patents.google.com/patent/{country}{doc_number}"
                                yield CrawlJob(
                                    url=url,
                                    source_id=self._source_id,
                                    priority=self._calculate_patent_priority("", cpc, []),
                                    depth=0,
                                )
                                self._total_collected += 1

                    except Exception as parse_err:
                        logger.debug("epo_parse_error", error=str(parse_err))

                elif resp.status_code == 403:
                    logger.info("epo_rate_limited", cpc=cpc)
                    await asyncio.sleep(60)  # Back off for a minute

                await asyncio.sleep(rate_delay)

            except Exception as e:
                logger.error("epo_check_failed", cpc=cpc, error=str(e))

    # ── WIPO PATENTSCOPE ──────────────────────────────────────────

    async def _check_wipo(
        self, client: httpx.AsyncClient, rate_delay: float
    ) -> AsyncGenerator[CrawlJob, None]:
        """
        Search WIPO PATENTSCOPE for international PCT applications.

        PCT (Patent Cooperation Treaty) applications are the earliest filings
        for patents that will be filed in multiple countries — the strongest
        indicator of serious commercial intent.
        """
        since = (datetime.utcnow() - timedelta(days=14)).strftime("%Y%m%d")

        for cpc in self._patent_config.cpc_codes:
            try:
                # WIPO PATENTSCOPE search
                params = {
                    "query": f'IC:({cpc}*) AND DP:[{since} TO *]',
                    "resultSetSize": "50",
                    "sortField": "DP",
                    "sortOrder": "desc",
                }

                resp = await client.get(
                    "https://patentscope.wipo.int/search/en/result.jsf",
                    params=params,
                )

                if resp.status_code == 200:
                    # Extract patent links from WIPO search results
                    wipo_links = re.findall(
                        r'href="(/search/en/detail\.jsf\?docId=[^"]+)"',
                        resp.text,
                    )

                    seen = set()
                    for link in wipo_links[:30]:
                        if link not in seen:
                            seen.add(link)
                            url = f"https://patentscope.wipo.int{link}"
                            yield CrawlJob(
                                url=url,
                                source_id=self._source_id,
                                priority=self._calculate_patent_priority("", cpc, []),
                                depth=0,
                            )
                            self._total_collected += 1

                    logger.debug("wipo_check", cpc=cpc, found=len(seen))

                await asyncio.sleep(rate_delay)

            except Exception as e:
                logger.error("wipo_check_failed", cpc=cpc, error=str(e))

    # ── Lens.org ──────────────────────────────────────────────────

    async def _check_lens(
        self, client: httpx.AsyncClient, rate_delay: float
    ) -> AsyncGenerator[CrawlJob, None]:
        """
        Search Lens.org for recent patents (aggregated from multiple offices).

        Lens.org is unique because it links patents to scholarly works,
        enabling detection of research-to-patent pipelines.
        """
        since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

        for cpc in self._patent_config.cpc_codes:
            try:
                search_url = (
                    f"https://www.lens.org/lens/search/patent/list?"
                    f"q=classification_cpc.symbol%3A{cpc}*"
                    f"&dateFilterField=date_published"
                    f"&filterMap=%7B%22date_published%22%3A%7B%22from%22%3A%22{since}%22%7D%7D"
                    f"&orderBy=%2Bdate_published"
                )

                resp = await client.get(search_url)

                if resp.status_code == 200:
                    # Extract patent links from Lens.org results
                    lens_links = re.findall(
                        r'href="(/lens/patent/\d+-\d+-\d+/fulltext)"',
                        resp.text,
                    )

                    seen = set()
                    for link in lens_links[:20]:
                        if link not in seen:
                            seen.add(link)
                            url = f"https://www.lens.org{link}"
                            yield CrawlJob(
                                url=url,
                                source_id=self._source_id,
                                priority=self._calculate_patent_priority("", cpc, []),
                                depth=0,
                            )
                            self._total_collected += 1

                    logger.debug("lens_check", cpc=cpc, found=len(seen))

                await asyncio.sleep(rate_delay)

            except Exception as e:
                logger.error("lens_check_failed", cpc=cpc, error=str(e))

    # ── Priority Calculation ──────────────────────────────────────

    def _calculate_patent_priority(
        self,
        title: str,
        cpc_code: str,
        assignees: list,
    ) -> float:
        """
        Calculate priority for a patent filing.

        Factors:
        - CPC code weight (AI/ML patents > general computing)
        - Assignee importance (FAANG/major tech > unknown)
        - Title keyword signals (e.g., "neural", "autonomous", "quantum")
        """
        base_priority = 0.6

        # CPC code weight
        cpc_weights = {
            "G06N": 1.0,  # AI/ML — highest value
            "G16H": 0.9,  # Healthcare IT
            "Y02E": 0.9,  # Clean energy
            "G06V": 0.85, # Computer vision
            "G16B": 0.85, # Bioinformatics
            "H04L": 0.7,  # Networking
            "G06F": 0.6,  # General computing
            "G06Q": 0.7,  # Business methods
        }
        cpc_prefix = cpc_code[:4] if len(cpc_code) >= 4 else cpc_code
        base_priority += cpc_weights.get(cpc_prefix, 0.5) * 0.3

        # Assignee boost
        if assignees:
            for assignee_data in assignees:
                org = ""
                if isinstance(assignee_data, dict):
                    org = assignee_data.get("assignee_organization", "")
                elif isinstance(assignee_data, str):
                    org = assignee_data

                if org and org.lower() in HIGH_VALUE_ASSIGNEES:
                    base_priority += 0.3
                    break

        # Title keyword boost
        title_lower = title.lower() if title else ""
        high_value_keywords = [
            "neural", "transformer", "autonomous", "quantum", "fusion",
            "crispr", "gene", "robot", "lidar", "blockchain",
            "foundation model", "large language", "diffusion",
            "protein", "battery", "photovoltaic", "nuclear",
        ]
        for kw in high_value_keywords:
            if kw in title_lower:
                base_priority += 0.15
                break

        return min(base_priority, 2.0)

    # ── Extraction ────────────────────────────────────────────────

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        """Extract structured content from a crawled patent page."""
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=self.source_type,
            content_type=ContentType.PATENT_DOC,
            title=crawl_result.title or f"Patent: {crawl_result.url}",
            full_text=crawl_result.cleaned_text or "No content extracted",
            published_at=datetime.utcnow(),
            structured_data={
                "patent_source": self._identify_source(crawl_result.url),
            },
        )

    def _identify_source(self, url: str) -> str:
        """Identify which patent database a URL came from."""
        if "patents.google.com" in url:
            return "google_patents"
        elif "patentsview" in url:
            return "uspto_patentsview"
        elif "epo.org" in url:
            return "epo"
        elif "patentscope.wipo.int" in url:
            return "wipo"
        elif "lens.org" in url:
            return "lens"
        return "unknown"

    def get_health(self) -> SourceHealth:
        return SourceHealth(
            source_id=self._source_id,
            source_name="patents",
            source_type=self.source_type,
            status="healthy" if self._consecutive_failures < 5 else "degraded",
            last_successful_check=self._last_check,
            consecutive_failures=self._consecutive_failures,
            total_items_collected=self._total_collected,
        )
