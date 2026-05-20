"""
SENTINEL Certificate Transparency Monitor.
Monitors crt.sh for newly issued certificates for target domains.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import AsyncGenerator

import httpx
import structlog

from sentinel.config import get_config
from sentinel.ingestion.base import BaseSource
from uuid import uuid4
from sentinel.models import (
    CrawlJob, CrawlResult, ExtractedContent, 
    SourceHealth, SourceType, ContentType
)

logger = structlog.get_logger(__name__)


class CTMonitorSource(BaseSource):
    """
    Monitors Certificate Transparency logs (crt.sh).
    Finds new subdomains, dev/staging environments, and hidden portals.
    """

    def __init__(self) -> None:
        self._config = get_config()
        self._domains = self._config.intelligence.semantic_diff.tracked_url_patterns
        self.source_id = uuid4()

    @property
    def name(self) -> str:
        return "ct_monitor"

    @property
    def source_type(self) -> SourceType:
        return SourceType.CT_MONITOR

    async def check(self) -> AsyncGenerator[CrawlJob, None]:
        """
        Poll crt.sh for certificates issued to tracked domains.
        """
        if not self._domains:
            logger.debug("no_ct_monitor_domains")
            return

        timeout = self._config.ingestion.request_timeout
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            for domain in self._domains:
                try:
                    # Clean up domain pattern for crt.sh (e.g., * -> %)
                    clean_domain = domain.replace("*", "%").replace("https://", "").replace("http://", "")
                    if "/" in clean_domain:
                        clean_domain = clean_domain.split("/")[0]

                    # crt.sh json endpoint
                    url = f"https://crt.sh/?q=%.{clean_domain}&output=json"
                    
                    resp = await client.get(
                        url, 
                        headers={"User-Agent": "SentinelBot/1.0"},
                        timeout=timeout
                    )
                    
                    if resp.status_code == 200:
                        certs = resp.json()
                        subdomains = set()
                        
                        # Process certs
                        for cert in certs:
                            name = cert.get("name_value", "")
                            for sub in name.split("\n"):
                                sub = sub.strip()
                                if sub and sub != clean_domain and not sub.startswith("*"):
                                    subdomains.add(sub)
                                    
                        for sub in subdomains:
                            yield CrawlJob(
                                url=f"https://{sub}",
                                source_id=self.source_id,
                                priority=0.8
                            )
                            
                    await asyncio.sleep(60.0 / self._config.stealth.rate_limiting.default_requests_per_minute)
                        
                except Exception as e:
                    logger.error("ct_monitor_failed", domain=domain, error=str(e))

    async def extract(self, crawl_result: CrawlResult) -> ExtractedContent:
        return ExtractedContent(
            crawl_job_id=crawl_result.job_id,
            url=crawl_result.url,
            source_type=self.source_type,
            content_type=ContentType.ARTICLE,
            title=f"CT Discovered: {crawl_result.url}",
            full_text=crawl_result.cleaned_text or "No content",
            published_at=datetime.utcnow()
        )

    def get_health(self) -> SourceHealth:
        return SourceHealth(
            source_id=self.source_id,
            source_name="ct_monitor",
            source_type=self.source_type,
            status="healthy",
            last_successful_check=datetime.utcnow(),
            consecutive_failures=0
        )
