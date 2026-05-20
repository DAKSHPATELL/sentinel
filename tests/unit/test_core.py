"""
SENTINEL unit tests.
Tests for URL normalization, priority calculation, rate limiter,
DOM pruning, deduplication, events, and frontier.
"""
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from sentinel.ingestion.frontier import normalize_url, calculate_priority, extract_domain


# ─── URL NORMALIZATION TESTS ────────────────────────────────────


class TestURLNormalization:
    """Test URL normalization per PRD requirements."""

    def test_lowercase_scheme_and_host(self):
        assert normalize_url("HTTP://WWW.EXAMPLE.COM/page") == "http://www.example.com/page"

    def test_strip_fragment(self):
        assert normalize_url("https://example.com/page#section") == "https://example.com/page"

    def test_strip_trailing_slash(self):
        assert normalize_url("https://example.com/page/") == "https://example.com/page"

    def test_keep_root_slash(self):
        assert normalize_url("https://example.com/") == "https://example.com/"

    def test_sort_query_params(self):
        result = normalize_url("https://example.com/page?b=2&a=1")
        assert "a=1" in result
        assert result.index("a=1") < result.index("b=2")

    def test_strip_utm_params(self):
        result = normalize_url("https://example.com/page?utm_source=twitter&utm_medium=social&real=data")
        assert "utm_source" not in result
        assert "utm_medium" not in result
        assert "real=data" in result

    def test_strip_fbclid(self):
        result = normalize_url("https://example.com/page?fbclid=abc123&key=value")
        assert "fbclid" not in result
        assert "key=value" in result

    def test_strip_gclid(self):
        result = normalize_url("https://example.com/page?gclid=abc123")
        assert "gclid" not in result

    def test_strip_ref(self):
        result = normalize_url("https://example.com/page?ref=homepage")
        assert "ref=" not in result

    def test_empty_path(self):
        assert normalize_url("https://example.com") == "https://example.com/"

    def test_already_normalized(self):
        url = "https://example.com/page?a=1&b=2"
        assert normalize_url(url) == url

    def test_complex_url(self):
        url = "HTTPS://NEWS.YCOMBINATOR.COM/item?id=12345&ref=rss#comments"
        result = normalize_url(url)
        assert result == "https://news.ycombinator.com/item?id=12345"


# ─── PRIORITY CALCULATION TESTS ────────────────────────────────


class TestPriorityCalculation:
    """Test priority calculation per PRD Appendix A.1."""

    def test_base_priority(self):
        """Base priority with all defaults should equal base_priority."""
        result = calculate_priority(5.0)
        assert result == 5.0

    def test_source_weight(self):
        """Source weight should scale linearly."""
        result = calculate_priority(5.0, source_weight=2.0)
        assert result == 10.0

    def test_freshness_decay(self):
        """Priority should decay over time."""
        fresh = calculate_priority(5.0, hours_since_created=0)
        old = calculate_priority(5.0, hours_since_created=24)
        assert old < fresh

    def test_depth_penalty(self):
        """Deeper URLs should have lower priority."""
        shallow = calculate_priority(5.0, depth=0)
        deep = calculate_priority(5.0, depth=3)
        assert deep < shallow

    def test_domain_reputation(self):
        """High-reputation domains should boost priority."""
        low_rep = calculate_priority(5.0, domain_reputation=0.5)
        high_rep = calculate_priority(5.0, domain_reputation=2.0)
        assert high_rep > low_rep

    def test_combined_factors(self):
        """All factors combined should produce expected result."""
        result = calculate_priority(
            base_priority=5.0,
            source_weight=1.5,
            hours_since_created=1.0,
            depth=2,
            domain_reputation=1.2,
            priority_decay=0.95,
        )
        expected = 5.0 * 1.5 * (0.95 ** 1.0) * (0.9 ** 2) * 1.2
        assert abs(result - expected) < 0.001


# ─── EXTRACT DOMAIN TESTS ──────────────────────────────────────


class TestExtractDomain:
    """Test domain extraction."""

    def test_simple(self):
        assert extract_domain("https://example.com/page") == "example.com"

    def test_with_port(self):
        assert extract_domain("https://example.com:8080/page") == "example.com:8080"

    def test_with_subdomain(self):
        assert extract_domain("https://www.example.com/page") == "www.example.com"

    def test_empty(self):
        assert extract_domain("") == ""


# ─── RATE LIMITER TESTS ────────────────────────────────────────


class TestRateLimiter:
    """Test the token bucket rate limiter."""

    def test_domain_bucket_acquire(self):
        from sentinel.stealth.rate_limiter import DomainBucket

        bucket = DomainBucket(rate_per_minute=10.0)
        # Should be able to acquire initial burst
        assert bucket.try_acquire() is True

    def test_domain_bucket_exhaust(self):
        from sentinel.stealth.rate_limiter import DomainBucket

        bucket = DomainBucket(rate_per_minute=1.0, burst_multiplier=1.0)
        assert bucket.try_acquire() is True
        assert bucket.try_acquire() is False

    def test_domain_bucket_429_handling(self):
        from sentinel.stealth.rate_limiter import DomainBucket

        bucket = DomainBucket(rate_per_minute=20.0)
        original_rate = bucket.rate_per_minute
        bucket.report_429()
        assert bucket.rate_per_minute == original_rate / 2


# ─── DOM PRUNING TESTS ─────────────────────────────────────────


class TestDOMPruning:
    """Test HTML cleaning and DOM pruning."""

    def test_remove_script(self):
        from sentinel.extraction.html_cleaner import clean
        html = "<html><body><p>Hello world</p><script>alert('x')</script></body></html>"
        result = clean(html)
        assert "alert" not in result
        assert "Hello world" in result

    def test_remove_nav(self):
        from sentinel.extraction.html_cleaner import clean
        html = "<html><body><nav>Menu items</nav><article><p>Important content here for testing</p></article></body></html>"
        result = clean(html)
        assert "Menu items" not in result
        assert "Important content here for testing" in result

    def test_remove_footer(self):
        from sentinel.extraction.html_cleaner import clean
        html = "<html><body><article><p>Main content paragraph one</p></article><footer>Copyright 2024</footer></body></html>"
        result = clean(html)
        assert "Copyright" not in result

    def test_truncation(self):
        from sentinel.extraction.html_cleaner import clean
        html = "<html><body><p>" + "A" * 100000 + "</p></body></html>"
        result = clean(html, max_text_length=1000)
        assert len(result) <= 1000

    def test_remove_ads_by_class(self):
        from sentinel.extraction.html_cleaner import clean
        html = '<html><body><div class="ad-banner">Buy now!</div><p>Real content for extraction testing</p></body></html>'
        result = clean(html)
        assert "Buy now" not in result

    def test_preserve_article(self):
        from sentinel.extraction.html_cleaner import clean
        html = "<html><body><article><p>Article content that should be preserved and extracted</p></article></body></html>"
        result = clean(html)
        assert "Article content" in result


# ─── DEDUPLICATION TESTS ───────────────────────────────────────


class TestDeduplication:
    """Test the 3-stage dedup pipeline."""

    def test_exact_hash_duplicate(self):
        from sentinel.extraction.deduplicator import Deduplicator
        dedup = Deduplicator()
        assert dedup.is_exact_duplicate("abc123") is False
        assert dedup.is_exact_duplicate("abc123") is True

    def test_exact_hash_unique(self):
        from sentinel.extraction.deduplicator import Deduplicator
        dedup = Deduplicator()
        assert dedup.is_exact_duplicate("hash1") is False
        assert dedup.is_exact_duplicate("hash2") is False


# ─── CONFIG TESTS ──────────────────────────────────────────────


class TestConfig:
    """Test configuration loading."""

    def test_default_config(self):
        from sentinel.config import SentinelConfig
        config = SentinelConfig()
        assert config.system.name == "sentinel"
        assert config.redis.url == "redis://localhost:6379/0"
        assert config.lancedb.embedding_dim == 384

    def test_config_validation(self):
        from sentinel.config import SentinelConfig, SystemConfig
        config = SentinelConfig()
        assert config.system.log_level in ("DEBUG", "INFO", "WARNING", "ERROR")


# ─── MODELS TESTS ──────────────────────────────────────────────


class TestModels:
    """Test Pydantic data models."""

    def test_source_model(self):
        from sentinel.models import Source, SourceType
        source = Source(name="test", source_type=SourceType.HACKERNEWS)
        assert source.name == "test"
        assert source.enabled is True
        assert source.priority == 1.0

    def test_crawl_job_defaults(self):
        from sentinel.models import CrawlJob, CrawlStatus
        job = CrawlJob(url="https://example.com")
        assert job.status == CrawlStatus.PENDING
        assert job.depth == 0
        assert job.attempt_count == 0

    def test_extracted_content(self):
        from sentinel.models import ExtractedContent, SourceType, ContentType
        from uuid import uuid4
        content = ExtractedContent(
            crawl_job_id=uuid4(),
            url="https://example.com",
            source_type=SourceType.HACKERNEWS,
            content_type=ContentType.ARTICLE,
            title="Test",
            full_text="Test content",
        )
        assert content.relevance_score == 0.0
        assert content.novelty_score == 0.0

    def test_signal_model(self):
        from sentinel.models import Signal, SignalType
        signal = Signal(
            signal_type=SignalType.ANOMALY,
            title="Test Signal",
            description="A test signal.",
        )
        assert signal.confidence == 0.5
        assert signal.acknowledged is False
