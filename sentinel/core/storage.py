"""
SENTINEL tiered storage manager.
Gzip-compressed HTML storage organized by domain.
"""
from __future__ import annotations

import gzip
import hashlib
from pathlib import Path
from typing import Optional

import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)


class StorageManager:
    """Tiered storage for raw HTML and cached content."""

    def __init__(self) -> None:
        """Initialize storage manager."""
        config = get_config()
        self._cache_dir = Path(config.system.data_dir) / "cache" / "html"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, url: str) -> Path:
        """
        Generate storage path for a URL.

        Path format: data/cache/html/{domain}/{hash}.html.gz

        Args:
            url: The URL to generate a path for.

        Returns:
            Path object for the storage file.
        """
        from urllib.parse import urlparse

        parsed = urlparse(url)
        domain = parsed.netloc.replace(":", "_")
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]

        domain_dir = self._cache_dir / domain
        domain_dir.mkdir(parents=True, exist_ok=True)

        return domain_dir / f"{url_hash}.html.gz"

    def store_raw_html(self, url: str, content: str | bytes) -> str:
        """
        Store raw HTML content, gzip compressed.

        Args:
            url: Source URL.
            content: Raw HTML content.

        Returns:
            Storage path as string.
        """
        path = self._get_path(url)
        try:
            if isinstance(content, str):
                content = content.encode("utf-8")

            with gzip.open(path, "wb") as f:
                f.write(content)

            logger.debug(
                "html_stored",
                url=url[:100],
                path=str(path),
                size_bytes=len(content),
                compressed_bytes=path.stat().st_size,
            )
            return str(path)
        except Exception as e:
            logger.error("html_store_failed", url=url[:100], error=str(e))
            raise

    def get_raw_html(self, path: str) -> Optional[str]:
        """
        Retrieve stored raw HTML content.

        Args:
            path: Storage path (as returned by store_raw_html).

        Returns:
            Decompressed HTML content, or None if not found.
        """
        file_path = Path(path)
        if not file_path.exists():
            logger.warning("html_not_found", path=path)
            return None

        try:
            with gzip.open(file_path, "rb") as f:
                return f.read().decode("utf-8")
        except Exception as e:
            logger.error("html_read_failed", path=path, error=str(e))
            return None

    def calculate_disk_usage(self) -> float:
        """
        Calculate total disk usage of the HTML cache in GB.

        Returns:
            Disk usage in gigabytes.
        """
        total_bytes = 0
        try:
            for path in self._cache_dir.rglob("*.html.gz"):
                total_bytes += path.stat().st_size
        except Exception as e:
            logger.error("disk_usage_calc_failed", error=str(e))

        return total_bytes / (1024 ** 3)

    def cleanup_old_files(self, max_age_days: int = 30) -> int:
        """
        Remove cached files older than max_age_days.

        Args:
            max_age_days: Maximum age in days.

        Returns:
            Number of files removed.
        """
        import time

        cutoff = time.time() - (max_age_days * 86400)
        removed = 0

        try:
            for path in self._cache_dir.rglob("*.html.gz"):
                if path.stat().st_mtime < cutoff:
                    path.unlink()
                    removed += 1
        except Exception as e:
            logger.error("cleanup_failed", error=str(e))

        if removed:
            logger.info("cache_cleanup_completed", files_removed=removed)
        return removed
