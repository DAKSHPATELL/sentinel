"""
SENTINEL SQLite client.
Async SQLite for URL frontier, domain stats, and crawl log.
Uses aiosqlite with WAL mode for concurrent reads.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import aiosqlite
import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)

# ─── SCHEMA DEFINITIONS (PRD Section 4.3.1) ────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS frontier (
    url TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    priority REAL DEFAULT 1.0,
    depth INTEGER DEFAULT 0,
    parent_url TEXT,
    source_id TEXT,
    status TEXT DEFAULT 'pending',
    attempt_count INTEGER DEFAULT 0,
    content_hash TEXT,
    last_crawled_at TEXT,
    next_crawl_at TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier(priority DESC);
CREATE INDEX IF NOT EXISTS idx_frontier_next_crawl ON frontier(next_crawl_at);

CREATE TABLE IF NOT EXISTS domain_stats (
    domain TEXT PRIMARY KEY,
    total_urls INTEGER DEFAULT 0,
    successful_crawls INTEGER DEFAULT 0,
    failed_crawls INTEGER DEFAULT 0,
    blocked_count INTEGER DEFAULT 0,
    avg_response_time_ms REAL DEFAULT 0.0,
    last_crawled_at TEXT,
    crawl_delay_seconds REAL DEFAULT 2.0,
    robots_txt TEXT,
    robots_fetched_at TEXT,
    is_blocked BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS crawl_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    status_code INTEGER,
    content_type TEXT,
    response_time_ms INTEGER,
    proxy_used TEXT,
    stealth_profile TEXT,
    blocked BOOLEAN DEFAULT FALSE,
    error TEXT,
    crawled_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_crawl_log_url ON crawl_log(url);
CREATE INDEX IF NOT EXISTS idx_crawl_log_time ON crawl_log(crawled_at);
"""


class SQLiteClient:
    """Async SQLite client for crawl state management."""

    def __init__(self) -> None:
        """Initialize SQLite client (call connect() to open database)."""
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Open SQLite database and create tables if needed."""
        config = get_config()
        db_path = Path(config.sqlite.crawl_state_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._db = await aiosqlite.connect(str(db_path))
        self._db.row_factory = aiosqlite.Row

        # Enable WAL mode for concurrent reads
        if config.sqlite.wal_mode:
            await self._db.execute("PRAGMA journal_mode=WAL")
            await self._db.execute("PRAGMA synchronous=NORMAL")

        # Create tables
        await self._db.executescript(SCHEMA_SQL)
        await self._db.commit()

        logger.info("sqlite_connected", path=str(db_path))

    @property
    def db(self) -> aiosqlite.Connection:
        """Get the database connection."""
        if self._db is None:
            raise RuntimeError("SQLite not connected. Call connect() first.")
        return self._db

    async def close(self) -> None:
        """Close the database connection."""
        if self._db:
            await self._db.close()
            self._db = None
            logger.info("sqlite_disconnected")

    # ─── GENERIC OPERATIONS ─────────────────────────────────────

    async def execute(self, sql: str, params: tuple = ()) -> None:
        """Execute a SQL statement."""
        await self.db.execute(sql, params)
        await self.db.commit()

    async def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        cursor = await self.db.execute(sql, params)
        rows = await cursor.fetchall()
        if not rows:
            return []
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    # ─── FRONTIER OPERATIONS ────────────────────────────────────

    async def add_to_frontier(
        self,
        url: str,
        domain: str,
        priority: float = 1.0,
        depth: int = 0,
        parent_url: Optional[str] = None,
        source_id: Optional[str] = None,
    ) -> bool:
        """
        Add a URL to the frontier.

        Args:
            url: Normalized URL to add.
            domain: Domain of the URL.
            priority: Crawl priority.
            depth: Depth from seed URL.
            parent_url: URL that linked to this one.
            source_id: Source that discovered this URL.

        Returns:
            True if added, False if already exists.
        """
        try:
            await self.db.execute(
                """INSERT INTO frontier (url, domain, priority, depth, parent_url, source_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (url, domain, priority, depth, parent_url, source_id),
            )
            await self.db.commit()

            # Update domain stats
            await self.db.execute(
                """INSERT INTO domain_stats (domain, total_urls) VALUES (?, 1)
                   ON CONFLICT(domain) DO UPDATE SET total_urls = total_urls + 1""",
                (domain,),
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def get_next_urls(self, n: int = 10) -> list[dict[str, Any]]:
        """
        Get the next N highest-priority pending URLs.

        Only returns URLs where:
        - status = 'pending'
        - next_crawl_at is NULL or <= now
        - domain is not blocked

        Args:
            n: Number of URLs to return.

        Returns:
            List of frontier rows as dicts.
        """
        return await self.query(
            """SELECT f.url, f.domain, f.priority, f.depth, f.parent_url, f.source_id, f.attempt_count
               FROM frontier f
               LEFT JOIN domain_stats ds ON f.domain = ds.domain
               WHERE f.status = 'pending'
                 AND (f.next_crawl_at IS NULL OR f.next_crawl_at <= datetime('now'))
                 AND (ds.is_blocked IS NULL OR ds.is_blocked = FALSE)
               ORDER BY f.priority DESC
               LIMIT ?""",
            (n,),
        )

    async def update_status(
        self,
        url: str,
        status: str,
        content_hash: Optional[str] = None,
        error_message: Optional[str] = None,
        next_crawl_at: Optional[str] = None,
    ) -> None:
        """
        Update the status of a URL in the frontier.

        Args:
            url: URL to update.
            status: New status.
            content_hash: SHA-256 of crawled content.
            error_message: Error message if failed.
            next_crawl_at: ISO 8601 datetime for next crawl.
        """
        await self.db.execute(
            """UPDATE frontier SET
                 status = ?,
                 content_hash = COALESCE(?, content_hash),
                 error_message = ?,
                 last_crawled_at = datetime('now'),
                 next_crawl_at = ?,
                 attempt_count = CASE WHEN ? = 'failed' THEN attempt_count + 1 ELSE attempt_count END
               WHERE url = ?""",
            (status, content_hash, error_message, next_crawl_at, status, url),
        )
        await self.db.commit()

    async def get_frontier_stats(self) -> dict[str, int]:
        """Get frontier statistics."""
        result = await self.query(
            """SELECT
                 COUNT(*) as total,
                 SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                 SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                 SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                 SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                 SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) as blocked,
                 COUNT(DISTINCT domain) as unique_domains
               FROM frontier"""
        )
        return result[0] if result else {}

    # ─── DOMAIN STATS OPERATIONS ────────────────────────────────

    async def get_domain_stats(self, domain: str) -> Optional[dict[str, Any]]:
        """Get stats for a specific domain."""
        results = await self.query("SELECT * FROM domain_stats WHERE domain = ?", (domain,))
        return results[0] if results else None

    async def update_domain_stats(
        self,
        domain: str,
        **kwargs: Any,
    ) -> None:
        """Update domain statistics."""
        set_clauses = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [domain]
        await self.db.execute(
            f"UPDATE domain_stats SET {set_clauses} WHERE domain = ?",
            tuple(values),
        )
        await self.db.commit()

    async def update_domain_robots(
        self, domain: str, robots_txt: str
    ) -> None:
        """Update cached robots.txt for a domain."""
        await self.db.execute(
            """INSERT INTO domain_stats (domain, robots_txt, robots_fetched_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(domain) DO UPDATE SET
                 robots_txt = ?, robots_fetched_at = datetime('now')""",
            (domain, robots_txt, robots_txt),
        )
        await self.db.commit()

    # ─── CRAWL LOG OPERATIONS ───────────────────────────────────

    async def log_crawl(
        self,
        url: str,
        status_code: Optional[int] = None,
        content_type: Optional[str] = None,
        response_time_ms: Optional[int] = None,
        proxy_used: Optional[str] = None,
        blocked: bool = False,
        error: Optional[str] = None,
    ) -> None:
        """Log a crawl attempt."""
        await self.db.execute(
            """INSERT INTO crawl_log (url, status_code, content_type, response_time_ms, proxy_used, blocked, error)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (url, status_code, content_type, response_time_ms, proxy_used, blocked, error),
        )
        await self.db.commit()
