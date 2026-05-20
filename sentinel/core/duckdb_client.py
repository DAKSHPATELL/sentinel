"""
SENTINEL DuckDB client.
Persistent DuckDB for time-series, signal log, experiments, and source metrics.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import duckdb
import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)

# ─── SCHEMA DEFINITIONS (PRD Section 4.3.3) ────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entity_timeseries (
    entity_name VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    mention_count INTEGER DEFAULT 0,
    sentiment_avg DOUBLE DEFAULT 0.0,
    relevance_avg DOUBLE DEFAULT 0.0,
    unique_urls INTEGER DEFAULT 0,
    PRIMARY KEY (entity_name, source_type, timestamp)
);

CREATE TABLE IF NOT EXISTS signal_log (
    signal_id VARCHAR PRIMARY KEY,
    signal_type VARCHAR NOT NULL,
    priority VARCHAR NOT NULL,
    title VARCHAR,
    description VARCHAR,
    entities VARCHAR,
    source_types VARCHAR,
    evidence_urls VARCHAR,
    confidence DOUBLE,
    metadata VARCHAR DEFAULT '{}',
    detected_at TIMESTAMP NOT NULL,
    acknowledged BOOLEAN DEFAULT FALSE,
    useful BOOLEAN
);

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id VARCHAR PRIMARY KEY,
    strategy_id VARCHAR NOT NULL,
    hypothesis VARCHAR,
    parameter_changes VARCHAR,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    baseline_score DOUBLE,
    result_score DOUBLE,
    improvement DOUBLE,
    accepted BOOLEAN
);

CREATE TABLE IF NOT EXISTS source_metrics (
    source_id VARCHAR NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    items_collected INTEGER DEFAULT 0,
    signals_generated INTEGER DEFAULT 0,
    useful_signals INTEGER DEFAULT 0,
    avg_relevance DOUBLE DEFAULT 0.0,
    avg_novelty DOUBLE DEFAULT 0.0,
    errors INTEGER DEFAULT 0,
    PRIMARY KEY (source_id, timestamp)
);
"""


class DuckDBClient:
    """Persistent DuckDB client for analytics and time-series data."""

    def __init__(self) -> None:
        """Initialize DuckDB client (call connect() to open database)."""
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self, read_only: bool = False) -> None:
        """
        Open DuckDB database.

        Args:
            read_only: If True, open in read-only mode (allows concurrent access
                       while another process holds the write lock).
        """
        config = get_config()
        db_path = Path(config.duckdb.path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = duckdb.connect(str(db_path), read_only=read_only)

        if not read_only:
            # Configure memory and threading
            self._conn.execute(f"SET memory_limit = '{config.duckdb.memory_limit}'")
            self._conn.execute(f"SET threads = {config.duckdb.threads}")

            # Create tables
            for stmt in SCHEMA_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    self._conn.execute(stmt)

            # Migrations for existing databases
            self._run_migrations()

        logger.info("duckdb_connected", path=str(db_path), read_only=read_only)

    def _run_migrations(self) -> None:
        """Apply schema migrations for existing databases."""
        try:
            # Check if signal_log.metadata column exists
            cols = [r[0] for r in self._conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='signal_log'"
            ).fetchall()]
            if "metadata" not in cols and cols:  # table exists but missing column
                self._conn.execute("ALTER TABLE signal_log ADD COLUMN metadata VARCHAR DEFAULT '{}'")
                logger.info("migration_applied", migration="signal_log_add_metadata")
        except Exception as e:
            logger.debug("migration_check_skipped", error=str(e))

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get the database connection."""
        if self._conn is None:
            raise RuntimeError("DuckDB not connected. Call connect() first.")
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("duckdb_disconnected")

    def execute(self, sql: str, params: Optional[list] = None) -> None:
        """Execute a SQL statement."""
        try:
            if params:
                self.conn.execute(sql, params)
            else:
                self.conn.execute(sql)
        except Exception as e:
            logger.error("duckdb_execute_failed", sql=sql[:100], error=str(e))
            raise

    def query(self, sql: str, params: Optional[list] = None) -> list[dict[str, Any]]:
        """
        Execute a query and return results as list of dicts.

        Args:
            sql: SQL query string.
            params: Optional query parameters.

        Returns:
            List of row dictionaries.
        """
        try:
            if params:
                result = self.conn.execute(sql, params)
            else:
                result = self.conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error("duckdb_query_failed", sql=sql[:100], error=str(e))
            return []
