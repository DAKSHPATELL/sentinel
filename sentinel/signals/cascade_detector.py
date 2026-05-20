"""
SENTINEL Cascade Detector.
Detects cross-domain information cascades — the same entity appearing
across multiple independent source types within a time window.

This is SENTINEL's most valuable signal type: it catches patterns
like "OpenAI mentioned on arXiv → HackerNews → SEC filing → patent"
which indicate real institutional-grade intelligence.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient
from sentinel.models import (
    AlertPriority,
    CascadeEvent,
    CascadePattern,
    Signal,
    SignalType,
    SourceType,
)

logger = structlog.get_logger(__name__)

# Source types ordered by "institutional weight" — SEC filings and patents
# carry more weight than social media posts
SOURCE_WEIGHT = {
    SourceType.SEC_FILING.value: 3.0,
    SourceType.PATENT.value: 2.5,
    SourceType.ARXIV.value: 2.0,
    SourceType.CRUNCHBASE.value: 2.0,
    SourceType.GITHUB.value: 1.5,
    SourceType.HACKERNEWS.value: 1.2,
    SourceType.RSS.value: 1.0,
    SourceType.REDDIT.value: 0.8,
    SourceType.TWITTER.value: 0.7,
    SourceType.PRODUCT_HUNT.value: 1.0,
    SourceType.WEB_CRAWL.value: 0.6,
    SourceType.CT_MONITOR.value: 0.5,
}


class CascadeDetector:
    """
    Detects when the same entity propagates across multiple source types.

    A cascade is defined as: entity E appears in source S1 at time T1,
    then in S2 at T2, then in S3 at T3, where:
      - S1, S2, S3 are different SourceTypes
      - T3 - T1 < cascade_max_window_hours
      - len({S1, S2, S3}) >= cascade_min_sources

    The strength of a cascade depends on the diversity and weight of sources.
    """

    def __init__(self, duckdb: DuckDBClient) -> None:
        self._duckdb = duckdb
        self._config = get_config().signals
        self._emitted_cascades: set[str] = set()  # Dedup within detection cycle

    def initialize(self) -> None:
        """Create cascade tracking tables."""
        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS entity_appearances (
                entity_name VARCHAR NOT NULL,
                source_type VARCHAR NOT NULL,
                url VARCHAR,
                title VARCHAR,
                summary VARCHAR,
                appeared_at TIMESTAMP NOT NULL,
                relevance DOUBLE DEFAULT 0.0
            )
        """)
        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS cascade_log (
                cascade_id VARCHAR PRIMARY KEY,
                entity_name VARCHAR NOT NULL,
                source_count INTEGER NOT NULL,
                source_types VARCHAR NOT NULL,
                span_hours DOUBLE NOT NULL,
                weighted_score DOUBLE NOT NULL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def record_appearance(
        self,
        entity_name: str,
        source_type: str,
        url: str = "",
        title: str = "",
        summary: str = "",
        relevance: float = 0.0,
    ) -> None:
        """Record an entity appearing in a specific source."""
        try:
            self._duckdb.execute(
                """
                INSERT INTO entity_appearances (entity_name, source_type, url, title, summary, appeared_at, relevance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (entity_name, source_type, url, title, summary, datetime.utcnow(), relevance),
            )
        except Exception as e:
            logger.error("record_appearance_failed", entity=entity_name, error=str(e))

    def detect(self) -> list[Signal]:
        """
        Scan for cross-domain cascades.

        Returns list of cascade signals.
        """
        min_sources = self._config.cascade_min_sources
        window_hours = self._config.cascade_max_window_hours
        signals: list[Signal] = []

        try:
            # Find entities appearing in multiple source types recently
            candidates = self._duckdb.query(f"""
                SELECT entity_name,
                       COUNT(DISTINCT source_type) as source_count,
                       LIST(DISTINCT source_type) as source_types,
                       MIN(appeared_at) as first_seen,
                       MAX(appeared_at) as last_seen
                FROM entity_appearances
                WHERE appeared_at >= CURRENT_TIMESTAMP - INTERVAL '{window_hours} hours'
                GROUP BY entity_name
                HAVING COUNT(DISTINCT source_type) >= {min_sources}
                ORDER BY COUNT(DISTINCT source_type) DESC
                LIMIT 100
            """)
        except Exception as e:
            logger.error("cascade_scan_failed", error=str(e))
            return signals

        for row in candidates:
            entity = row["entity_name"]
            # Dedup within cycle
            if entity in self._emitted_cascades:
                continue

            signal = self._build_cascade_signal(
                entity=entity,
                source_types=row["source_types"],
                source_count=row["source_count"],
                first_seen=row["first_seen"],
                last_seen=row["last_seen"],
                window_hours=window_hours,
            )
            if signal:
                signals.append(signal)
                self._emitted_cascades.add(entity)

        if signals:
            logger.info("cascades_detected", count=len(signals))

        # Reset dedup set periodically (every detection cycle)
        if len(self._emitted_cascades) > 1000:
            self._emitted_cascades.clear()

        return signals

    def _build_cascade_signal(
        self,
        entity: str,
        source_types: list[str],
        source_count: int,
        first_seen: datetime,
        last_seen: datetime,
        window_hours: int,
    ) -> Optional[Signal]:
        """Build a cascade signal with weighted scoring."""
        span_hours = (last_seen - first_seen).total_seconds() / 3600.0

        # Calculate weighted score: sum of source weights
        weighted_score = sum(SOURCE_WEIGHT.get(st, 0.5) for st in source_types)

        # Higher source count + higher-weight sources = stronger signal
        # Normalize: 2 sources with weight 0.7 each = 1.4
        # 4 sources with SEC + patent + arxiv + HN = 3.0 + 2.5 + 2.0 + 1.2 = 8.7
        confidence = min(1.0, weighted_score / 6.0)

        if weighted_score < 2.0:
            priority = AlertPriority.LOW
        elif weighted_score < 4.0:
            priority = AlertPriority.MEDIUM
        elif weighted_score < 6.0:
            priority = AlertPriority.HIGH
        else:
            priority = AlertPriority.CRITICAL

        # Fetch the actual events for this cascade
        try:
            events = self._duckdb.query(
                """
                SELECT source_type, url, title, appeared_at
                FROM entity_appearances
                WHERE entity_name = ?
                ORDER BY appeared_at ASC
                LIMIT 20
                """,
                (entity,),
            )
        except Exception:
            events = []

        event_timeline = " → ".join(
            f"{e['source_type']}({e.get('title', '')[:30]})" for e in events[:6]
        )

        # Log cascade
        import uuid
        cascade_id = str(uuid.uuid4())
        try:
            self._duckdb.execute(
                """
                INSERT INTO cascade_log (cascade_id, entity_name, source_count, source_types, span_hours, weighted_score)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (cascade_id, entity, source_count, str(source_types), span_hours, weighted_score),
            )
        except Exception:
            pass

        return Signal(
            signal_type=SignalType.CASCADE,
            priority=priority,
            title=f"Cascade: {entity} propagating across {source_count} sources",
            description=(
                f"{entity} detected across {source_count} independent source types "
                f"within {span_hours:.1f} hours. Sources: {', '.join(source_types)}. "
                f"Weighted score: {weighted_score:.1f}. "
                f"Timeline: {event_timeline}"
            ),
            entities=[entity],
            source_types=[SourceType(st) for st in source_types if st in SourceType._value2member_map_],
            confidence=confidence,
            evidence_urls=[e.get("url", "") for e in events if e.get("url")],
            metadata={
                "source_count": source_count,
                "source_types": source_types,
                "span_hours": round(span_hours, 2),
                "weighted_score": round(weighted_score, 2),
                "cascade_id": cascade_id,
            },
        )
