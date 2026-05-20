"""
SENTINEL Anomaly Detector.
Statistical anomaly detection on time-series entity mention counts.
Uses Z-score over rolling windows with adaptive thresholds.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient
from sentinel.models import AlertPriority, Signal, SignalType

logger = structlog.get_logger(__name__)


class AnomalyDetector:
    """
    Detects statistical anomalies in entity mention frequency.

    Approach: For each entity, maintain an hourly mention count time series.
    Compute a rolling mean and std dev over a configurable window.
    Flag any hour where the count exceeds mean + (threshold_sigma * std).

    This catches things like: "Entity X is normally mentioned 5 times/hour
    but suddenly appeared 40 times in the last hour."
    """

    def __init__(self, duckdb: DuckDBClient) -> None:
        self._duckdb = duckdb
        self._config = get_config().signals

    def initialize(self) -> None:
        """Create the time-series tables."""
        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS entity_mention_ts (
                entity_name VARCHAR NOT NULL,
                hour_bucket TIMESTAMP NOT NULL,
                mention_count INTEGER DEFAULT 0,
                source_types VARCHAR DEFAULT '[]',
                avg_relevance DOUBLE DEFAULT 0.0,
                PRIMARY KEY (entity_name, hour_bucket)
            )
        """)
        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_log (
                anomaly_id VARCHAR PRIMARY KEY,
                entity_name VARCHAR NOT NULL,
                hour_bucket TIMESTAMP NOT NULL,
                observed_count INTEGER NOT NULL,
                expected_mean DOUBLE NOT NULL,
                expected_std DOUBLE NOT NULL,
                z_score DOUBLE NOT NULL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def record_mentions(
        self,
        entity_name: str,
        count: int,
        source_types: list[str],
        relevance: float = 0.0,
    ) -> None:
        """Record entity mentions for the current hour bucket."""
        now = datetime.utcnow()
        hour_bucket = now.replace(minute=0, second=0, microsecond=0)

        try:
            self._duckdb.execute(
                """
                INSERT INTO entity_mention_ts (entity_name, hour_bucket, mention_count, source_types, avg_relevance)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (entity_name, hour_bucket)
                DO UPDATE SET
                    mention_count = entity_mention_ts.mention_count + excluded.mention_count,
                    avg_relevance = (entity_mention_ts.avg_relevance + excluded.avg_relevance) / 2
                """,
                (entity_name, hour_bucket, count, str(source_types), relevance),
            )
        except Exception as e:
            logger.error("record_mentions_failed", entity=entity_name, error=str(e))

    def detect(self) -> list[Signal]:
        """
        Scan all entities for anomalous mention frequency.

        Returns list of Signal objects for detected anomalies.
        """
        window = self._config.anomaly_window_size
        threshold = self._config.anomaly_threshold_sigma
        min_points = self._config.time_series.min_data_points
        signals: list[Signal] = []

        try:
            # Get entities with enough data points
            entities = self._duckdb.query(f"""
                SELECT entity_name, COUNT(*) as points
                FROM entity_mention_ts
                WHERE hour_bucket >= CURRENT_TIMESTAMP - INTERVAL '{window * 3} hours'
                GROUP BY entity_name
                HAVING COUNT(*) >= {min_points}
            """)
        except Exception as e:
            logger.error("anomaly_scan_query_failed", error=str(e))
            return signals

        for row in entities:
            entity = row["entity_name"]
            signal = self._check_entity(entity, window, threshold)
            if signal:
                signals.append(signal)

        if signals:
            logger.info("anomalies_detected", count=len(signals))

        return signals

    def _check_entity(
        self, entity: str, window: int, threshold: float
    ) -> Optional[Signal]:
        """Check a single entity for anomaly in the latest hour."""
        try:
            rows = self._duckdb.query(
                """
                SELECT hour_bucket, mention_count
                FROM entity_mention_ts
                WHERE entity_name = ?
                ORDER BY hour_bucket DESC
                LIMIT ?
                """,
                (entity, window + 1),
            )
        except Exception:
            return None

        if len(rows) < 3:
            return None

        # Latest observation vs historical window
        latest_count = rows[0]["mention_count"]
        historical = [r["mention_count"] for r in rows[1:]]

        mean = sum(historical) / len(historical)
        variance = sum((x - mean) ** 2 for x in historical) / len(historical)
        std = variance**0.5

        if std < 0.1:
            # Flat time series — use absolute threshold
            if latest_count <= mean + 3:
                return None
            z_score = (latest_count - mean) / 0.1
        else:
            z_score = (latest_count - mean) / std

        if z_score < threshold:
            return None

        # Determine priority by severity
        if z_score > threshold * 2:
            priority = AlertPriority.CRITICAL
        elif z_score > threshold * 1.5:
            priority = AlertPriority.HIGH
        else:
            priority = AlertPriority.MEDIUM

        # Log the anomaly
        import uuid
        anomaly_id = str(uuid.uuid4())
        try:
            self._duckdb.execute(
                """
                INSERT INTO anomaly_log (anomaly_id, entity_name, hour_bucket, observed_count, expected_mean, expected_std, z_score)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (anomaly_id, entity, rows[0]["hour_bucket"], latest_count, mean, std, z_score),
            )
        except Exception:
            pass

        return Signal(
            signal_type=SignalType.ANOMALY,
            priority=priority,
            title=f"Anomaly: {entity} mentions surged {z_score:.1f}σ above normal",
            description=(
                f"{entity} was mentioned {latest_count} times in the last hour, "
                f"compared to a baseline of {mean:.1f} ± {std:.1f}. "
                f"Z-score: {z_score:.2f} (threshold: {threshold})"
            ),
            entities=[entity],
            confidence=min(1.0, z_score / (threshold * 3)),
            metadata={
                "z_score": round(z_score, 3),
                "observed": latest_count,
                "expected_mean": round(mean, 2),
                "expected_std": round(std, 2),
                "window_hours": len(historical),
            },
        )
