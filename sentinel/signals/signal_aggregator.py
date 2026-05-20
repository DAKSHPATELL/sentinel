"""
SENTINEL Signal Aggregator.
Combines output from all detectors, deduplicates, prioritizes,
and emits signals to the event bus for downstream processing
(red team challenge, hypothesis court, alerts).
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient
from sentinel.events import EventBus, STREAM_SIGNALS
from sentinel.models import AlertPriority, Signal, SignalType
from sentinel.signals.anomaly_detector import AnomalyDetector
from sentinel.signals.burst_detector import BurstDetector
from sentinel.signals.cascade_detector import CascadeDetector

logger = structlog.get_logger(__name__)


class SignalAggregator:
    """
    Orchestrates all signal detectors and manages signal lifecycle.

    Detection cycle:
    1. Run anomaly detector → signals
    2. Run burst detector → signals
    3. Run cascade detector → signals
    4. Merge & deduplicate overlapping signals
    5. Persist to DuckDB signal_log
    6. Emit to Redis Streams for downstream processing
    """

    def __init__(
        self,
        duckdb: DuckDBClient,
        event_bus: Optional[EventBus] = None,
    ) -> None:
        self._duckdb = duckdb
        self._event_bus = event_bus
        self._config = get_config().signals

        self.anomaly_detector = AnomalyDetector(duckdb)
        self.burst_detector = BurstDetector(duckdb)
        self.cascade_detector = CascadeDetector(duckdb)

        self._recent_signal_keys: set[str] = set()

    def initialize(self) -> None:
        """Initialize all detectors and the signal log table."""
        self.anomaly_detector.initialize()
        self.burst_detector.initialize()
        self.cascade_detector.initialize()

        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS signal_log (
                signal_id VARCHAR PRIMARY KEY,
                signal_type VARCHAR NOT NULL,
                priority VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                description TEXT,
                entities VARCHAR DEFAULT '[]',
                source_types VARCHAR DEFAULT '[]',
                evidence_urls VARCHAR DEFAULT '[]',
                confidence DOUBLE DEFAULT 0.5,
                metadata VARCHAR DEFAULT '{}',
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                acknowledged BOOLEAN DEFAULT FALSE,
                useful BOOLEAN
            )
        """)

    async def run_detection_cycle(self) -> list[Signal]:
        """
        Run all detectors and aggregate results.

        Returns list of new, deduplicated signals.
        """
        all_signals: list[Signal] = []

        # Run detectors (these are synchronous — they query DuckDB)
        try:
            anomalies = self.anomaly_detector.detect()
            all_signals.extend(anomalies)
        except Exception as e:
            logger.error("anomaly_detection_failed", error=str(e))

        try:
            bursts = self.burst_detector.detect()
            all_signals.extend(bursts)
        except Exception as e:
            logger.error("burst_detection_failed", error=str(e))

        try:
            cascades = self.cascade_detector.detect()
            all_signals.extend(cascades)
        except Exception as e:
            logger.error("cascade_detection_failed", error=str(e))

        # Deduplicate: same entity + same signal type within recent memory
        unique_signals = self._deduplicate(all_signals)

        # Merge overlapping signals (boost priority when multiple detectors agree)
        merged = self._merge_overlapping(unique_signals)

        # Persist and emit
        for signal in merged:
            self._persist_signal(signal)
            if self._event_bus:
                try:
                    await self._event_bus.emit(
                        STREAM_SIGNALS,
                        {
                            "signal_id": str(signal.id),
                            "signal_type": signal.signal_type.value,
                            "priority": signal.priority.value,
                            "title": signal.title,
                            "entities": signal.entities,
                            "confidence": signal.confidence,
                        },
                    )
                except Exception as e:
                    logger.error("signal_emit_failed", signal_id=str(signal.id), error=str(e))

        logger.info(
            "detection_cycle_complete",
            anomalies=len(anomalies) if 'anomalies' in dir() else 0,
            bursts=len(bursts) if 'bursts' in dir() else 0,
            cascades=len(cascades) if 'cascades' in dir() else 0,
            emitted=len(merged),
        )

        return merged

    def _deduplicate(self, signals: list[Signal]) -> list[Signal]:
        """Remove duplicate signals based on entity + type key."""
        unique: list[Signal] = []
        for sig in signals:
            key = f"{sig.signal_type.value}:{','.join(sorted(sig.entities))}"
            if key not in self._recent_signal_keys:
                self._recent_signal_keys.add(key)
                unique.append(sig)

        # Trim memory
        if len(self._recent_signal_keys) > 5000:
            self._recent_signal_keys.clear()

        return unique

    def _merge_overlapping(self, signals: list[Signal]) -> list[Signal]:
        """
        When multiple detectors fire on the same entity, boost confidence.

        E.g., if "OpenAI" triggers both anomaly AND burst, merge into one
        signal with boosted confidence and higher priority.
        """
        entity_signals: dict[str, list[Signal]] = {}
        for sig in signals:
            for entity in sig.entities:
                entity_signals.setdefault(entity, []).append(sig)

        merged: list[Signal] = []
        seen_ids = set()

        for entity, sigs in entity_signals.items():
            if len(sigs) == 1:
                if str(sigs[0].id) not in seen_ids:
                    merged.append(sigs[0])
                    seen_ids.add(str(sigs[0].id))
                continue

            # Multiple signals for same entity — boost the strongest one
            sigs.sort(key=lambda s: s.confidence, reverse=True)
            best = sigs[0]
            if str(best.id) in seen_ids:
                continue

            # Boost confidence based on detector agreement
            agreement_bonus = min(0.2, 0.1 * (len(sigs) - 1))
            best.confidence = min(1.0, best.confidence + agreement_bonus)

            # Upgrade priority if multiple detectors agree
            if len(sigs) >= 3 and best.priority != AlertPriority.CRITICAL:
                best.priority = AlertPriority.CRITICAL
            elif len(sigs) >= 2 and best.priority == AlertPriority.MEDIUM:
                best.priority = AlertPriority.HIGH

            # Merge metadata
            detector_types = list(set(s.signal_type.value for s in sigs))
            best.metadata["corroborating_detectors"] = detector_types
            best.metadata["detector_agreement"] = len(sigs)

            merged.append(best)
            seen_ids.add(str(best.id))

            # Still emit the others as secondary signals
            for s in sigs[1:]:
                if str(s.id) not in seen_ids:
                    s.metadata["secondary_to"] = str(best.id)
                    merged.append(s)
                    seen_ids.add(str(s.id))

        return merged

    def _persist_signal(self, signal: Signal) -> None:
        """Save a signal to the DuckDB signal_log."""
        import json
        try:
            self._duckdb.execute(
                """
                INSERT INTO signal_log (
                    signal_id, signal_type, priority, title, description,
                    entities, source_types, evidence_urls, confidence, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(signal.id),
                    signal.signal_type.value,
                    signal.priority.value,
                    signal.title,
                    signal.description,
                    json.dumps(signal.entities),
                    json.dumps([st.value for st in signal.source_types]),
                    json.dumps(signal.evidence_urls),
                    signal.confidence,
                    json.dumps(signal.metadata),
                ),
            )
        except Exception as e:
            logger.error("persist_signal_failed", signal_id=str(signal.id), error=str(e))

    def get_recent_signals(self, limit: int = 50) -> list[dict]:
        """Fetch recent signals from the log."""
        try:
            return self._duckdb.query(
                """
                SELECT * FROM signal_log
                ORDER BY detected_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        except Exception:
            return []

    def update_signal_in_db(self, signal: Signal) -> None:
        """Update an existing signal's confidence, priority, and metadata in DuckDB."""
        import json
        try:
            self._duckdb.execute(
                """
                UPDATE signal_log
                SET confidence = ?, priority = ?, metadata = ?
                WHERE signal_id = ?
                """,
                (
                    signal.confidence,
                    signal.priority.value,
                    json.dumps(signal.metadata),
                    str(signal.id),
                ),
            )
            logger.info("signal_updated_in_db", signal_id=str(signal.id), confidence=signal.confidence)
        except Exception as e:
            logger.error("update_signal_failed", signal_id=str(signal.id), error=str(e))

    def mark_signal_useful(self, signal_id: str, useful: bool) -> None:
        """Mark a signal as useful/not useful (human feedback)."""
        try:
            self._duckdb.execute(
                "UPDATE signal_log SET useful = ?, acknowledged = TRUE WHERE signal_id = ?",
                (useful, signal_id),
            )
        except Exception as e:
            logger.error("mark_useful_failed", signal_id=signal_id, error=str(e))
