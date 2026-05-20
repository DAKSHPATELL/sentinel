"""
SENTINEL Burst Detector.
Implements Kleinberg's burst detection algorithm adapted for streaming data.
Detects sudden transitions from baseline to elevated mention rates.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime
from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient
from sentinel.models import AlertPriority, Signal, SignalType

logger = structlog.get_logger(__name__)


class BurstDetector:
    """
    Detects bursts in entity/topic mention streams using a two-state automaton.

    The model has two states:
      - State 0 (baseline): Normal mention rate
      - State 1 (burst): Elevated mention rate

    Transition cost from state 0→1 is controlled by gamma (higher = harder to trigger).
    The burst rate is s * baseline_rate (s > 1, controls how much higher burst must be).

    This is a simplified online version of Kleinberg's infinite automaton,
    reduced to 2 states for efficiency on streaming data.
    """

    def __init__(self, duckdb: DuckDBClient) -> None:
        self._duckdb = duckdb
        self._config = get_config().signals
        # In-memory state tracking per entity
        self._entity_states: dict[str, int] = {}  # 0=baseline, 1=burst
        self._burst_start: dict[str, datetime] = {}

    def initialize(self) -> None:
        """Create burst tracking tables."""
        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS burst_log (
                burst_id VARCHAR PRIMARY KEY,
                entity_name VARCHAR NOT NULL,
                burst_start TIMESTAMP NOT NULL,
                burst_end TIMESTAMP,
                peak_rate DOUBLE NOT NULL,
                baseline_rate DOUBLE NOT NULL,
                burst_strength DOUBLE NOT NULL,
                total_mentions INTEGER DEFAULT 0,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def detect(self) -> list[Signal]:
        """
        Run burst detection on recent entity time series.

        Returns list of burst signals.
        """
        s = self._config.burst_detection_s  # Burst multiplier (default 2.0)
        gamma = self._config.burst_detection_gamma  # Transition cost (default 1.0)
        signals: list[Signal] = []

        try:
            # Get entities with recent activity (last 72 hours)
            entities = self._duckdb.query("""
                SELECT entity_name,
                       LIST(mention_count ORDER BY hour_bucket ASC) as counts,
                       LIST(hour_bucket ORDER BY hour_bucket ASC) as hours
                FROM entity_mention_ts
                WHERE hour_bucket >= CURRENT_TIMESTAMP - INTERVAL '72 hours'
                GROUP BY entity_name
                HAVING COUNT(*) >= 6
            """)
        except Exception as e:
            logger.error("burst_query_failed", error=str(e))
            return signals

        for row in entities:
            entity = row["entity_name"]
            counts = row["counts"]
            signal = self._detect_burst(entity, counts, s, gamma)
            if signal:
                signals.append(signal)

        if signals:
            logger.info("bursts_detected", count=len(signals))

        return signals

    def _detect_burst(
        self,
        entity: str,
        counts: list[int],
        s: float,
        gamma: float,
    ) -> Optional[Signal]:
        """
        Apply two-state burst model to a single entity's time series.

        The key insight: we compute the log-likelihood ratio of observing
        each count under burst-rate vs baseline-rate, minus the transition cost.
        If the cumulative ratio exceeds 0, we're in a burst.
        """
        if not counts or len(counts) < 3:
            return None

        total = sum(counts)
        n = len(counts)
        baseline_rate = total / n

        if baseline_rate < 0.5:
            return None  # Too few mentions to detect meaningful burst

        burst_rate = s * baseline_rate
        transition_cost = gamma * math.log(n)

        # Current state
        prev_state = self._entity_states.get(entity, 0)
        current_state = prev_state

        # Check latest window (last 3 hours)
        recent = counts[-3:]
        recent_rate = sum(recent) / len(recent)

        # Log-likelihood ratio for recent window
        llr = 0.0
        for x in recent:
            if x == 0:
                continue
            # Poisson log-likelihood ratio: x*log(burst/base) - (burst - base)
            if baseline_rate > 0 and burst_rate > 0:
                llr += x * math.log(burst_rate / max(baseline_rate, 0.01)) - (burst_rate - baseline_rate)

        # State transition logic
        if prev_state == 0 and llr > transition_cost:
            # Transition to burst state
            current_state = 1
            self._entity_states[entity] = 1
            self._burst_start[entity] = datetime.utcnow()
        elif prev_state == 1 and llr < 0:
            # Transition back to baseline
            current_state = 0
            self._entity_states[entity] = 0
            self._burst_start.pop(entity, None)
            return None  # Burst ended, no new signal

        if current_state == 0:
            return None

        # Only emit signal on burst start (not continuation)
        if prev_state == 1:
            return None  # Already in burst, don't re-signal

        burst_strength = recent_rate / max(baseline_rate, 0.01)
        peak = max(recent)

        # Priority by burst strength
        if burst_strength > s * 3:
            priority = AlertPriority.CRITICAL
        elif burst_strength > s * 2:
            priority = AlertPriority.HIGH
        elif burst_strength > s:
            priority = AlertPriority.MEDIUM
        else:
            priority = AlertPriority.LOW

        # Log to DB
        import uuid
        try:
            self._duckdb.execute(
                """
                INSERT INTO burst_log (burst_id, entity_name, burst_start, peak_rate, baseline_rate, burst_strength, total_mentions)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), entity, datetime.utcnow(), recent_rate, baseline_rate, burst_strength, sum(recent)),
            )
        except Exception:
            pass

        return Signal(
            signal_type=SignalType.BURST,
            priority=priority,
            title=f"Burst: {entity} mentions surged {burst_strength:.1f}x above baseline",
            description=(
                f"{entity} is experiencing a burst: {recent_rate:.1f} mentions/hour "
                f"vs baseline of {baseline_rate:.1f}/hour ({burst_strength:.1f}x increase). "
                f"Peak: {peak} mentions in a single hour."
            ),
            entities=[entity],
            confidence=min(1.0, burst_strength / (s * 4)),
            metadata={
                "burst_strength": round(burst_strength, 2),
                "recent_rate": round(recent_rate, 2),
                "baseline_rate": round(baseline_rate, 2),
                "peak_count": peak,
                "window_hours": n,
            },
        )
