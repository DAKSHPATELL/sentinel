"""
SENTINEL Intelligence Report Generator.
Produces periodic intelligence digests in Markdown format.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient

logger = structlog.get_logger(__name__)


class ReportGenerator:
    """
    Generates daily/weekly intelligence reports from accumulated signals.

    Report structure:
    - Executive summary (top signals, confidence)
    - Anomalies detected
    - Burst events
    - Cross-domain cascades
    - Emerging entities
    - System health metrics
    """

    def __init__(self, duckdb: DuckDBClient) -> None:
        self._duckdb = duckdb
        self._config = get_config().interface.reports
        self._data_dir = Path(get_config().system.data_dir) / "reports"
        self._data_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, hours: int = 24) -> str:
        """
        Generate an intelligence report covering the last N hours.

        Returns Markdown string and saves to file.
        """
        now = datetime.utcnow()
        period_start = now - timedelta(hours=hours)

        sections = []
        sections.append(self._header(hours, period_start, now))
        sections.append(self._executive_summary(hours))
        sections.append(self._signal_breakdown(hours))
        sections.append(self._top_entities(hours))
        sections.append(self._cascade_section(hours))
        sections.append(self._system_health())
        sections.append(self._footer())

        report = "\n\n".join(sections)

        # Save to file
        filename = f"report_{now.strftime('%Y%m%d_%H%M')}.md"
        path = self._data_dir / filename
        path.write_text(report, encoding="utf-8")
        logger.info("report_generated", path=str(path), hours=hours)

        return report

    def _header(self, hours: int, start: datetime, end: datetime) -> str:
        return (
            f"# SENTINEL Intelligence Report\n"
            f"**Period:** {start.strftime('%Y-%m-%d %H:%M')} → {end.strftime('%Y-%m-%d %H:%M')} UTC ({hours}h)\n"
            f"**Generated:** {end.strftime('%Y-%m-%d %H:%M')} UTC"
        )

    def _executive_summary(self, hours: int) -> str:
        try:
            stats = self._duckdb.query(f"""
                SELECT
                    COUNT(*) as total_signals,
                    COUNT(*) FILTER (WHERE priority = 'critical') as critical,
                    COUNT(*) FILTER (WHERE priority = 'high') as high,
                    COUNT(*) FILTER (WHERE priority = 'medium') as medium,
                    COUNT(*) FILTER (WHERE priority = 'low') as low,
                    AVG(confidence) as avg_confidence
                FROM signal_log
                WHERE detected_at >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
            """)
            row = stats[0] if stats else {}
        except Exception:
            row = {}

        total = row.get("total_signals", 0)
        critical = row.get("critical", 0)
        high = row.get("high", 0)

        return (
            f"## Executive Summary\n\n"
            f"- **Total signals detected:** {total}\n"
            f"- **Critical:** {critical} | **High:** {high} | "
            f"**Medium:** {row.get('medium', 0)} | **Low:** {row.get('low', 0)}\n"
            f"- **Average confidence:** {row.get('avg_confidence', 0):.0%}"
        )

    def _signal_breakdown(self, hours: int) -> str:
        try:
            signals = self._duckdb.query(f"""
                SELECT signal_type, priority, title, entities, confidence, detected_at
                FROM signal_log
                WHERE detected_at >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
                  AND priority IN ('critical', 'high')
                ORDER BY
                    CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
                    confidence DESC
                LIMIT {self._config.max_signals_per_report}
            """)
        except Exception:
            signals = []

        if not signals:
            return "## High-Priority Signals\n\n*No high-priority signals in this period.*"

        lines = ["## High-Priority Signals\n"]
        for s in signals:
            lines.append(
                f"### [{s['priority'].upper()}] {s['title']}\n"
                f"- **Type:** {s['signal_type']} | **Confidence:** {s['confidence']:.0%}\n"
                f"- **Entities:** {s['entities']}\n"
                f"- **Detected:** {s['detected_at']}\n"
            )
        return "\n".join(lines)

    def _top_entities(self, hours: int) -> str:
        try:
            entities = self._duckdb.query(f"""
                SELECT entity_name, SUM(mention_count) as total_mentions,
                       COUNT(DISTINCT source_types) as source_diversity
                FROM entity_mention_ts
                WHERE hour_bucket >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
                GROUP BY entity_name
                ORDER BY total_mentions DESC
                LIMIT 20
            """)
        except Exception:
            entities = []

        if not entities:
            return "## Top Entities\n\n*No entity data available.*"

        lines = ["## Top Entities\n", "| Entity | Mentions | Source Diversity |", "|--------|----------|-----------------|"]
        for e in entities:
            lines.append(f"| {e['entity_name']} | {e['total_mentions']} | {e.get('source_diversity', 'N/A')} |")
        return "\n".join(lines)

    def _cascade_section(self, hours: int) -> str:
        try:
            cascades = self._duckdb.query(f"""
                SELECT entity_name, source_count, source_types, span_hours, weighted_score
                FROM cascade_log
                WHERE detected_at >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
                ORDER BY weighted_score DESC
                LIMIT 10
            """)
        except Exception:
            cascades = []

        if not cascades:
            return "## Cross-Domain Cascades\n\n*No cascades detected in this period.*"

        lines = ["## Cross-Domain Cascades\n"]
        for c in cascades:
            lines.append(
                f"- **{c['entity_name']}** — {c['source_count']} sources, "
                f"{c['span_hours']:.1f}h span, score {c['weighted_score']:.1f}\n"
                f"  Sources: {c['source_types']}"
            )
        return "\n".join(lines)

    def _system_health(self) -> str:
        try:
            crawl_stats = self._duckdb.query("""
                SELECT COUNT(*) as total FROM entity_mention_ts
                WHERE hour_bucket >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            """)
            total = crawl_stats[0]["total"] if crawl_stats else 0
        except Exception:
            total = 0

        return (
            f"## System Health\n\n"
            f"- **Entity mention records (24h):** {total}\n"
            f"- **Report generated successfully**"
        )

    def _footer(self) -> str:
        return (
            "---\n"
            "*Generated by SENTINEL Intelligence Engine*"
        )
