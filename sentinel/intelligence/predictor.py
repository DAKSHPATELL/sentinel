"""
SENTINEL Predictive Crawler.
Forecasts future events and generates proactive search queries.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

import httpx
import orjson
import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient
from sentinel.models import Prediction, PredictionAction

logger = structlog.get_logger(__name__)

PREDICTIVE_PROMPT = """Analyze these recent high-priority signals from the web intelligence system.
Based on this data, predict 3 likely future events or disclosures that might happen in the next 7-30 days.

For EACH prediction, generate 2-3 specific Google/Twitter search queries that would detect early whispers or leaks of this event BEFORE it is officially announced. Use advanced search operators (site:, filetype:, etc) where appropriate.

Respond ONLY with valid JSON:
{{
  "predictions": [
    {{
      "event_description": "Company X is likely acquiring Startup Y",
      "probability": 0.65,
      "timeframe_days": 14,
      "queries": [
        "\\"Startup Y\\" site:sec.gov",
        "\\"acquisition\\" OR \\"merger\\" \\"Company X\\" \\"Startup Y\\" -news"
      ]
    }}
  ]
}}

Recent Signals:
{signals}"""


class PredictiveCrawler:
    """
    Analyzes historical high-value signals to forecast future events.
    Generates targeted search queries to detect early evidence.
    """

    def __init__(self, duckdb_client: DuckDBClient) -> None:
        self._duckdb = duckdb_client
        self._config = get_config()

    def initialize(self) -> None:
        """Create tables for tracking predictions."""
        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                prediction_id VARCHAR PRIMARY KEY,
                event_description TEXT NOT NULL,
                probability DOUBLE NOT NULL,
                timeframe_days INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR DEFAULT 'PENDING',
                resolution_score DOUBLE
            )
        """)

        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS prediction_actions (
                action_id VARCHAR PRIMARY KEY,
                prediction_id VARCHAR NOT NULL,
                query TEXT NOT NULL,
                source_type VARCHAR NOT NULL,
                executed_at TIMESTAMP,
                discovered_urls INTEGER DEFAULT 0
            )
        """)

    async def generate_predictions(self) -> list[Prediction]:
        """
        Analyze recent signals and generate new predictions + queries.

        Returns:
            List of generated Prediction objects.
        """
        config = self._config.extraction
        if not self._config.intelligence.immune.enabled:  # Re-use config or general intelligence toggle
            pass

        # Fetch recent high priority useful signals
        try:
            rows = self._duckdb.query("""
                SELECT description, entities, signal_type, detected_at
                FROM signal_log
                WHERE useful = TRUE
                  AND priority IN ('high', 'critical')
                ORDER BY detected_at DESC
                LIMIT 20
            """)
            if not rows:
                logger.info("predictive_crawler_no_signals")
                return []

            signal_text = "\n\n".join(
                f"- [{row['detected_at']}] {row['signal_type']}: {row['description']} (Entities: {row['entities']})"
                for row in rows
            )
        except Exception as e:
            logger.error("predictive_crawler_db_failed", error=str(e))
            return []

        prompt = PREDICTIVE_PROMPT.format(signals=signal_text)

        try:
            async with httpx.AsyncClient(timeout=config.llm_timeout_seconds) as client:
                resp = await client.post(
                    f"{config.llm_base_url.rstrip('/')}/v1/chat/completions",
                    json={
                        "model": config.llm_model,
                        "messages": [
                            {"role": "system", "content": "You are a forecasting intelligence engine. Respond ONLY with valid JSON."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.4,
                        "max_tokens": 1500,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"].strip()

                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

                result = orjson.loads(content)
                predictions_data = result.get("predictions", [])

                predictions = []
                import uuid
                for p in predictions_data:
                    pred_id = str(uuid.uuid4())
                    
                    actions = []
                    for q in p.get("queries", []):
                        # Simple heuristic: if query looks like a domain search, maybe it goes to generic, etc.
                        # We'll use GOOGLE_ALERTS as a placeholder for search queries.
                        from sentinel.models import SourceType
                        actions.append(PredictionAction(
                            action_id=str(uuid.uuid4()),
                            prediction_id=pred_id,
                            query=q,
                            source_type=SourceType.GOOGLE_ALERTS, 
                        ))

                    pred = Prediction(
                        prediction_id=pred_id,
                        event_description=p.get("event_description", ""),
                        probability=p.get("probability", 0.0),
                        timeframe_days=p.get("timeframe_days", 14),
                        actions=actions,
                    )
                    predictions.append(pred)

                # Persist to DB
                self._save_predictions(predictions)

                logger.info("predictions_generated", count=len(predictions))
                return predictions

        except Exception as e:
            logger.error("prediction_generation_failed", error=str(e))
            return []

    def _save_predictions(self, predictions: list[Prediction]) -> None:
        """Save new predictions and their actions to DuckDB."""
        for p in predictions:
            try:
                self._duckdb.execute(
                    """
                    INSERT INTO predictions (prediction_id, event_description, probability, timeframe_days)
                    VALUES (?, ?, ?, ?)
                    """,
                    (p.prediction_id, p.event_description, p.probability, p.timeframe_days)
                )

                for a in p.actions:
                    self._duckdb.execute(
                        """
                        INSERT INTO prediction_actions (action_id, prediction_id, query, source_type)
                        VALUES (?, ?, ?, ?)
                        """,
                        (a.action_id, a.prediction_id, a.query, a.source_type.value)
                    )
            except Exception as e:
                logger.error("save_prediction_failed", prediction_id=p.prediction_id, error=str(e))
