"""
SENTINEL Red Team Agent.
Adversarial signal challenge system — 5 strategies to disprove signals before they reach the human.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID

import httpx
import orjson
import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient
from sentinel.core.lancedb_client import LanceDBClient
from sentinel.extraction.embedder import Embedder
from sentinel.models import ChallengeResult, Signal

logger = structlog.get_logger(__name__)

# Source reliability tiers
TIER_1_DOMAINS = {
    "sec.gov", "patents.google.com", "arxiv.org", "scholar.google.com",
    "nature.com", "science.org", "ieee.org", "acm.org",
}
TIER_2_DOMAINS = {
    "nytimes.com", "wsj.com", "reuters.com", "bloomberg.com",
    "techcrunch.com", "arstechnica.com", "wired.com", "theverge.com",
    "hackernews.com", "github.com",
}
TIER_3_DOMAINS = {
    "reddit.com", "news.ycombinator.com", "twitter.com", "x.com",
    "medium.com", "substack.com",
}


class RedTeamAgent:
    """
    Adversarial signal challenge agent.

    Five challenges run in parallel:
    1. Counterevidence search
    2. Alternative explanation generation
    3. Base rate check
    4. Historical false positive matching
    5. Source reliability scoring

    Aggregation: final_challenge = min(1.0, sum(scores))
    Adjusted confidence = signal.confidence * (1 - final_challenge * challenge_weight)
    Signal survives if adjusted_confidence > survival_threshold
    """

    def __init__(
        self,
        lancedb_client: Optional[LanceDBClient] = None,
        duckdb_client: Optional[DuckDBClient] = None,
        embedder: Optional[Embedder] = None,
    ) -> None:
        self._lancedb = lancedb_client
        self._duckdb = duckdb_client
        self._embedder = embedder
        self._config = get_config()

    async def challenge(self, signal: Signal) -> ChallengeResult:
        """
        Run all 5 adversarial challenges against a signal.

        Args:
            signal: Signal to challenge.

        Returns:
            ChallengeResult with adjusted confidence and challenge details.
        """
        config = self._config.intelligence.red_team

        if not config.enabled:
            return ChallengeResult(
                original_signal_id=signal.id,
                adjusted_confidence=signal.confidence,
                survived=True,
            )

        # Check minimum priority
        priority_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
        signal_priority = priority_order.get(signal.priority.value, 0)
        min_priority = priority_order.get(config.min_signal_priority_to_challenge, 1)
        if signal_priority < min_priority:
            return ChallengeResult(
                original_signal_id=signal.id,
                adjusted_confidence=signal.confidence,
                survived=True,
            )

        # Run all 5 challenges in parallel
        results = await asyncio.gather(
            self._find_counterevidence(signal),
            self._find_alternative_explanations(signal),
            self._check_base_rate(signal),
            self._check_historical_false_positives(signal),
            self._check_source_reliability(signal),
            return_exceptions=True,
        )

        # Extract scores (default 0 on exception)
        scores = []
        for r in results:
            if isinstance(r, (int, float)):
                scores.append(float(r))
            elif isinstance(r, Exception):
                logger.debug("challenge_error", error=str(r))
                scores.append(0.0)
            else:
                scores.append(0.0)

        counter_score, alt_score, base_rate_score, fp_score, reliability_score = scores

        # Aggregate
        total_challenge = min(1.0, sum(scores))
        adjusted_confidence = signal.confidence * (1 - total_challenge * config.challenge_weight)
        survived = adjusted_confidence > config.survival_threshold

        # Determine kill reason
        kill_reason = None
        if not survived:
            max_score_idx = scores.index(max(scores))
            reasons = [
                "counterevidence_found",
                "alternative_explanations_plausible",
                "low_base_rate",
                "historical_false_positive_match",
                "low_source_reliability",
            ]
            kill_reason = reasons[max_score_idx]

        logger.info(
            "red_team_challenge_completed",
            signal_id=str(signal.id),
            original_confidence=signal.confidence,
            adjusted_confidence=round(adjusted_confidence, 3),
            challenge_score=round(total_challenge, 3),
            survived=survived,
            kill_reason=kill_reason,
        )

        return ChallengeResult(
            original_signal_id=signal.id,
            adjusted_confidence=adjusted_confidence,
            challenge_score=total_challenge,
            counterevidence_score=counter_score,
            alternative_explanation_score=alt_score,
            base_rate_score=base_rate_score,
            false_positive_score=fp_score,
            source_reliability_score=reliability_score,
            survived=survived,
            kill_reason=kill_reason,
            details={
                "entities": signal.entities,
                "evidence_count": len(signal.evidence_urls),
            },
        )

    async def _find_counterevidence(self, signal: Signal) -> float:
        """
        Challenge 1: Search for content contradicting the signal.

        Score += 0.3 if 3+ counterevidence hits with relevance > 0.5
        """
        if self._lancedb is None or self._embedder is None:
            return 0.0

        score = 0.0

        for entity in signal.entities[:3]:
            # Negate the signal claim
            negation_query = f"evidence against {entity} declining failing"
            embedding = self._embedder.embed_text(negation_query)

            try:
                results = await self._lancedb.search(
                    "content_embeddings",
                    query_vector=embedding,
                    limit=self._config.intelligence.red_team.counterevidence_search_limit,
                )

                # Count high-relevance counterevidence
                high_relevance_hits = sum(
                    1 for r in results
                    if (1 - r.get("_distance", 1.0)) > 0.5
                )

                if high_relevance_hits >= 3:
                    score += 0.3
                    break  # One entity with strong counterevidence is enough

            except Exception as e:
                logger.debug("counterevidence_search_failed", entity=entity, error=str(e))

        return min(0.3, score)

    async def _find_alternative_explanations(self, signal: Signal) -> float:
        """
        Challenge 2: Generate alternative explanations via LLM.

        Score += 0.2 per alternative with plausibility > 0.6
        """
        config = self._config.extraction
        prompt = f"""Signal detected: {signal.description}

Generate {self._config.intelligence.red_team.alternative_explanations_count} alternative explanations for this data pattern that would NOT indicate the claimed trend.

For each alternative, rate its plausibility from 0.0 to 1.0.

Respond ONLY with valid JSON:
{{
  "alternatives": [
    {{"explanation": "...", "plausibility": 0.7}},
    {{"explanation": "...", "plausibility": 0.4}},
    {{"explanation": "...", "plausibility": 0.6}}
  ]
}}"""

        try:
            async with httpx.AsyncClient(timeout=config.llm_timeout_seconds) as client:
                resp = await client.post(
                    f"{config.llm_base_url.rstrip('/')}/v1/chat/completions",
                    json={
                        "model": config.llm_model,
                        "messages": [
                            {"role": "system", "content": "You are a critical analyst finding alternative explanations. Respond ONLY with JSON."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.7,
                        "max_tokens": 1024,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"].strip()

                # Parse JSON
                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

                result = orjson.loads(content)
                alternatives = result.get("alternatives", [])

                score = 0.0
                for alt in alternatives:
                    plausibility = alt.get("plausibility", 0.0)
                    if plausibility > 0.6:
                        score += 0.2

                return min(0.6, score)

        except Exception as e:
            logger.debug("alternative_explanations_failed", error=str(e))
            return 0.0

    async def _check_base_rate(self, signal: Signal) -> float:
        """
        Challenge 3: Check historical base rate for this signal type.

        Score += 0.3 if historically_useful_rate < 0.15
        """
        if self._duckdb is None:
            return 0.0

        try:
            lookback_days = self._config.intelligence.red_team.base_rate_lookback_days
            rows = self._duckdb.query(
                """
                SELECT
                    COUNT(*) FILTER (WHERE useful = true) AS useful_count,
                    COUNT(*) AS total_count
                FROM signal_log
                WHERE signal_type = ?
                  AND detected_at > CURRENT_TIMESTAMP - INTERVAL ? DAY
                """,
                (signal.signal_type.value, lookback_days),
            )

            if rows and rows[0]["total_count"] > 5:
                rate = rows[0]["useful_count"] / rows[0]["total_count"]
                if rate < 0.15:
                    return 0.3
                elif rate < 0.3:
                    return 0.15
        except Exception as e:
            logger.debug("base_rate_check_failed", error=str(e))

        return 0.0

    async def _check_historical_false_positives(self, signal: Signal) -> float:
        """
        Challenge 4: Find similar past signals that were rated NOT useful.

        Score += 0.25 if 3+ similar false positives exist (cosine > 0.8)
        """
        if self._lancedb is None or self._embedder is None:
            return 0.0

        try:
            embedding = self._embedder.embed_text(signal.description)

            results = await self._lancedb.search(
                "content_embeddings",
                query_vector=embedding,
                limit=20,
            )

            threshold = self._config.intelligence.red_team.false_positive_similarity_threshold
            similar_fps = sum(
                1 for r in results
                if (1 - r.get("_distance", 1.0)) > threshold
            )

            if similar_fps >= 3:
                return 0.25
            elif similar_fps >= 1:
                return 0.1

        except Exception as e:
            logger.debug("false_positive_check_failed", error=str(e))

        return 0.0

    async def _check_source_reliability(self, signal: Signal) -> float:
        """
        Challenge 5: Score source reliability of evidence URLs.

        Tier 1 (SEC, arXiv, patents): 1.0
        Tier 2 (major news, tech blogs): 0.8
        Tier 3 (Reddit, HN, forums): 0.5
        Tier 4 (unknown): 0.3

        Score += 0.2 if mean reliability < 0.5
        """
        if not signal.evidence_urls:
            return 0.1  # No evidence at all is slightly suspicious

        scores = []
        for url in signal.evidence_urls:
            try:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc.lower()
                # Strip www.
                if domain.startswith("www."):
                    domain = domain[4:]

                if domain in TIER_1_DOMAINS or any(domain.endswith(f".{d}") for d in TIER_1_DOMAINS):
                    scores.append(1.0)
                elif domain in TIER_2_DOMAINS or any(domain.endswith(f".{d}") for d in TIER_2_DOMAINS):
                    scores.append(0.8)
                elif domain in TIER_3_DOMAINS or any(domain.endswith(f".{d}") for d in TIER_3_DOMAINS):
                    scores.append(0.5)
                else:
                    scores.append(0.3)
            except Exception:
                scores.append(0.3)

        mean_reliability = sum(scores) / max(len(scores), 1)

        if mean_reliability < 0.4:
            return 0.2
        elif mean_reliability < 0.5:
            return 0.1

        return 0.0
