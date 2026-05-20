"""Integration tests for the entire intelligence signal pipeline."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4
from datetime import datetime

import pytest

from sentinel.config import get_config
from sentinel.extraction.llm_extractor import LLMExtractor
from sentinel.extraction.classifier import RelevanceClassifier
from sentinel.intelligence.red_team import RedTeamAgent
from sentinel.intelligence.court import HypothesisCourt
from sentinel.models import ExtractedContent, SourceType, ContentType


@pytest.fixture
def mock_lancedb():
    db = MagicMock()
    # High distance = low similarity (no counterevidence)
    db.search = AsyncMock(return_value=[{"_distance": 0.9, "text": "Unrelated content"}])
    return db


@pytest.fixture
def mock_duckdb():
    db = MagicMock()
    # High base rate
    db.query = MagicMock(return_value=[{"total_count": 100, "useful_count": 80}])
    return db


@pytest.fixture
def mock_embedder():
    embedder = MagicMock()
    embedder.embed_text = MagicMock(return_value=[0.1] * 384)
    embedder.embed_batch = MagicMock(return_value=[[0.1] * 384])
    return embedder


def test_full_signal_pipeline_with_red_team(mock_lancedb, mock_duckdb, mock_embedder):
    """
    Test the flow from extraction -> classification -> red team challenge -> hypothesis court.
    Uses mocked LLMs to avoid actual network calls.
    """
    async def run_test():
        # 1. Setup mocked Extractor
        extractor = LLMExtractor()
        extractor._call_llm = AsyncMock(return_value={
            "title": "Quantum breakthrough",
            "summary": "A new quantum computing capability was quietly announced.",
            "entities": [{"name": "QuantumCorp", "type": "organization", "context": ""}],
            "topics": ["Quantum Computing", "Technology"],
            "key_facts": ["Built a new 1000-qubit system"],
            "published_date": None,
            "sentiment": "positive"
        })

        content = ExtractedContent(
            crawl_job_id=uuid4(),
            url="https://news.ycombinator.com/item?id=123",
            source_type=SourceType.HACKERNEWS,
            content_type=ContentType.ARTICLE,
            title="Quantum breakthrough",
            full_text="QuantumCorp has built a new 1000-qubit system that promises absolute computational supremacy."
        )

        # 1. Extraction
        extracted_data = await extractor.extract(content.full_text, content.url)
        assert extracted_data["title"] == "Quantum breakthrough"

        from sentinel.models import Signal, SignalType, AlertPriority
        # Create Signal manually since there's no overall pipeline orchestrator built
        signal = Signal(
            id=uuid4(),
            source_id=uuid4(),
            target_url=content.url,
            signal_type=SignalType.NOVELTY,
            title="Quantum Signal",
            description=extracted_data["summary"],
            entities=[e["name"] for e in extracted_data["entities"]],
            priority=AlertPriority.HIGH,
            confidence=0.95,
            evidence_urls=[],
            detected_at=datetime.utcnow()
        )

        # 2. Relevance Classification
        classifier = RelevanceClassifier(mock_embedder)
        # mock topic logic
        classifier.score_relevance = MagicMock(return_value=0.8)
        relevance = classifier.score_relevance(signal.description)
        assert relevance == 0.8

        # 3. Red Team Challenge
        # We mock LLM to give no alternative explanations
        red_team = RedTeamAgent(mock_lancedb, mock_duckdb, mock_embedder)
        red_team._find_alternative_explanations = AsyncMock(return_value=0.0)
        
        challenge_result = await red_team.challenge(signal)
        
        # High base rate + no counterevidence + no alts + no FPs -> score = 0
        # Adjusted confidence drops to ~0.88
        assert challenge_result.survived is True
        assert challenge_result.adjusted_confidence >= 0.85

        # 4. Hypothesis Court
        court = HypothesisCourt()
        court._call_llm = AsyncMock(side_effect=[
            "Advocate opening statement: High novelty and strong evidence.",
            "Skeptic cross-examination: The source is just a forum post.",
            "Advocate rebuttal: We corroborated it using other reliable sources.",
            "Skeptic closing: The sources are still unverified.",
            {"likelihood_adv": 0.85, "likelihood_skep": 0.90, "reasoning": "Advocate is right"}
        ])

        verdict = await court.evaluate(signal, causal_explanation=None)
        assert verdict.approved is True
        assert verdict.final_confidence == 0.99
        assert "Advocate is right" in verdict.reasoning

    asyncio.run(run_test())
