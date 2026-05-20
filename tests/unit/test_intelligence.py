"""Unit tests for intelligence and signal modules."""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

from sentinel.config import get_config
from sentinel.intelligence.immune_explorer import ImmuneExplorer
from sentinel.intelligence.red_team import RedTeamAgent
from sentinel.models import Signal, SignalType, AlertPriority


@pytest.fixture
def mock_duckdb():
    db = MagicMock()
    db.execute = MagicMock()
    db.query = MagicMock(return_value=[])
    return db


@pytest.fixture
def mock_lancedb():
    db = MagicMock()
    db.search = AsyncMock(return_value=[{"_distance": 0.1, "text": "Related paragraph"}])
    return db


@pytest.fixture
def mock_embedder():
    embedder = MagicMock()
    embedder.embed_text = MagicMock(return_value=[0.1] * 384)
    return embedder


def test_immune_explorer_evolution(mock_duckdb):
    """Test genetic algorithm evolution mechanics."""
    config = get_config()
    dim = config.lancedb.embedding_dim

    explorer = ImmuneExplorer(mock_duckdb)
    
    # Init blank population to bypass db loading tests
    explorer._population = explorer._generate_random_vectors(count=100, start_id=1, generation=0)
    explorer._update_matrix()

    # Score a document to simulate activation
    embedding = [0.1] * dim
    # Normalize expected vec structure
    vec = np.array(embedding, dtype=np.float32)
    vec = vec / np.linalg.norm(vec)

    score, winner_id = explorer.score("doc_1", vec.tolist())
    assert score > 0.0
    assert winner_id > 0

    # Ensure evolution runs and resets population correctly
    metrics = explorer.evolve()
    assert metrics["generation"] == 1
    assert metrics["population_size"] == 100
    assert "diversity" in metrics


def test_red_team_challenge(mock_lancedb, mock_duckdb, mock_embedder):
    """Test adversarial red team signal challenges."""
    async def run_test():
        from uuid import uuid4
        from datetime import datetime

        signal = Signal(
            id=uuid4(),
            source_id=uuid4(),
            target_url="https://sec.gov/filing",
            signal_type=SignalType.NOVELTY,
            title="Merger Signal",
            description="Merger announcement expected next week.",
            entities=["Company A"],
            priority=AlertPriority.HIGH,
            confidence=0.9,
            evidence_urls=["https://sec.gov/filing1", "https://reddit.com/r/stocks/merger"],
            detected_at=datetime.utcnow(),
        )

        # Mock alternative explanations to return nothing logic
        agent = RedTeamAgent(mock_lancedb, mock_duckdb, mock_embedder)
        agent._find_alternative_explanations = AsyncMock(return_value=0.2)

        result = await agent.challenge(signal)

        # Should have run tests
        assert result.original_signal_id == signal.id
        assert result.survived is True  # with 0.9 confidence and small penalties
        
        # Source penalty: sec.gov is Tier 1 (1.0), reddit is Tier 3 (0.5). Mean = 0.75 (>0.5) so score should be 0.0
        assert result.source_reliability_score == 0.0
        
        # Alternative explanation returned 0.2
        assert result.alternative_explanation_score == 0.2
        
    asyncio.run(run_test())
