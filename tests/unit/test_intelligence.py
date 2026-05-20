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


def test_causal_simulator(mock_lancedb, mock_embedder):
    """Test CausalSimulator counterfactual reasoning logic."""
    import sqlite3
    import tempfile
    from pathlib import Path
    from unittest.mock import patch
    from sentinel.intelligence.causal_simulator import CausalSimulator

    # Setup a mock SQLite database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "knowledge_graph.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, canonical_name TEXT, entity_type TEXT, mention_count INTEGER, pagerank REAL)")
        conn.execute("CREATE TABLE edges (source_id TEXT, target_id TEXT, relationship_type TEXT, confidence REAL, weight REAL)")
        conn.execute("INSERT INTO nodes VALUES ('n1', 'Entity A', 'company', 10, 0.1)")
        conn.execute("INSERT INTO nodes VALUES ('n2', 'Entity B', 'product', 5, 0.05)")
        conn.execute("INSERT INTO edges VALUES ('n1', 'n2', 'develops', 0.9, 0.8)")
        conn.commit()
        conn.close()

        # Initialize simulator and mock path
        sim = CausalSimulator(mock_lancedb, mock_embedder)
        sim._db_path = db_path

        async def run_sim_test():
            # Test subgraph fetching
            subgraph = await sim.get_local_subgraph(["Entity A", "Entity B"])
            assert len(subgraph["nodes"]) == 2
            assert len(subgraph["edges"]) == 1

            # Mock httpx.AsyncClient response
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_response.json = MagicMock(return_value={
                "choices": [{
                    "message": {
                        "content": '{"abduction_notes": "Abduction text", "action_adjustments": "Action text", "prediction_outcome": "Prediction text", "estimated_counterfactual_probability": 0.45, "causal_necessity": 0.8, "causal_sufficiency": 0.6, "detailed_explanation": "Detailed text"}'
                    }
                }]
            })

            with patch("httpx.AsyncClient.post", AsyncMock(return_value=mock_response)):
                result = await sim.simulate_counterfactual(
                    signal_title="Test Signal",
                    signal_desc="Test Description",
                    treatment_node="Entity A",
                    outcome_node="Entity B",
                    intervention_val="inactive"
                )
                assert result["status"] == "success"
                assert result["causal_necessity"] == 0.8
                assert result["causal_sufficiency"] == 0.6
                assert result["estimated_counterfactual_probability"] == 0.45

        asyncio.run(run_sim_test())

