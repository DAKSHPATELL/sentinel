"""Unit tests for stealth and acquisition modules."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sentinel.config import get_config
from sentinel.stealth.annihilator import AcquisitionOrchestrator
from sentinel.stealth.temporal_arbitrage import TemporalArbitrageScheduler
from sentinel.models import AcquisitionResult


@pytest.fixture
def mock_sqlite():
    db = MagicMock()
    db.execute = AsyncMock()
    db.query = AsyncMock(return_value=[])
    return db


@pytest.fixture
def mock_duckdb():
    db = MagicMock()
    db.execute = MagicMock()
    db.query = MagicMock(return_value=[{"hour_of_day": 3, "total_attempts": 100, "total_successes": 90, "attempts": 100, "successes": 90}])
    return db


def test_orchestrator_fallback(mock_sqlite):
    """Test AcquisitionOrchestrator try-fallback logic."""
    async def run_test():
        orchestrator = AcquisitionOrchestrator(mock_sqlite)
        await orchestrator.initialize()

        # Mock strategies to always fail except the last one
        from sentinel.stealth import annihilator

        # Capture original strategies
        orig_strategies = annihilator.ALL_STRATEGIES

        try:
            # Create mock strategies
            async def fail_strategy(url):
                raise Exception("Failed")

            async def success_strategy(url):
                return "<html>Success</html>" + "A" * 100

            test_strategies = [
                ("strat1", fail_strategy),
                ("strat2", fail_strategy),
                ("strat3", success_strategy)
            ]
            # Override the instance's strategies directly
            annihilator.ALL_STRATEGIES = test_strategies
            orchestrator._strategies = dict(test_strategies)

            # Set config top N = 1 so it fails wave 1, succeeds wave 2
            config = get_config()
            config.stealth.annihilator.max_parallel_strategies = 2

            result = await orchestrator.acquire("https://example.com/test")

            assert result.success is True
            assert result.strategy_used == "strat3"
            assert result.content.startswith("<html>Success</html>")
            assert result.strategies_attempted <= 3

        finally:
            # Restore original strategies
            annihilator.ALL_STRATEGIES = orig_strategies
            
    asyncio.run(run_test())


def test_temporal_arbitrage_scheduler(mock_duckdb):
    """Test temporal scheduler computes best hours and scheduling."""
    # Temporarily set min bounds low for test
    config = get_config()
    config.stealth.temporal_arbitrage.min_success_data_points = 50

    scheduler = TemporalArbitrageScheduler(mock_duckdb)
    scheduler.initialize()

    best_hour = scheduler.get_best_hour("example.com")
    assert best_hour == 3  # Based on the mocked DB return above

    # Should compute a datetime pointing to 3 AM
    dt = scheduler.compute_next_retry_time("example.com")
    assert dt is not None
    assert dt.hour == 3
