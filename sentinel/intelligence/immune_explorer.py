"""
SENTINEL Adaptive Immune Exploration.
Evolutionary algorithm for discovering "unknown unknowns" using embedding-space vectors.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import structlog
from pydantic import RootModel

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient
from sentinel.models import ExplorationVector

logger = structlog.get_logger(__name__)


class ExplorationVectorList(RootModel):
    """List of exploration vectors for JSON serialization."""
    root: list[ExplorationVector]


class ImmuneExplorer:
    """
    Maintains and evolves a population of exploration vectors.

    These vectors scour the embedding space for content that doesn't match
    curated topics, but historically leads to useful signals.
    """

    def __init__(self, duckdb_client: DuckDBClient) -> None:
        self._duckdb = duckdb_client
        self._config = get_config()
        self._population: list[ExplorationVector] = []
        self._vectors_matrix: Optional[np.ndarray] = None
        self._state_file = Path(self._config.system.data_dir) / "immune_state.json"

    def initialize(self) -> None:
        """Initialize DB tables and load/create population."""
        self._duckdb.execute("""
            CREATE TABLE IF NOT EXISTS immune_activations (
                vector_id INTEGER NOT NULL,
                content_id VARCHAR NOT NULL,
                score DOUBLE NOT NULL,
                activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                led_to_signal BOOLEAN DEFAULT FALSE
            )
        """)

        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        self._load_population()

    def _load_population(self) -> None:
        """Load population from disk or initialize fresh."""
        config = self._config.intelligence.immune
        if not config.enabled:
            return

        if self._state_file.exists():
            try:
                with open(self._state_file, "r") as f:
                    data = json.load(f)
                    self._population = ExplorationVectorList.model_validate(data).root
            except Exception as e:
                logger.error("immune_state_load_failed", error=str(e))
                self._population = []

        if not self._population:
            self._population = self._generate_random_vectors(
                count=config.population_size,
                start_id=1,
            )

        self._update_matrix()
        logger.info(
            "immune_population_loaded",
            size=len(self._population),
            generation=max((v.generation for v in self._population), default=0),
        )

    def _save_population(self) -> None:
        """Save population to disk."""
        data = ExplorationVectorList(root=self._population).model_dump(mode="json")
        with open(self._state_file, "w") as f:
            json.dump(data, f, separators=(",", ":"))

    def _generate_random_vectors(self, count: int, start_id: int, generation: int = 0) -> list[ExplorationVector]:
        """Generate N random L2-normalized vectors."""
        dim = self._config.lancedb.embedding_dim
        raw = np.random.randn(count, dim).astype(np.float32)
        norms = np.linalg.norm(raw, axis=1, keepdims=True)
        normalized = raw / norms

        return [
            ExplorationVector(
                id=start_id + i,
                vector=normalized[i].tolist(),
                generation=generation,
            )
            for i in range(count)
        ]

    def _update_matrix(self) -> None:
        """Update fast numpy matrix for scoring."""
        if not self._population:
            self._vectors_matrix = None
            return

        self._vectors_matrix = np.array([v.vector for v in self._population], dtype=np.float32)

    def score(self, content_id: str, content_embedding: list[float]) -> tuple[float, int]:
        """
        Score content against all exploration vectors.

        Args:
            content_id: ID of the content.
            content_embedding: Normalized embedding vector.

        Returns:
            Tuple of (max_score, winning_vector_id).
        """
        if self._vectors_matrix is None or not self._population:
            return 0.0, 0

        vec = np.array(content_embedding, dtype=np.float32)
        # Fast cosine similarity (dot product of L2-normalized vectors)
        scores = self._vectors_matrix @ vec
        best_idx = int(np.argmax(scores))
        max_score = float(scores[best_idx])
        winner_id = self._population[best_idx].id

        threshold = self._config.intelligence.immune.activation_threshold
        if max_score > threshold:
            self._record_activation(winner_id, content_id, max_score)

        return max_score, winner_id

    def _record_activation(self, vector_id: int, content_id: str, score: float) -> None:
        """Record vector activation for later reward calculation."""
        try:
            self._duckdb.execute(
                """
                INSERT INTO immune_activations (vector_id, content_id, score)
                VALUES (?, ?, ?)
                """,
                (vector_id, content_id, score),
            )
        except Exception as e:
            logger.debug("immune_activation_record_failed", error=str(e))

    def evolve(self) -> dict:
        """
        Run evolutionary cycle on population.

        1. Join activations with signal log to compute rewards.
        2. Rank vectors.
        3. Clone top N% with mutation.
        4. Cull bottom M% and replace with clones.
        5. Check diversity, inject randoms if too low.

        Returns:
            Dict of evolution metrics.
        """
        config = self._config.intelligence.immune
        if not config.enabled or not self._population:
            return {}

        logger.info("immune_evolution_started", population=len(self._population))

        # 1. Update rewards from DB
        try:
            # Mark activations that led to useful signals
            self._duckdb.execute("""
                UPDATE immune_activations
                SET led_to_signal = TRUE
                WHERE content_id IN (
                    SELECT DISTINCT json_extract_string(metadata, '$.content_id')
                    FROM signal_log
                    WHERE json_extract_string(metadata, '$.content_id') IS NOT NULL
                      AND (useful = TRUE OR useful IS NULL)
                ) AND led_to_signal = FALSE
            """)

            # Fetch stats
            rows = self._duckdb.query("""
                SELECT
                    vector_id,
                    COUNT(*) as total_acts,
                    SUM(CAST(led_to_signal AS INTEGER)) as useful_acts
                FROM immune_activations
                GROUP BY vector_id
            """)

            stats_map = {row["vector_id"]: row for row in rows}

            # Update population stats
            for v in self._population:
                stats = stats_map.get(v.id)
                if stats:
                    v.total_activations = stats["total_acts"]
                    v.useful_activations = stats["useful_acts"]
                    # Laplace smoothing (add 1)
                    v.reward = (v.useful_activations + 1) / (v.total_activations + 1)
                else:
                    v.reward = 0.0

        except Exception as e:
            logger.error("immune_reward_update_failed", error=str(e))
            return {}

        # 2. Rank vectors
        self._population.sort(key=lambda x: x.reward, reverse=True)

        num_elite = int(len(self._population) * config.elite_fraction)
        num_cull = int(len(self._population) * config.cull_fraction)

        elites = self._population[:num_elite]
        generation = max((v.generation for v in self._population), default=0) + 1
        next_id = max((v.id for v in self._population), default=0) + 1

        # 3. Create offspring from elites (mutation)
        offspring = []
        for _ in range(num_cull):
            # Select random elite parent
            parent = elites[np.random.randint(len(elites))]
            parent_vec = np.array(parent.vector, dtype=np.float32)

            # Gaussian mutation
            noise = np.random.normal(0, config.mutation_sigma, size=parent_vec.shape).astype(np.float32)
            child_vec = parent_vec + noise
            # L2 normalize
            child_vec = child_vec / np.linalg.norm(child_vec)

            offspring.append(ExplorationVector(
                id=next_id,
                vector=child_vec.tolist(),
                generation=generation,
                parent_id=parent.id,
            ))
            next_id += 1

        # 4. Replace worst vectors with offspring
        self._population[-num_cull:] = offspring

        # 5. Check diversity
        self._update_matrix()
        diversity = 0.0
        if self._vectors_matrix is not None:
            # Sample 200 vectors for fast pairwise distance
            indices = np.random.choice(len(self._vectors_matrix), min(200, len(self._vectors_matrix)), replace=False)
            sample = self._vectors_matrix[indices]
            # Cosine similarity matrix
            sims = sample @ sample.T
            # Cosine distance = 1 - similarity
            dists = 1.0 - sims
            # Mean of upper triangle (excluding diagonal)
            diversity = float(np.mean(dists[np.triu_indices(len(sample), k=1)]))

        new_discoveries = 0

        if diversity < config.min_diversity:
            # Inject fresh random vectors into middle of pack
            logger.info("immune_injecting_diversity", current_diversity=diversity, threshold=config.min_diversity)
            fresh = self._generate_random_vectors(
                count=config.diversity_injection_count,
                start_id=next_id,
                generation=generation,
            )
            # Replace vectors just above the cull line
            start_idx = len(self._population) - num_cull - len(fresh)
            if start_idx > num_elite:
                self._population[start_idx:start_idx+len(fresh)] = fresh
                new_discoveries = len(fresh)

        self._update_matrix()
        self._save_population()

        metrics = {
            "generation": generation,
            "population_size": len(self._population),
            "diversity": round(diversity, 4),
            "top_rewards": [round(v.reward, 3) for v in self._population[:5]],
            "mean_reward": round(sum(v.reward for v in self._population) / len(self._population), 3),
            "injected": new_discoveries,
        }

        logger.info("immune_evolution_completed", **metrics)
        return metrics
