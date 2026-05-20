"""
SENTINEL relevance classifier.
Topic embedding-based relevance scoring.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import structlog

from sentinel.config import get_config
from sentinel.constants import DEFAULT_TOPICS
from sentinel.extraction.embedder import Embedder
from sentinel.intelligence.immune_explorer import ImmuneExplorer

logger = structlog.get_logger(__name__)


class RelevanceClassifier:
    """
    Content relevance scorer using topic embeddings.

    On init, encodes DEFAULT_TOPICS into embeddings.
    Scores content by max cosine similarity against topic embeddings.
    """

    def __init__(self, embedder: Embedder, immune_explorer: Optional[ImmuneExplorer] = None) -> None:
        """
        Initialize relevance classifier.

        Args:
            embedder: Embedder instance for encoding topics.
            immune_explorer: Optional immune system explorer.
        """
        self._embedder = embedder
        self._immune_explorer = immune_explorer
        self._config = get_config()
        self._topic_embeddings: Optional[np.ndarray] = None

    def initialize(self) -> None:
        """Compute and cache topic embeddings."""
        embeddings_path = Path(self._config.extraction.relevance.topic_embeddings_path)

        # Try loading cached embeddings
        if embeddings_path.exists():
            try:
                self._topic_embeddings = np.load(str(embeddings_path))
                logger.info("topic_embeddings_loaded", path=str(embeddings_path), count=len(self._topic_embeddings))
                return
            except Exception:
                pass

        # Compute fresh embeddings
        logger.info("computing_topic_embeddings", count=len(DEFAULT_TOPICS))
        embeddings = self._embedder.embed_batch(DEFAULT_TOPICS)
        self._topic_embeddings = np.array(embeddings, dtype=np.float32)

        # Save to disk
        embeddings_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(str(embeddings_path), self._topic_embeddings)
        logger.info("topic_embeddings_saved", path=str(embeddings_path))

    def score_relevance(self, embedding: list[float]) -> float:
        """
        Score content relevance against topic embeddings.

        Uses max cosine similarity across all topic embeddings.

        Args:
            embedding: Content embedding vector (L2-normalized).

        Returns:
            Relevance score [0.0, 1.0].
        """
        if self._topic_embeddings is None:
            self.initialize()

        if self._topic_embeddings is None or len(self._topic_embeddings) == 0:
            return 0.0

        try:
            query = np.array(embedding, dtype=np.float32)
            # Cosine similarity = dot product (since embeddings are L2-normalized)
            similarities = np.dot(self._topic_embeddings, query)
            return float(np.max(similarities))
        except Exception as e:
            logger.error("relevance_scoring_failed", error=str(e))
            return 0.0

    def score_batch(self, embeddings: list[list[float]]) -> list[float]:
        """
        Score relevance for a batch of embeddings.

        Uses vectorized numpy dot product for efficiency.

        Args:
            embeddings: List of content embedding vectors.

        Returns:
            List of relevance scores.
        """
        if self._topic_embeddings is None:
            self.initialize()

        if self._topic_embeddings is None or len(self._topic_embeddings) == 0:
            return [0.0] * len(embeddings)

        try:
            queries = np.array(embeddings, dtype=np.float32)
            # (n_queries, dim) @ (dim, n_topics) -> (n_queries, n_topics)
            similarities = np.dot(queries, self._topic_embeddings.T)
            return np.max(similarities, axis=1).tolist()
        except Exception as e:
            logger.error("batch_relevance_scoring_failed", error=str(e))
            return [0.0] * len(embeddings)

    def score_exploration(self, content_id: str, embedding: list[float]) -> tuple[float, int]:
        """
        Score content against immune exploration vectors.
        Records activation if score > threshold.

        Args:
            content_id: ID of the extracted content.
            embedding: Content embedding vector.

        Returns:
            Tuple of (max_exploration_score, winning_vector_id).
        """
        if self._immune_explorer:
            return self._immune_explorer.score(content_id, embedding)
        return 0.0, 0
