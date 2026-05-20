"""
SENTINEL embedding pipeline.
Sentence-transformer embeddings with MPS acceleration.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)


class Embedder:
    """
    Embedding pipeline using sentence-transformers.

    Lazy-loads all-MiniLM-L6-v2 on first use.
    Uses MPS (Metal Performance Shaders) on Apple Silicon.
    L2-normalizes all embeddings.
    """

    def __init__(self) -> None:
        """Initialize embedder (model loaded on first call)."""
        self._model = None
        self._config = get_config()

    def _load_model(self) -> None:
        """Lazy-load the sentence-transformer model."""
        if self._model is not None:
            return

        model_name = self._config.extraction.embedding.model
        device = self._config.extraction.embedding.device

        try:
            from sentence_transformers import SentenceTransformer

            # Try requested device, fall back to CPU
            try:
                self._model = SentenceTransformer(model_name, device=device)
            except Exception:
                self._model = SentenceTransformer(model_name, device="cpu")
                logger.warning("embedder_device_fallback", device="cpu")

            logger.info("embedder_model_loaded", model=model_name, device=device)

            # Warmup
            self._model.encode(["warmup"], normalize_embeddings=True)

        except Exception as e:
            logger.error("embedder_load_failed", model=model_name, error=str(e))
            self._model = None

    def embed_text(self, text: str) -> list[float]:
        """
        Embed a single text.

        Encodes title + first 512 tokens, L2-normalized.

        Args:
            text: Text to embed.

        Returns:
            384-dimensional normalized embedding vector.
        """
        self._load_model()
        if self._model is None:
            return [0.0] * self._config.lancedb.embedding_dim

        try:
            # Truncate to reasonable length
            truncated = text[:2048]
            embedding = self._model.encode(
                truncated,
                normalize_embeddings=self._config.extraction.embedding.normalize,
            )
            return embedding.tolist()
        except Exception as e:
            logger.error("embed_text_failed", error=str(e))
            return [0.0] * self._config.lancedb.embedding_dim

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a batch of texts.

        Args:
            texts: List of texts to embed.

        Returns:
            List of 384-dimensional normalized embedding vectors.
        """
        self._load_model()
        if self._model is None:
            dim = self._config.lancedb.embedding_dim
            return [[0.0] * dim for _ in texts]

        try:
            truncated = [t[:2048] for t in texts]
            batch_size = self._config.extraction.embedding.batch_size
            embeddings = self._model.encode(
                truncated,
                batch_size=batch_size,
                normalize_embeddings=self._config.extraction.embedding.normalize,
                show_progress_bar=False,
            )
            return embeddings.tolist()
        except Exception as e:
            logger.error("embed_batch_failed", error=str(e), count=len(texts))
            dim = self._config.lancedb.embedding_dim
            return [[0.0] * dim for _ in texts]

    def embed_paragraphs(self, title: str, full_text: str) -> list[tuple[str, list[float]]]:
        """
        Embed up to 20 paragraphs from a document.

        Args:
            title: Document title (used as context prefix).
            full_text: Full text to split into paragraphs.

        Returns:
            List of (paragraph_text, embedding_vector) tuples.
        """
        paragraphs = [p.strip() for p in full_text.split("\n\n") if len(p.strip()) > 50]
        # Cap at 20 paragraphs to bound storage
        paragraphs = paragraphs[:20]

        if not paragraphs:
            return []

        # Prefix with title for better context
        texts_to_embed = [f"{title}. {p}" for p in paragraphs]
        embeddings = self.embed_batch(texts_to_embed)

        return list(zip(paragraphs, embeddings))

    def embed_relationship(self, subject: str, predicate: str, object_: str) -> list[float]:
        """
        Embed a knowledge graph relationship.

        Args:
            subject: Source node name.
            predicate: Relationship type.
            object_: Target node name.

        Returns:
            Embedding vector representing the triple.
        """
        text = f"{subject} {predicate} {object_}"
        return self.embed_text(text)
