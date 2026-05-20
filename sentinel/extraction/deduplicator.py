"""
SENTINEL deduplication pipeline.
3-stage dedup: exact hash, MinHash LSH, semantic similarity.
"""
from __future__ import annotations

from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.core.lancedb_client import LanceDBClient

logger = structlog.get_logger(__name__)


class Deduplicator:
    """
    Multi-stage content deduplication.

    Stage 1: Exact hash match (SHA-256)
    Stage 2: Near-duplicate via MinHash LSH (Jaccard threshold 0.5)
    Stage 3: Semantic duplicate via embedding cosine similarity (threshold 0.92)
    """

    def __init__(self, lancedb_client: Optional[LanceDBClient] = None) -> None:
        """
        Initialize deduplicator.

        Args:
            lancedb_client: LanceDB client for semantic search.
        """
        self._config = get_config()
        self._lancedb = lancedb_client
        self._seen_hashes: set[str] = set()

        # MinHash LSH index
        self._lsh = None
        self._minhashes: dict[str, any] = {}
        self._init_lsh()

    def _init_lsh(self) -> None:
        """Initialize MinHash LSH index."""
        try:
            from datasketch import MinHashLSH
            self._lsh = MinHashLSH(
                threshold=self._config.extraction.dedup.minhash_threshold,
                num_perm=self._config.extraction.dedup.minhash_num_perm,
            )
        except ImportError:
            logger.warning("datasketch_not_available", msg="MinHash dedup disabled")

    def _create_minhash(self, text: str) -> any:
        """Create a MinHash from text using word shingles."""
        from datasketch import MinHash

        shingle_size = self._config.extraction.dedup.shingle_size
        num_perm = self._config.extraction.dedup.minhash_num_perm

        mh = MinHash(num_perm=num_perm)
        words = text.lower().split()

        for i in range(len(words) - shingle_size + 1):
            shingle = " ".join(words[i : i + shingle_size])
            mh.update(shingle.encode("utf-8"))

        return mh

    def is_exact_duplicate(self, content_hash: str) -> bool:
        """
        Check if content hash has been seen before (Stage 1).

        Args:
            content_hash: SHA-256 hash of content.

        Returns:
            True if exact duplicate.
        """
        if content_hash in self._seen_hashes:
            return True
        self._seen_hashes.add(content_hash)
        return False

    def is_near_duplicate(self, text: str, doc_id: str = "") -> bool:
        """
        Check for near-duplicate using MinHash LSH (Stage 2).

        Args:
            text: Text content.
            doc_id: Document identifier.

        Returns:
            True if near-duplicate found.
        """
        if self._lsh is None:
            return False

        try:
            mh = self._create_minhash(text)

            # Query LSH for similar documents
            result = self._lsh.query(mh)
            if result:
                logger.debug("near_duplicate_found", doc_id=doc_id, similar_to=result[:3])
                return True

            # Add to index
            if doc_id:
                try:
                    self._lsh.insert(doc_id, mh)
                    self._minhashes[doc_id] = mh
                except ValueError:
                    pass  # Duplicate key
            return False

        except Exception as e:
            logger.debug("minhash_check_failed", error=str(e))
            return False

    async def is_semantic_duplicate(self, embedding: list[float]) -> bool:
        """
        Check for semantic duplicate using vector similarity (Stage 3).

        Args:
            embedding: Content embedding vector.

        Returns:
            True if semantic duplicate found (cosine similarity > threshold).
        """
        if self._lancedb is None:
            return False

        try:
            results = await self._lancedb.search(
                "content_embeddings",
                query_vector=embedding,
                limit=1,
            )
            if results:
                distance = results[0].get("_distance", 1.0)
                similarity = 1 - distance  # LanceDB returns L2 distance
                threshold = self._config.extraction.dedup.semantic_threshold
                if similarity > threshold:
                    logger.debug("semantic_duplicate_found", similarity=round(similarity, 3))
                    return True
            return False

        except Exception as e:
            logger.debug("semantic_dedup_failed", error=str(e))
            return False

    async def check_all(
        self, content_hash: str, text: str, embedding: list[float], doc_id: str = ""
    ) -> tuple[bool, str]:
        """
        Run all dedup stages sequentially.

        Args:
            content_hash: SHA-256 of content.
            text: Text content.
            embedding: Content embedding.
            doc_id: Document identifier.

        Returns:
            Tuple of (is_duplicate, reason).
        """
        if self.is_exact_duplicate(content_hash):
            return True, "exact_hash"

        if self.is_near_duplicate(text, doc_id):
            return True, "near_duplicate_minhash"

        if await self.is_semantic_duplicate(embedding):
            return True, "semantic_duplicate"

        return False, ""
