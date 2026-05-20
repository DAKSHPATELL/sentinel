"""
SENTINEL Entity Resolver.
Merges duplicate entities using hybrid string + semantic similarity.
"OpenAI", "Open AI", "openai" → one canonical entity node.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.constants import ENTITY_SUFFIXES
from sentinel.core.lancedb_client import LanceDBClient
from sentinel.extraction.embedder import Embedder
from sentinel.models import EntityType, ExtractedEntity, GraphNode

logger = structlog.get_logger(__name__)


def _normalize_name(name: str) -> str:
    """Normalize an entity name for comparison."""
    s = name.strip().lower()
    # Remove common suffixes (Inc, LLC, etc.)
    for suffix in ENTITY_SUFFIXES:
        s = re.sub(rf"\s*{re.escape(suffix.lower())}\s*$", "", s)
    # Remove punctuation
    s = re.sub(r"[,.\-'\"()]", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _jaro_winkler(s1: str, s2: str) -> float:
    """Jaro-Winkler string similarity (0.0 to 1.0)."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    max_dist = max(len(s1), len(s2)) // 2 - 1
    if max_dist < 0:
        max_dist = 0

    s1_matches = [False] * len(s1)
    s2_matches = [False] * len(s2)
    matches = 0
    transpositions = 0

    for i in range(len(s1)):
        start = max(0, i - max_dist)
        end = min(i + max_dist + 1, len(s2))
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len(s1)):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches / len(s1) + matches / len(s2) + (matches - transpositions / 2) / matches) / 3

    # Winkler bonus for common prefix
    prefix = 0
    for i in range(min(4, len(s1), len(s2))):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break

    return jaro + prefix * 0.1 * (1 - jaro)


class EntityResolver:
    """
    Resolves extracted entities into canonical graph nodes.

    Resolution pipeline:
    1. Normalize name (lowercase, strip suffixes)
    2. Check exact match in known entities
    3. Compute Jaro-Winkler string similarity
    4. Compute embedding cosine similarity
    5. Hybrid score = 0.4 * string_sim + 0.6 * embedding_sim
    6. If hybrid > threshold → merge into existing node
    7. Otherwise → create new node
    """

    def __init__(self, lance: LanceDBClient, embedder: Embedder) -> None:
        self._lance = lance
        self._embedder = embedder
        self._config = get_config().knowledge
        self._known_entities: dict[str, GraphNode] = {}  # normalized_name → node
        self._alias_index: dict[str, str] = {}  # alias → canonical normalized name

    def resolve(self, entity: ExtractedEntity) -> GraphNode:
        """
        Resolve an extracted entity to a canonical GraphNode.

        Returns existing node if match found, otherwise creates new one.
        """
        norm = _normalize_name(entity.text)
        if not norm:
            return self._create_node(entity)

        # 1. Exact match in alias index
        if norm in self._alias_index:
            canonical = self._alias_index[norm]
            node = self._known_entities[canonical]
            self._update_node(node, entity)
            return node

        # 2. Fuzzy match against known entities
        best_match: Optional[str] = None
        best_score = 0.0
        threshold = self._config.entity_resolution_threshold

        for known_norm, known_node in self._known_entities.items():
            # String similarity
            str_sim = _jaro_winkler(norm, known_norm)

            # Quick reject: if string similarity is very low, skip embedding
            if str_sim < 0.5:
                continue

            # Compute hybrid score
            hybrid = str_sim  # Start with string sim only; embedding below if needed

            if str_sim < threshold and str_sim >= 0.6:
                # Borderline case: use embedding to disambiguate
                try:
                    entity_emb = self._embedder.embed_text(entity.text)
                    if known_node.embedding:
                        emb_sim = sum(a * b for a, b in zip(entity_emb, known_node.embedding))
                        hybrid = 0.4 * str_sim + 0.6 * emb_sim
                except Exception:
                    pass

            if hybrid > best_score:
                best_score = hybrid
                best_match = known_norm

        if best_match and best_score >= threshold:
            node = self._known_entities[best_match]
            self._update_node(node, entity)
            # Register new alias
            self._alias_index[norm] = best_match
            if entity.text not in node.aliases:
                node.aliases.append(entity.text)
            return node

        # 3. No match → create new node
        return self._create_node(entity)

    def resolve_batch(self, entities: list[ExtractedEntity]) -> list[GraphNode]:
        """Resolve a batch of entities."""
        return [self.resolve(e) for e in entities]

    def _create_node(self, entity: ExtractedEntity) -> GraphNode:
        """Create a new graph node from an extracted entity."""
        norm = _normalize_name(entity.text)
        try:
            embedding = self._embedder.embed_text(entity.text)
        except Exception:
            embedding = None

        node = GraphNode(
            id=f"entity:{norm.replace(' ', '_')}",
            canonical_name=entity.text,
            entity_type=entity.entity_type,
            aliases=[entity.text],
            embedding=embedding,
        )

        if norm:
            self._known_entities[norm] = node
            self._alias_index[norm] = norm

        return node

    def _update_node(self, node: GraphNode, entity: ExtractedEntity) -> None:
        """Update an existing node with new mention data."""
        from datetime import datetime
        node.last_seen = datetime.utcnow()
        node.mention_count += 1
        if entity.text not in node.aliases:
            node.aliases.append(entity.text)

    def get_node(self, name: str) -> Optional[GraphNode]:
        """Look up a known entity by name."""
        norm = _normalize_name(name)
        canonical = self._alias_index.get(norm, norm)
        return self._known_entities.get(canonical)

    def get_all_nodes(self) -> list[GraphNode]:
        """Return all known entity nodes."""
        return list(self._known_entities.values())

    def load_from_graph(self, nodes: list[GraphNode]) -> None:
        """Preload resolver state from existing graph nodes."""
        for node in nodes:
            norm = _normalize_name(node.canonical_name)
            self._known_entities[norm] = node
            self._alias_index[norm] = norm
            for alias in node.aliases:
                alias_norm = _normalize_name(alias)
                if alias_norm:
                    self._alias_index[alias_norm] = norm
