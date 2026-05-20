"""
SENTINEL Graph Builder.
Constructs and maintains the knowledge graph from resolved entities
and extracted relationships. The central memory of the system.

Enhanced with:
- Confidence-aware edge creation
- Event node generation from high-signal content
- Relationship ontology validation
- Batch persistence for performance
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.core.neo4j_client import Neo4jClient, RELATIONSHIP_ONTOLOGY
from sentinel.core.lancedb_client import LanceDBClient
from sentinel.models import ExtractedContent, GraphEdge, GraphNode

logger = structlog.get_logger(__name__)

# Event types that warrant creating first-class event nodes
EVENT_TRIGGER_RELATIONSHIPS = {
    "ACQUIRED", "ACQUIRED_BY", "RAISED_FUNDING", "FILED_PATENT",
    "FOUNDED", "FOUNDED_BY", "CAUSES", "CAUSED_BY",
}


class GraphBuilder:
    """
    Builds and maintains the knowledge graph.

    Operations:
    - Upsert nodes with canonical resolution
    - Upsert edges with confidence scoring and ontology validation
    - Generate event nodes for significant relationships
    - Track mention sources and timestamps
    - Trigger algorithm recomputation when graph changes significantly
    """

    def __init__(self, neo4j: Neo4jClient, lance: LanceDBClient) -> None:
        self._graph = neo4j
        self._lance = lance
        self._config = get_config().knowledge
        self._upsert_count = 0

    async def initialize(self) -> None:
        """Initialize graph store."""
        logger.info("graph_builder_initialized")

    async def upsert_node(self, node: GraphNode) -> None:
        """Create or update an entity node in the graph."""
        try:
            self._graph.upsert_node(
                node_id=node.id,
                canonical_name=node.canonical_name,
                entity_type=node.entity_type.value,
                aliases=node.aliases,
                description=node.description or "",
                first_seen=node.first_seen.isoformat(),
                last_seen=node.last_seen.isoformat(),
                mention_count=node.mention_count,
                community_id=node.community_id or 0,
            )

            # Store embedding in LanceDB for vector search
            if node.embedding:
                await self._lance.insert(
                    "entity_embeddings",
                    [{
                        "id": node.id,
                        "name": node.canonical_name,
                        "entity_type": node.entity_type.value,
                        "vector": node.embedding,
                    }],
                )

        except Exception as e:
            logger.error("upsert_node_failed", node_id=node.id, error=str(e))

    async def upsert_edge(self, edge: GraphEdge) -> None:
        """Create or update a relationship edge with validation and confidence."""
        try:
            # Block self-loops and low-value generic relationships
            if edge.source_node_id == edge.target_node_id:
                return
            if edge.relationship_type in ("RELATED_TO", "MENTIONS"):
                return

            # Validate relationship against ontology
            src_type = self._get_node_type(edge.source_node_id)
            tgt_type = self._get_node_type(edge.target_node_id)
            rel_type = edge.relationship_type

            if not self._graph.validate_relationship(src_type, tgt_type, rel_type):
                # Try swapping direction for inverse relationships
                ontology = RELATIONSHIP_ONTOLOGY.get(rel_type, {})
                inverse = ontology.get("inverse")
                if inverse and self._graph.validate_relationship(tgt_type, src_type, inverse):
                    # Swap source/target and use inverse relationship
                    edge.source_node_id, edge.target_node_id = edge.target_node_id, edge.source_node_id
                    rel_type = inverse
                else:
                    # Drop the edge rather than adding noise with RELATED_TO
                    logger.debug("edge_dropped_invalid_types",
                                 rel=rel_type, src_type=src_type, tgt_type=tgt_type)
                    return

            self._graph.upsert_edge(
                source_id=edge.source_node_id,
                target_id=edge.target_node_id,
                relationship_type=rel_type,
                weight=edge.weight,
                confidence=0.5,  # Initial confidence for single-source
                evidence_urls=edge.evidence_urls,
                first_seen=edge.first_seen.isoformat(),
                last_seen=edge.last_seen.isoformat(),
                properties=edge.properties,
            )
            logger.info("edge_upserted", rel=rel_type,
                        src=edge.source_node_id[:40], tgt=edge.target_node_id[:40])
        except Exception as e:
            logger.error("upsert_edge_failed", error=str(e),
                         src=edge.source_node_id[:40], tgt=edge.target_node_id[:40],
                         rel=edge.relationship_type)

    def _get_node_type(self, node_id: str) -> str:
        """Get entity type for a node."""
        if self._graph._graph.has_node(node_id):
            return self._graph._graph.nodes[node_id].get("entity_type", "unknown")
        return "unknown"

    async def process_content(
        self,
        content: ExtractedContent,
        nodes: list[GraphNode],
        edges: list[GraphEdge],
    ) -> None:
        """
        Process extracted content into the knowledge graph.

        Includes:
        - Node upserts with embedding storage
        - Edge upserts with ontology validation
        - Event node generation for significant relationships
        - Periodic algorithm recomputation
        """
        for node in nodes:
            await self.upsert_node(node)

        for edge in edges:
            await self.upsert_edge(edge)

            # Generate event nodes for significant relationships
            if edge.relationship_type in EVENT_TRIGGER_RELATIONSHIPS:
                self._create_event_from_edge(edge, content)

        # Store content embedding for semantic search
        if content.embedding:
            try:
                await self._lance.insert(
                    "content_embeddings",
                    [{
                        "id": str(content.id),
                        "url": content.url,
                        "title": content.title,
                        "source_type": content.source_type.value,
                        "vector": content.embedding,
                        "timestamp": content.extracted_at.isoformat(),
                    }],
                )
            except Exception as e:
                logger.error("content_embedding_store_failed", error=str(e))

        self._upsert_count += len(nodes) + len(edges)

        # Recompute graph algorithms every 100 upserts
        if self._upsert_count >= 100:
            self._graph.recompute_if_stale(max_age_hours=1)
            self._upsert_count = 0

        logger.debug(
            "content_processed",
            url=content.url,
            nodes=len(nodes),
            edges=len(edges),
        )

    def _create_event_from_edge(self, edge: GraphEdge, content: ExtractedContent) -> None:
        """Generate a first-class event node from a significant relationship."""
        event_id = f"event:{hashlib.sha256(f'{edge.source_node_id}:{edge.relationship_type}:{edge.target_node_id}:{content.url}'.encode()).hexdigest()[:16]}"
        self._graph.add_event(
            event_id=event_id,
            event_type=edge.relationship_type.lower(),
            title=f"{edge.relationship_type}: {edge.source_node_id} → {edge.target_node_id}",
            entity_ids=[edge.source_node_id, edge.target_node_id],
            description=edge.properties.get("evidence", ""),
            source_url=content.url,
            confidence=0.5,
        )

    async def get_entity_neighbors(self, entity_id: str, max_hops: int = 2) -> dict:
        """Get an entity's neighborhood in the graph."""
        neighbors = self._graph.get_neighbors(entity_id, max_hops)
        return {"entity_id": entity_id, "neighbors": neighbors}

    async def get_graph_stats(self) -> dict:
        """Get comprehensive graph statistics."""
        return self._graph.get_graph_stats()
