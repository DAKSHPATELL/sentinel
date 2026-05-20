"""
SENTINEL Graph Store — Temporal-Probabilistic-Event Knowledge Graph (TPE-KG).

Architecture innovations over standard property graphs:
1. Temporal facts: every edge has valid_from/valid_to for time-aware queries
2. Probabilistic edges: confidence scores that strengthen with corroboration
3. Event nodes: first-class events linking entities through causal chains
4. Graph algorithms: PageRank, community detection, centrality — computed and cached
5. Relationship ontology: typed constraints on valid source→target entity pairs

Built on NetworkX + SQLite. Zero external dependencies.
"""
from __future__ import annotations

import json
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import networkx as nx
import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)

# ─── RELATIONSHIP ONTOLOGY ─────────────────────────────────────
# Defines valid (source_type, target_type) constraints per relationship.
# None means any type is allowed in that position.
RELATIONSHIP_ONTOLOGY: dict[str, dict[str, Any]] = {
    # Relaxed constraints — NER often misclassifies entity types
    # (e.g., "GrapheneOS" as product instead of organization)
    # so we allow broader source/target types
    "FOUNDED_BY":      {"source": None, "target": ["person"], "inverse": "FOUNDED"},
    "FOUNDED":         {"source": ["person"], "target": None, "inverse": "FOUNDED_BY"},
    "ACQUIRED":        {"source": None, "target": None, "inverse": "ACQUIRED_BY"},
    "ACQUIRED_BY":     {"source": None, "target": None, "inverse": "ACQUIRED"},
    "COMPETES_WITH":   {"source": None, "target": None, "symmetric": True},
    "PARTNERS_WITH":   {"source": None, "target": None, "symmetric": True},
    "USES_TECHNOLOGY": {"source": None, "target": None},
    "PRODUCES":        {"source": None, "target": None},
    "FILED_PATENT":    {"source": None, "target": None},
    "RAISED_FUNDING":  {"source": None, "target": None},
    "WORKS_AT":        {"source": ["person"], "target": None, "inverse": "EMPLOYS"},
    "EMPLOYS":         {"source": None, "target": ["person"], "inverse": "WORKS_AT"},
    "INVENTED":        {"source": None, "target": None},
    "AUTHORED":        {"source": ["person"], "target": None},
    "ENABLES":         {"source": None, "target": None},
    "SUPERSEDES":      {"source": None, "target": None},
    "IMPLEMENTS":      {"source": None, "target": None},
    "INVESTS_IN":      {"source": None, "target": None},
    "SUBSIDIARY_OF":   {"source": None, "target": None, "inverse": "PARENT_OF"},
    "PARENT_OF":       {"source": None, "target": None, "inverse": "SUBSIDIARY_OF"},
    "LOCATED_IN":      {"source": None, "target": ["location"]},
    "CAUSED_BY":       {"source": None, "target": None, "inverse": "CAUSES"},
    "CAUSES":          {"source": None, "target": None, "inverse": "CAUSED_BY"},
    "PARTICIPATES_IN": {"source": None, "target": None},
    "RELATED_TO":      {"source": None, "target": None},
    "MENTIONS":        {"source": None, "target": None},
}


class Neo4jClient:
    """
    Temporal-Probabilistic-Event Knowledge Graph (TPE-KG).

    Provides:
    - Node/edge CRUD with temporal validity and confidence scoring
    - Event nodes as first-class citizens for causal reasoning
    - Graph algorithms: PageRank, betweenness centrality, community detection
    - Time-aware queries: "what was true at time T?"
    - Confidence decay: edges weaken over time without corroboration
    - Relationship ontology validation

    Backed by NetworkX (in-memory) + SQLite (persistent).
    """

    def __init__(self) -> None:
        self._graph: nx.DiGraph = nx.DiGraph()
        self._db: Optional[sqlite3.Connection] = None
        self._db_path: Optional[Path] = None
        # Cached algorithm results
        self._pagerank: dict[str, float] = {}
        self._communities: dict[str, int] = {}
        self._centrality: dict[str, float] = {}
        self._algo_last_computed: Optional[datetime] = None

    async def connect(self) -> None:
        """Initialize graph store and load from SQLite if exists."""
        config = get_config()
        data_dir = Path(config.system.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = data_dir / "knowledge_graph.db"

        self._db = sqlite3.connect(str(self._db_path))
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")

        # ── Schema: nodes ─────────────────────────────────────
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                entity_type TEXT,
                aliases TEXT,
                description TEXT,
                first_seen TEXT,
                last_seen TEXT,
                mention_count INTEGER DEFAULT 1,
                community_id INTEGER DEFAULT 0,
                pagerank REAL DEFAULT 0.0,
                centrality REAL DEFAULT 0.0,
                properties TEXT
            )
        """)

        # ── Schema: edges with temporal + confidence ──────────
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS edges (
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                relationship_type TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                confidence REAL DEFAULT 0.5,
                evidence_count INTEGER DEFAULT 1,
                evidence_urls TEXT,
                valid_from TEXT,
                valid_to TEXT,
                first_seen TEXT,
                last_seen TEXT,
                properties TEXT,
                PRIMARY KEY (source_id, target_id, relationship_type)
            )
        """)

        # ── Schema: events (first-class event nodes) ──────────
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                occurred_at TEXT,
                detected_at TEXT,
                source_url TEXT,
                confidence REAL DEFAULT 0.5,
                entity_ids TEXT,
                causal_parent_id TEXT,
                properties TEXT
            )
        """)

        # Add new columns to existing tables (migration-safe, must run before indexes)
        for col, coltype, default in [
            ("pagerank", "REAL", "0.0"),
            ("centrality", "REAL", "0.0"),
        ]:
            try:
                self._db.execute(f"ALTER TABLE nodes ADD COLUMN {col} {coltype} DEFAULT {default}")
            except sqlite3.OperationalError:
                pass  # Column already exists
        for col, coltype, default in [
            ("confidence", "REAL", "0.5"),
            ("evidence_count", "INTEGER", "1"),
            ("valid_from", "TEXT", "NULL"),
            ("valid_to", "TEXT", "NULL"),
        ]:
            try:
                self._db.execute(f"ALTER TABLE edges ADD COLUMN {col} {coltype} DEFAULT {default}")
            except sqlite3.OperationalError:
                pass

        # ── Indexes ───────────────────────────────────────────
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_node_name ON nodes(canonical_name)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_node_type ON nodes(entity_type)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_node_pagerank ON nodes(pagerank DESC)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_edge_source ON edges(source_id)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_edge_target ON edges(target_id)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_edge_confidence ON edges(confidence DESC)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_edge_valid_from ON edges(valid_from)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_event_time ON events(occurred_at)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_event_type ON events(event_type)")

        self._db.commit()

        # Load existing graph from SQLite
        self._load_from_db()

        # Compute algorithms on startup if graph is non-trivial
        if self._graph.number_of_nodes() > 10:
            self._compute_algorithms()

        logger.info(
            "graph_store_connected",
            path=str(self._db_path),
            nodes=self._graph.number_of_nodes(),
            edges=self._graph.number_of_edges(),
        )

    def _load_from_db(self) -> None:
        """Load graph state from SQLite."""
        if not self._db:
            return
        cursor = self._db.execute("SELECT * FROM nodes")
        cols = [d[0] for d in cursor.description]
        for row in cursor:
            data = dict(zip(cols, row))
            node_id = data.pop("id")
            for json_field in ("aliases", "properties"):
                if data.get(json_field):
                    try:
                        data[json_field] = json.loads(data[json_field])
                    except (json.JSONDecodeError, TypeError):
                        data[json_field] = [] if json_field == "aliases" else {}
            self._graph.add_node(node_id, **data)

        cursor = self._db.execute("SELECT * FROM edges")
        cols = [d[0] for d in cursor.description]
        for row in cursor:
            data = dict(zip(cols, row))
            src = data.pop("source_id")
            tgt = data.pop("target_id")
            for json_field in ("evidence_urls", "properties"):
                if data.get(json_field):
                    try:
                        data[json_field] = json.loads(data[json_field])
                    except (json.JSONDecodeError, TypeError):
                        data[json_field] = [] if json_field == "evidence_urls" else {}
            self._graph.add_edge(src, tgt, **data)

    # ── Persistence ───────────────────────────────────────────

    def _persist_node(self, node_id: str) -> None:
        """Write a single node to SQLite."""
        if not self._db:
            return
        attrs = dict(self._graph.nodes[node_id])
        aliases = json.dumps(attrs.pop("aliases", []))
        properties = json.dumps(attrs.pop("properties", {}))
        self._db.execute(
            """INSERT OR REPLACE INTO nodes
               (id, canonical_name, entity_type, aliases, description,
                first_seen, last_seen, mention_count, community_id,
                pagerank, centrality, properties)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                node_id,
                attrs.get("canonical_name", ""),
                attrs.get("entity_type", ""),
                aliases,
                attrs.get("description", ""),
                attrs.get("first_seen", ""),
                attrs.get("last_seen", ""),
                attrs.get("mention_count", 1),
                attrs.get("community_id", 0),
                attrs.get("pagerank", 0.0),
                attrs.get("centrality", 0.0),
                properties,
            ),
        )
        self._db.commit()

    def _persist_edge(self, src: str, tgt: str) -> None:
        """Write a single edge to SQLite."""
        if not self._db:
            return
        attrs = dict(self._graph.edges[src, tgt])
        evidence = json.dumps(attrs.pop("evidence_urls", []))
        properties = json.dumps(attrs.pop("properties", {}))
        self._db.execute(
            """INSERT OR REPLACE INTO edges
               (source_id, target_id, relationship_type, weight, confidence,
                evidence_count, evidence_urls, valid_from, valid_to,
                first_seen, last_seen, properties)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                src,
                tgt,
                attrs.get("relationship_type", "RELATED_TO"),
                attrs.get("weight", 1.0),
                attrs.get("confidence", 0.5),
                attrs.get("evidence_count", 1),
                evidence,
                attrs.get("valid_from", ""),
                attrs.get("valid_to", ""),
                attrs.get("first_seen", ""),
                attrs.get("last_seen", ""),
                properties,
            ),
        )
        self._db.commit()

    def _persist_event(self, event_id: str, event_data: dict) -> None:
        """Write an event to SQLite."""
        if not self._db:
            return
        self._db.execute(
            """INSERT OR REPLACE INTO events
               (id, event_type, title, description, occurred_at, detected_at,
                source_url, confidence, entity_ids, causal_parent_id, properties)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event_id,
                event_data.get("event_type", "unknown"),
                event_data.get("title", ""),
                event_data.get("description", ""),
                event_data.get("occurred_at", ""),
                event_data.get("detected_at", datetime.utcnow().isoformat()),
                event_data.get("source_url", ""),
                event_data.get("confidence", 0.5),
                json.dumps(event_data.get("entity_ids", [])),
                event_data.get("causal_parent_id"),
                json.dumps(event_data.get("properties", {})),
            ),
        )
        self._db.commit()

    # ── Public API: Node Operations ───────────────────────────

    def upsert_node(
        self,
        node_id: str,
        canonical_name: str,
        entity_type: str,
        aliases: list[str],
        description: str = "",
        first_seen: str = "",
        last_seen: str = "",
        mention_count: int = 1,
        community_id: int = 0,
    ) -> None:
        """Create or update a node in the graph."""
        if self._graph.has_node(node_id):
            n = self._graph.nodes[node_id]
            n["last_seen"] = last_seen
            n["mention_count"] = n.get("mention_count", 0) + mention_count
            existing_aliases = set(n.get("aliases", []))
            existing_aliases.update(aliases)
            n["aliases"] = list(existing_aliases)
            if description and not n.get("description"):
                n["description"] = description
        else:
            self._graph.add_node(
                node_id,
                canonical_name=canonical_name,
                entity_type=entity_type,
                aliases=aliases,
                description=description,
                first_seen=first_seen,
                last_seen=last_seen,
                mention_count=mention_count,
                community_id=community_id,
                pagerank=0.0,
                centrality=0.0,
            )
        self._persist_node(node_id)

    def upsert_edge(
        self,
        source_id: str,
        target_id: str,
        relationship_type: str,
        weight: float = 1.0,
        confidence: float = 0.5,
        evidence_urls: list[str] | None = None,
        valid_from: str = "",
        valid_to: str = "",
        first_seen: str = "",
        last_seen: str = "",
        properties: dict | None = None,
    ) -> None:
        """
        Create or update an edge with confidence scoring.

        Confidence grows with corroboration:
        - Each new evidence source increases confidence toward 1.0
        - Formula: conf = 1 - (1 - base_conf) * decay^(evidence_count - 1)
        """
        if self._graph.has_edge(source_id, target_id):
            e = self._graph.edges[source_id, target_id]
            e["weight"] = e.get("weight", 0) + weight
            e["last_seen"] = last_seen or datetime.utcnow().isoformat()
            # Corroboration: each new evidence increases confidence
            prev_count = e.get("evidence_count", 1)
            e["evidence_count"] = prev_count + 1
            # Asymptotic confidence: approaches 1.0 with more evidence
            e["confidence"] = 1.0 - (0.5 * (0.7 ** e["evidence_count"]))
            existing_urls = e.get("evidence_urls", [])
            if evidence_urls:
                new_urls = [u for u in evidence_urls if u not in existing_urls]
                e["evidence_urls"] = existing_urls + new_urls
            if valid_from and not e.get("valid_from"):
                e["valid_from"] = valid_from
            if valid_to:
                e["valid_to"] = valid_to
        else:
            # Ensure both nodes exist
            for nid in (source_id, target_id):
                if not self._graph.has_node(nid):
                    self._graph.add_node(
                        nid,
                        canonical_name=nid.replace("entity:", "").replace("_", " "),
                        entity_type="unknown",
                        pagerank=0.0,
                        centrality=0.0,
                    )
            self._graph.add_edge(
                source_id,
                target_id,
                relationship_type=relationship_type,
                weight=weight,
                confidence=confidence,
                evidence_count=1,
                evidence_urls=evidence_urls or [],
                valid_from=valid_from or first_seen,
                valid_to=valid_to,
                first_seen=first_seen or datetime.utcnow().isoformat(),
                last_seen=last_seen or datetime.utcnow().isoformat(),
                properties=properties or {},
            )
        self._persist_edge(source_id, target_id)

    # ── Public API: Event Operations ──────────────────────────

    def add_event(
        self,
        event_id: str,
        event_type: str,
        title: str,
        entity_ids: list[str],
        description: str = "",
        occurred_at: str = "",
        source_url: str = "",
        confidence: float = 0.5,
        causal_parent_id: str | None = None,
        properties: dict | None = None,
    ) -> None:
        """
        Add a first-class event node to the graph.

        Events connect entities through temporal/causal chains:
        entity_A --PARTICIPATES_IN--> event --CAUSES--> event_B
        """
        event_data = {
            "event_type": event_type,
            "title": title,
            "description": description,
            "occurred_at": occurred_at or datetime.utcnow().isoformat(),
            "detected_at": datetime.utcnow().isoformat(),
            "source_url": source_url,
            "confidence": confidence,
            "entity_ids": entity_ids,
            "causal_parent_id": causal_parent_id,
            "properties": properties or {},
        }

        # Add event as a node in the graph
        self._graph.add_node(
            event_id,
            canonical_name=title,
            entity_type="event",
            node_kind="event",
            **event_data,
        )

        # Link participating entities to the event
        for eid in entity_ids:
            if self._graph.has_node(eid):
                self._graph.add_edge(
                    eid, event_id,
                    relationship_type="PARTICIPATES_IN",
                    confidence=confidence,
                    weight=1.0,
                    evidence_count=1,
                )

        # Link causal parent if specified
        if causal_parent_id and self._graph.has_node(causal_parent_id):
            self._graph.add_edge(
                causal_parent_id, event_id,
                relationship_type="CAUSES",
                confidence=confidence,
                weight=1.0,
                evidence_count=1,
            )

        self._persist_event(event_id, event_data)

    def get_causal_chain(self, event_id: str, max_depth: int = 5) -> list[dict]:
        """Trace the causal chain backward from an event."""
        chain = []
        visited = set()
        current = event_id
        for _ in range(max_depth):
            if current in visited or not self._graph.has_node(current):
                break
            visited.add(current)
            node = self._graph.nodes[current]
            chain.append({
                "id": current,
                "title": node.get("title", node.get("canonical_name", "")),
                "type": node.get("event_type", node.get("entity_type", "")),
                "occurred_at": node.get("occurred_at", ""),
            })
            # Find causal parent
            parent = node.get("causal_parent_id")
            if not parent:
                # Look for incoming CAUSES edges
                for pred in self._graph.predecessors(current):
                    edge = self._graph.edges[pred, current]
                    if edge.get("relationship_type") == "CAUSES":
                        parent = pred
                        break
            if not parent:
                break
            current = parent
        return chain

    # ── Public API: Graph Algorithms ──────────────────────────

    def _compute_algorithms(self) -> None:
        """Compute and cache PageRank, communities, and centrality."""
        try:
            # PageRank — importance of each node
            if self._graph.number_of_edges() > 0:
                self._pagerank = nx.pagerank(self._graph, alpha=0.85, max_iter=100)
            else:
                self._pagerank = {n: 1.0 / max(self._graph.number_of_nodes(), 1)
                                  for n in self._graph.nodes()}

            # Community detection — Louvain via greedy modularity
            undirected = self._graph.to_undirected()
            if undirected.number_of_edges() > 0:
                try:
                    from networkx.algorithms.community import greedy_modularity_communities
                    communities = greedy_modularity_communities(undirected, resolution=1.0)
                    self._communities = {}
                    for idx, comm in enumerate(communities):
                        for node in comm:
                            self._communities[node] = idx
                except Exception:
                    self._communities = {n: 0 for n in self._graph.nodes()}
            else:
                self._communities = {n: 0 for n in self._graph.nodes()}

            # Betweenness centrality — bridge nodes
            if self._graph.number_of_edges() > 0:
                # Use approximate centrality for large graphs
                k = min(100, self._graph.number_of_nodes())
                self._centrality = nx.betweenness_centrality(self._graph, k=k)
            else:
                self._centrality = {n: 0.0 for n in self._graph.nodes()}

            # Write back to nodes
            for node_id in self._graph.nodes():
                n = self._graph.nodes[node_id]
                n["pagerank"] = self._pagerank.get(node_id, 0.0)
                n["community_id"] = self._communities.get(node_id, 0)
                n["centrality"] = self._centrality.get(node_id, 0.0)

            # Batch persist all nodes
            if self._db:
                for node_id in self._graph.nodes():
                    attrs = self._graph.nodes[node_id]
                    self._db.execute(
                        "UPDATE nodes SET pagerank=?, community_id=?, centrality=? WHERE id=?",
                        (attrs.get("pagerank", 0.0),
                         attrs.get("community_id", 0),
                         attrs.get("centrality", 0.0),
                         node_id),
                    )
                self._db.commit()

            self._algo_last_computed = datetime.utcnow()
            n_communities = len(set(self._communities.values())) if self._communities else 0
            logger.info(
                "graph_algorithms_computed",
                nodes=self._graph.number_of_nodes(),
                communities=n_communities,
            )
        except Exception as e:
            logger.error("graph_algorithm_failed", error=str(e))

    def recompute_if_stale(self, max_age_hours: int = 1) -> None:
        """Recompute algorithms if cache is stale."""
        if self._algo_last_computed is None:
            if self._graph.number_of_nodes() > 10:
                self._compute_algorithms()
            return
        age = datetime.utcnow() - self._algo_last_computed
        if age > timedelta(hours=max_age_hours) and self._graph.number_of_nodes() > 10:
            self._compute_algorithms()

    def get_pagerank_top(self, n: int = 20) -> list[dict]:
        """Get the top-N nodes by PageRank."""
        self.recompute_if_stale()
        ranked = sorted(
            self._pagerank.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:n]
        result = []
        for node_id, score in ranked:
            attrs = self._graph.nodes.get(node_id, {})
            result.append({
                "id": node_id,
                "name": attrs.get("canonical_name", node_id),
                "type": attrs.get("entity_type", "unknown"),
                "pagerank": round(score, 6),
                "mentions": attrs.get("mention_count", 0),
                "community": attrs.get("community_id", 0),
            })
        return result

    def get_communities(self) -> dict[int, list[dict]]:
        """Get all communities with their members."""
        self.recompute_if_stale()
        communities: dict[int, list[dict]] = defaultdict(list)
        for node_id, comm_id in self._communities.items():
            attrs = self._graph.nodes.get(node_id, {})
            communities[comm_id].append({
                "id": node_id,
                "name": attrs.get("canonical_name", node_id),
                "type": attrs.get("entity_type", "unknown"),
                "pagerank": attrs.get("pagerank", 0.0),
            })
        # Sort each community by pagerank
        for comm_id in communities:
            communities[comm_id].sort(key=lambda x: x["pagerank"], reverse=True)
        return dict(communities)

    def get_bridge_nodes(self, n: int = 10) -> list[dict]:
        """Get nodes with highest betweenness centrality (bridge/connector nodes)."""
        self.recompute_if_stale()
        ranked = sorted(
            self._centrality.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:n]
        result = []
        for node_id, score in ranked:
            attrs = self._graph.nodes.get(node_id, {})
            result.append({
                "id": node_id,
                "name": attrs.get("canonical_name", node_id),
                "type": attrs.get("entity_type", "unknown"),
                "centrality": round(score, 6),
                "community": attrs.get("community_id", 0),
            })
        return result

    # ── Public API: Temporal Queries ──────────────────────────

    def get_edges_at_time(self, timestamp: str) -> list[dict]:
        """Get all edges that were valid at a given time."""
        results = []
        for src, tgt, data in self._graph.edges(data=True):
            valid_from = data.get("valid_from", "")
            valid_to = data.get("valid_to", "")
            # Include if: no temporal bounds, or timestamp falls within bounds
            if valid_from and timestamp < valid_from:
                continue
            if valid_to and timestamp > valid_to:
                continue
            results.append({
                "source": src,
                "target": tgt,
                "type": data.get("relationship_type", ""),
                "confidence": data.get("confidence", 0.5),
                "valid_from": valid_from,
                "valid_to": valid_to,
            })
        return results

    def get_entity_timeline(self, entity_id: str) -> list[dict]:
        """Get temporal timeline of all relationships for an entity."""
        if not self._graph.has_node(entity_id):
            return []
        timeline = []
        # Outgoing edges
        for _, tgt, data in self._graph.out_edges(entity_id, data=True):
            timeline.append({
                "direction": "out",
                "other": tgt,
                "other_name": self._graph.nodes.get(tgt, {}).get("canonical_name", tgt),
                "type": data.get("relationship_type", ""),
                "confidence": data.get("confidence", 0.5),
                "first_seen": data.get("first_seen", ""),
                "last_seen": data.get("last_seen", ""),
                "valid_from": data.get("valid_from", ""),
                "valid_to": data.get("valid_to", ""),
            })
        # Incoming edges
        for src, _, data in self._graph.in_edges(entity_id, data=True):
            timeline.append({
                "direction": "in",
                "other": src,
                "other_name": self._graph.nodes.get(src, {}).get("canonical_name", src),
                "type": data.get("relationship_type", ""),
                "confidence": data.get("confidence", 0.5),
                "first_seen": data.get("first_seen", ""),
                "last_seen": data.get("last_seen", ""),
                "valid_from": data.get("valid_from", ""),
                "valid_to": data.get("valid_to", ""),
            })
        # Sort by first_seen
        timeline.sort(key=lambda x: x.get("first_seen", ""))
        return timeline

    def decay_confidences(self, half_life_days: int = 30) -> int:
        """
        Apply time-based confidence decay to all edges.

        Edges not corroborated recently lose confidence.
        Formula: conf * 2^(-days_since_last_seen / half_life)
        Returns number of edges decayed.
        """
        now = datetime.utcnow()
        decayed = 0
        for src, tgt, data in self._graph.edges(data=True):
            last_seen = data.get("last_seen", "")
            if not last_seen:
                continue
            try:
                last = datetime.fromisoformat(last_seen)
                days_since = (now - last).total_seconds() / 86400
                if days_since > 1:  # Only decay if >1 day old
                    decay_factor = 2 ** (-days_since / half_life_days)
                    old_conf = data.get("confidence", 0.5)
                    new_conf = max(0.05, old_conf * decay_factor)  # Floor at 0.05
                    if new_conf < old_conf - 0.01:
                        data["confidence"] = new_conf
                        decayed += 1
            except (ValueError, TypeError):
                continue
        if decayed > 0:
            logger.info("confidence_decay_applied", edges_decayed=decayed)
        return decayed

    # ── Public API: Queries ───────────────────────────────────

    def get_neighbors(self, entity_id: str, max_hops: int = 2, limit: int = 50,
                      min_confidence: float = 0.0) -> list[dict]:
        """Get an entity's neighborhood, filtered by edge confidence."""
        if not self._graph.has_node(entity_id):
            return []
        neighbors = []
        visited = {entity_id}
        frontier = [(entity_id, 0)]
        while frontier and len(neighbors) < limit:
            current, depth = frontier.pop(0)
            if depth >= max_hops:
                continue
            for nbr in list(self._graph.successors(current)) + list(self._graph.predecessors(current)):
                if nbr in visited:
                    continue
                # Check edge confidence
                edge_data = (self._graph.edges.get((current, nbr))
                             or self._graph.edges.get((nbr, current))
                             or {})
                if edge_data.get("confidence", 1.0) < min_confidence:
                    continue
                visited.add(nbr)
                n = self._graph.nodes[nbr]
                neighbors.append({
                    "id": nbr,
                    "name": n.get("canonical_name", nbr),
                    "type": n.get("entity_type", "unknown"),
                    "mentions": n.get("mention_count", 0),
                    "pagerank": n.get("pagerank", 0.0),
                    "community": n.get("community_id", 0),
                    "distance": depth + 1,
                    "edge_confidence": edge_data.get("confidence", 0.0),
                    "edge_type": edge_data.get("relationship_type", ""),
                })
                frontier.append((nbr, depth + 1))
        neighbors.sort(key=lambda x: (x["distance"], -x["pagerank"]))
        return neighbors[:limit]

    def find_shortest_path(self, source_id: str, target_id: str) -> list[dict] | None:
        """Find shortest path between two entities."""
        if not self._graph.has_node(source_id) or not self._graph.has_node(target_id):
            return None
        try:
            path = nx.shortest_path(self._graph, source_id, target_id)
            result = []
            for i, node_id in enumerate(path):
                entry = {
                    "id": node_id,
                    "name": self._graph.nodes[node_id].get("canonical_name", node_id),
                    "type": self._graph.nodes[node_id].get("entity_type", "unknown"),
                }
                if i > 0:
                    edge = self._graph.edges.get((path[i-1], node_id), {})
                    entry["edge_type"] = edge.get("relationship_type", "")
                    entry["edge_confidence"] = edge.get("confidence", 0.0)
                result.append(entry)
            return result
        except nx.NetworkXNoPath:
            return None

    def get_high_confidence_subgraph(self, min_confidence: float = 0.7) -> dict:
        """Extract the high-confidence subgraph for reliable reasoning."""
        nodes = set()
        edges = []
        for src, tgt, data in self._graph.edges(data=True):
            if data.get("confidence", 0.0) >= min_confidence:
                nodes.add(src)
                nodes.add(tgt)
                edges.append({
                    "source": src,
                    "target": tgt,
                    "type": data.get("relationship_type", ""),
                    "confidence": data.get("confidence", 0.0),
                })
        return {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "edges": edges[:100],  # Limit for API response size
        }

    def get_graph_stats(self) -> dict:
        """Get comprehensive graph statistics."""
        type_dist: dict[str, int] = {}
        for _, data in self._graph.nodes(data=True):
            t = data.get("entity_type", "unknown")
            type_dist[t] = type_dist.get(t, 0) + 1

        # Edge statistics
        edge_type_dist: dict[str, int] = {}
        confidence_sum = 0.0
        confidence_count = 0
        for _, _, data in self._graph.edges(data=True):
            rt = data.get("relationship_type", "RELATED_TO")
            edge_type_dist[rt] = edge_type_dist.get(rt, 0) + 1
            confidence_sum += data.get("confidence", 0.5)
            confidence_count += 1

        n_communities = len(set(self._communities.values())) if self._communities else 0

        return {
            "node_count": self._graph.number_of_nodes(),
            "edge_count": self._graph.number_of_edges(),
            "type_distribution": type_dist,
            "edge_type_distribution": edge_type_dist,
            "avg_confidence": round(confidence_sum / max(confidence_count, 1), 3),
            "community_count": n_communities,
            "algorithms_last_computed": self._algo_last_computed.isoformat() if self._algo_last_computed else None,
        }

    # ── Relationship Ontology Validation ──────────────────────

    def validate_relationship(self, source_type: str, target_type: str,
                              relationship_type: str) -> bool:
        """Check if a relationship type is valid for given entity types."""
        ontology = RELATIONSHIP_ONTOLOGY.get(relationship_type)
        if not ontology:
            return relationship_type in ("RELATED_TO", "MENTIONS")
        src_allowed = ontology.get("source")
        tgt_allowed = ontology.get("target")
        # "unknown" type = ad-hoc node from relationship extractor, allow through
        if src_allowed and source_type != "unknown" and source_type not in src_allowed:
            return False
        if tgt_allowed and target_type != "unknown" and target_type not in tgt_allowed:
            return False
        return True

    # ── Lifecycle ─────────────────────────────────────────────

    async def close(self) -> None:
        """Close the graph store."""
        if self._db:
            self._db.close()
            self._db = None
        logger.info(
            "graph_store_closed",
            nodes=self._graph.number_of_nodes(),
            edges=self._graph.number_of_edges(),
        )

    async def health_check(self) -> dict[str, Any]:
        """Check graph store health."""
        return {
            "status": "healthy",
            "nodes": self._graph.number_of_nodes(),
            "edges": self._graph.number_of_edges(),
            "communities": len(set(self._communities.values())) if self._communities else 0,
            "algorithms_fresh": (
                self._algo_last_computed is not None
                and (datetime.utcnow() - self._algo_last_computed) < timedelta(hours=2)
            ),
        }

    async def get_node_count(self) -> int:
        return self._graph.number_of_nodes()

    async def get_edge_count(self) -> int:
        return self._graph.number_of_edges()

    # Legacy compatibility
    async def execute(self, cypher: str, params: Optional[dict] = None) -> list[dict]:
        logger.debug("legacy_cypher_ignored", query=cypher[:80])
        return []

    async def run_query(self, cypher: str, params: Optional[dict] = None) -> list[dict]:
        return await self.execute(cypher, params)
