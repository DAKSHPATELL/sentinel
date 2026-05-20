"""
Standalone Knowledge Graph Viewer.

Serves the interactive graph visualization from SQLite only.
No DuckDB dependency — runs alongside SENTINEL without conflicts.

Usage:
    python -m sentinel.interface.graph_server
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(title="SENTINEL Knowledge Graph Viewer")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DATA_DIR = Path("./data")
GRAPH_DB = DATA_DIR / "knowledge_graph.db"
GRAPH_HTML = Path(__file__).parent / "graph.html"


def _get_db():
    """Get a read-only SQLite connection to the knowledge graph."""
    if not GRAPH_DB.exists():
        return None
    return sqlite3.connect(f"file:{GRAPH_DB}?mode=ro", uri=True)


@app.get("/", response_class=HTMLResponse)
async def serve_graph():
    if GRAPH_HTML.exists():
        return HTMLResponse(GRAPH_HTML.read_text())
    return HTMLResponse("<h1>graph.html not found</h1>")


@app.get("/graph", response_class=HTMLResponse)
async def serve_graph_alt():
    return await serve_graph()


@app.get("/api/graph/full")
async def graph_full(max_nodes: int = 300, min_mentions: int = 1):
    db = _get_db()
    if not db:
        return {"nodes": [], "edges": [], "events": [], "stats": {"error": "No knowledge_graph.db found"}}
    try:
        # Edge-first approach: get ALL edges, then ensure their nodes are included
        edges = []
        edge_node_ids = set()
        for row in db.execute(
            """SELECT source_id, target_id, relationship_type,
                      weight, COALESCE(confidence, 0.5) as conf,
                      COALESCE(evidence_count, 1) as ev_count,
                      first_seen, last_seen
               FROM edges"""
        ):
            edges.append({
                "source": row[0], "target": row[1], "type": row[2],
                "weight": round(row[3], 2), "confidence": round(row[4], 3),
                "evidence_count": row[5],
                "first_seen": row[6] or "", "last_seen": row[7] or "",
            })
            edge_node_ids.add(row[0])
            edge_node_ids.add(row[1])

        # Fetch all nodes referenced by edges + top PageRank nodes
        nodes = []
        node_ids = set()
        node_query = """SELECT id, canonical_name, entity_type, mention_count,
                      COALESCE(pagerank, 0.0) as pr,
                      COALESCE(community_id, 0) as comm,
                      COALESCE(centrality, 0.0) as cent,
                      first_seen, last_seen
               FROM nodes
               WHERE mention_count >= ?
               ORDER BY pr DESC, mention_count DESC
               LIMIT ?"""
        for row in db.execute(node_query, (min_mentions, max_nodes)):
            nodes.append({
                "id": row[0], "name": row[1], "type": row[2],
                "mentions": row[3], "pagerank": round(row[4], 6),
                "community": row[5], "centrality": round(row[6], 6),
                "first_seen": row[7] or "", "last_seen": row[8] or "",
            })
            node_ids.add(row[0])

        # Add any edge-referenced nodes not yet included
        missing_ids = edge_node_ids - node_ids
        if missing_ids:
            placeholders = ",".join("?" for _ in missing_ids)
            for row in db.execute(
                f"""SELECT id, canonical_name, entity_type, mention_count,
                           COALESCE(pagerank, 0.0) as pr,
                           COALESCE(community_id, 0) as comm,
                           COALESCE(centrality, 0.0) as cent,
                           first_seen, last_seen
                    FROM nodes WHERE id IN ({placeholders})""",
                list(missing_ids),
            ):
                nodes.append({
                    "id": row[0], "name": row[1], "type": row[2],
                    "mentions": row[3], "pagerank": round(row[4], 6),
                    "community": row[5], "centrality": round(row[6], 6),
                    "first_seen": row[7] or "", "last_seen": row[8] or "",
                })
                node_ids.add(row[0])

        # Filter edges to only those with both nodes present
        edges = [e for e in edges if e["source"] in node_ids and e["target"] in node_ids]

        events = []
        try:
            for row in db.execute(
                "SELECT id, event_type, title, occurred_at, confidence FROM events ORDER BY occurred_at DESC LIMIT 50"
            ):
                events.append({"id": row[0], "type": row[1], "title": row[2],
                                "occurred_at": row[3] or "", "confidence": round(row[4], 3)})
        except Exception:
            pass

        total_nodes = db.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        total_edges = db.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        db.close()

        # Edge type distribution
        edge_types = {}
        for e in edges:
            t = e["type"]
            edge_types[t] = edge_types.get(t, 0) + 1

        # Top hub nodes (most connected)
        hub_counts = {}
        for e in edges:
            hub_counts[e["source"]] = hub_counts.get(e["source"], 0) + 1
            hub_counts[e["target"]] = hub_counts.get(e["target"], 0) + 1
        top_hubs = sorted(hub_counts.items(), key=lambda x: -x[1])[:10]
        hub_names = []
        for hid, count in top_hubs:
            node = next((n for n in nodes if n["id"] == hid), None)
            if node:
                hub_names.append({"name": node["name"], "type": node["type"], "connections": count})

        return {
            "nodes": nodes, "edges": edges, "events": events,
            "stats": {
                "total_nodes": total_nodes, "total_edges": total_edges,
                "visible_nodes": len(nodes), "visible_edges": len(edges),
                "edge_types": edge_types,
                "top_hubs": hub_names,
            },
        }
    except Exception as e:
        db.close()
        return {"nodes": [], "edges": [], "events": [], "stats": {"error": str(e)}}


@app.get("/api/graph/pagerank")
async def graph_pagerank(n: int = 20):
    db = _get_db()
    if not db:
        return []
    rows = db.execute(
        """SELECT canonical_name, entity_type, mention_count,
                  COALESCE(pagerank, 0.0) as pr, COALESCE(community_id, 0) as comm
           FROM nodes ORDER BY pr DESC LIMIT ?""", (n,)
    ).fetchall()
    db.close()
    return [{"name": r[0], "type": r[1], "mentions": r[2],
             "pagerank": round(r[3], 6), "community": r[4]} for r in rows]


@app.get("/api/system-snapshot")
async def system_snapshot():
    """Minimal snapshot for the graph.html initial fetch."""
    db = _get_db()
    if not db:
        return {"graph": {"node_count": 0, "edge_count": 0, "top_nodes": []}}
    try:
        node_count = db.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        edge_count = db.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        db.close()
        return {"graph": {"node_count": node_count, "edge_count": edge_count, "top_nodes": []},
                "frontier": {}, "recent_crawls": [], "top_domains": [],
                "signals": [], "top_entities": [], "anomalies": [], "bursts": [], "cascades": [],
                "lancedb": {}, "redis": {}, "sources": {}, "system": {}}
    except Exception:
        db.close()
        return {"graph": {"node_count": 0, "edge_count": 0, "top_nodes": []}}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8051, log_level="info")
