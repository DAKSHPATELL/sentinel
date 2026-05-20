"""
SENTINEL Live Dashboard Server.
Serves the real-time intelligence dashboard with WebSocket live feeds.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse

from sentinel.config import get_config

logger = structlog.get_logger(__name__)

dashboard_app = FastAPI(title="SENTINEL Dashboard", docs_url=None, redoc_url=None)

dashboard_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Runtime dependencies (set by CLI)
_duckdb = None
_sqlite = None
_redis = None
_signal_aggregator = None
_config = None
_start_time = time.time()


def set_dashboard_deps(duckdb, sqlite, redis_client, signal_aggregator, config):
    global _duckdb, _sqlite, _redis, _signal_aggregator, _config
    _duckdb = duckdb
    _sqlite = sqlite
    _redis = redis_client
    _signal_aggregator = signal_aggregator
    _config = config


# ─── Dashboard HTML ──────────────────────────────────────────

DASHBOARD_HTML = Path(__file__).parent / "dashboard.html"


@dashboard_app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the main dashboard page."""
    if DASHBOARD_HTML.exists():
        return HTMLResponse(DASHBOARD_HTML.read_text())
    return HTMLResponse("<h1>Dashboard HTML not found</h1>")


GRAPH_HTML = Path(__file__).parent / "graph.html"

@dashboard_app.get("/graph", response_class=HTMLResponse)
async def serve_graph():
    """Serve the interactive knowledge graph visualization."""
    if GRAPH_HTML.exists():
        return HTMLResponse(GRAPH_HTML.read_text())
    return HTMLResponse("<h1>Graph visualization not found</h1>")


# ─── REST Endpoints for Dashboard ────────────────────────────


@dashboard_app.get("/api/system-snapshot")
async def system_snapshot():
    """Full system snapshot for initial dashboard load."""
    return await _build_snapshot()


@dashboard_app.get("/api/graph/pagerank")
async def graph_pagerank(n: int = 20):
    """Top entities by PageRank importance."""
    try:
        import sqlite3
        db_path = Path(_config.system.data_dir if _config else "./data") / "knowledge_graph.db"
        gdb = sqlite3.connect(str(db_path))
        rows = gdb.execute(
            """SELECT canonical_name, entity_type, mention_count,
                      COALESCE(pagerank, 0.0) as pr, COALESCE(community_id, 0) as comm
               FROM nodes ORDER BY pr DESC LIMIT ?""", (n,)
        ).fetchall()
        gdb.close()
        return [{"name": r[0], "type": r[1], "mentions": r[2],
                 "pagerank": round(r[3], 6), "community": r[4]} for r in rows]
    except Exception:
        return []


@dashboard_app.get("/api/graph/communities")
async def graph_communities():
    """Get community structure."""
    try:
        import sqlite3
        from collections import defaultdict
        db_path = Path(_config.system.data_dir if _config else "./data") / "knowledge_graph.db"
        gdb = sqlite3.connect(str(db_path))
        rows = gdb.execute(
            """SELECT community_id, canonical_name, entity_type,
                      COALESCE(pagerank, 0.0) as pr
               FROM nodes WHERE community_id > 0
               ORDER BY community_id, pr DESC"""
        ).fetchall()
        gdb.close()
        communities: dict[int, list] = defaultdict(list)
        for r in rows:
            communities[r[0]].append({"name": r[1], "type": r[2], "pagerank": round(r[3], 6)})
        return {str(k): v[:10] for k, v in communities.items()}  # Top 10 per community
    except Exception:
        return {}


@dashboard_app.get("/api/graph/entity/{entity_name}")
async def graph_entity(entity_name: str):
    """Get entity details and neighborhood."""
    try:
        import sqlite3
        db_path = Path(_config.system.data_dir if _config else "./data") / "knowledge_graph.db"
        gdb = sqlite3.connect(str(db_path))
        # Find entity
        row = gdb.execute(
            "SELECT id, canonical_name, entity_type, mention_count, aliases, description, pagerank, community_id FROM nodes WHERE canonical_name LIKE ? LIMIT 1",
            (f"%{entity_name}%",)
        ).fetchone()
        if not row:
            return {"error": "Entity not found"}
        entity_id = row[0]
        # Get edges
        edges = []
        for e in gdb.execute(
            """SELECT source_id, target_id, relationship_type,
                      COALESCE(confidence, 0.5) as conf, evidence_count
               FROM edges WHERE source_id = ? OR target_id = ?
               ORDER BY conf DESC LIMIT 50""",
            (entity_id, entity_id)
        ):
            edges.append({"source": e[0], "target": e[1], "type": e[2],
                          "confidence": round(e[3], 3), "evidence_count": e[4]})
        gdb.close()
        return {
            "id": row[0], "name": row[1], "type": row[2], "mentions": row[3],
            "aliases": json.loads(row[4]) if row[4] else [], "description": row[5],
            "pagerank": round(row[6] or 0, 6), "community": row[7],
            "edges": edges,
        }
    except Exception as e:
        return {"error": str(e)}


@dashboard_app.get("/api/graph/full")
async def graph_full(max_nodes: int = 300, min_mentions: int = 1):
    """
    Full graph data for D3.js force visualization.
    Returns nodes and edges with all metadata needed for rendering.
    """
    try:
        import sqlite3
        db_path = Path(_config.system.data_dir if _config else "./data") / "knowledge_graph.db"
        gdb = sqlite3.connect(str(db_path))

        # Get top nodes by pagerank (or mentions), limited
        nodes = []
        node_ids = set()
        for row in gdb.execute(
            """SELECT id, canonical_name, entity_type, mention_count,
                      COALESCE(pagerank, 0.0) as pr,
                      COALESCE(community_id, 0) as comm,
                      COALESCE(centrality, 0.0) as cent,
                      first_seen, last_seen
               FROM nodes
               WHERE mention_count >= ?
               ORDER BY pr DESC, mention_count DESC
               LIMIT ?""",
            (min_mentions, max_nodes),
        ):
            nodes.append({
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "mentions": row[3],
                "pagerank": round(row[4], 6),
                "community": row[5],
                "centrality": round(row[6], 6),
                "first_seen": row[7] or "",
                "last_seen": row[8] or "",
            })
            node_ids.add(row[0])

        # Get edges between visible nodes
        edges = []
        for row in gdb.execute(
            """SELECT source_id, target_id, relationship_type,
                      weight, COALESCE(confidence, 0.5) as conf,
                      COALESCE(evidence_count, 1) as ev_count,
                      first_seen, last_seen
               FROM edges"""
        ):
            if row[0] in node_ids and row[1] in node_ids:
                edges.append({
                    "source": row[0],
                    "target": row[1],
                    "type": row[2],
                    "weight": round(row[3], 2),
                    "confidence": round(row[4], 3),
                    "evidence_count": row[5],
                    "first_seen": row[6] or "",
                    "last_seen": row[7] or "",
                })

        # Get events
        events = []
        try:
            for row in gdb.execute(
                """SELECT id, event_type, title, occurred_at, confidence
                   FROM events ORDER BY occurred_at DESC LIMIT 50"""
            ):
                events.append({
                    "id": row[0], "type": row[1], "title": row[2],
                    "occurred_at": row[3] or "", "confidence": round(row[4], 3),
                })
        except Exception:
            pass  # events table might not exist yet

        # Stats
        total_nodes = gdb.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        total_edges = gdb.execute("SELECT COUNT(*) FROM edges").fetchone()[0]

        gdb.close()
        return {
            "nodes": nodes,
            "edges": edges,
            "events": events,
            "stats": {
                "total_nodes": total_nodes,
                "total_edges": total_edges,
                "visible_nodes": len(nodes),
                "visible_edges": len(edges),
            },
        }
    except Exception as e:
        return {"nodes": [], "edges": [], "events": [], "stats": {}, "error": str(e)}


@dashboard_app.get("/api/causal/simulate")
async def run_causal_simulation(
    signal_id: str,
    treatment: str,
    outcome: str,
    val: str = "inactive",
):
    """Run Pearl-based counterfactual simulation for a given signal and treatment/outcome."""
    title = "Unknown Signal"
    desc = ""
    if _duckdb:
        try:
            rows = _duckdb.query(
                "SELECT title, description FROM signal_log WHERE signal_id = ?",
                (signal_id,)
            )
            if rows:
                title = rows[0].get("title", title)
                desc = rows[0].get("description", desc)
        except Exception:
            pass

    from sentinel.intelligence.causal_simulator import CausalSimulator
    from sentinel.core.lancedb_client import LanceDBClient
    from sentinel.extraction.embedder import Embedder

    try:
        ldb = LanceDBClient()
        await ldb.connect()
        emb = Embedder()
        
        sim = CausalSimulator(lancedb_client=ldb, embedder=emb)
        result = await sim.simulate_counterfactual(
            signal_title=title,
            signal_desc=desc,
            treatment_node=treatment,
            outcome_node=outcome,
            intervention_val=val,
        )
        await ldb.close()
        return result
    except Exception as e:
        logger.error("causal_simulation_endpoint_failed", error=str(e))
        return {"status": "error", "error": str(e)}



async def _build_snapshot() -> dict[str, Any]:
    """Build a complete system state snapshot."""
    data: dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": int(time.time() - _start_time),
    }

    # Frontier stats
    try:
        if _sqlite:
            stats = await _sqlite.get_frontier_stats()
            data["frontier"] = stats
        else:
            data["frontier"] = {}
    except Exception:
        data["frontier"] = {}

    # Recent crawl activity
    try:
        if _sqlite:
            recent = await _sqlite.query(
                """SELECT url, status, domain,
                          CAST(julianday('now') - julianday(last_crawled_at) AS REAL) * 86400 as seconds_ago
                   FROM frontier
                   WHERE last_crawled_at IS NOT NULL
                   ORDER BY last_crawled_at DESC LIMIT 20"""
            )
            data["recent_crawls"] = recent
        else:
            data["recent_crawls"] = []
    except Exception:
        data["recent_crawls"] = []

    # Top domains
    try:
        if _sqlite:
            domains = await _sqlite.query(
                """SELECT domain, COUNT(*) as url_count,
                          SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed
                   FROM frontier
                   GROUP BY domain
                   ORDER BY url_count DESC
                   LIMIT 15"""
            )
            data["top_domains"] = domains
        else:
            data["top_domains"] = []
    except Exception:
        data["top_domains"] = []

    # Signals — try DuckDB first, fallback to SENTINEL API
    data["signals"] = []
    data["top_entities"] = []
    data["anomalies"] = []
    data["bursts"] = []
    data["cascades"] = []

    if _duckdb:
        try:
            data["signals"] = _duckdb.query(
                """SELECT signal_id, signal_type, priority, title, confidence,
                          detected_at, entities, source_types
                   FROM signal_log ORDER BY detected_at DESC LIMIT 30"""
            )
        except Exception:
            pass

        try:
            data["top_entities"] = _duckdb.query(
                """SELECT entity_name, SUM(mention_count) as total_mentions,
                          MAX(hour_bucket) as last_seen
                   FROM entity_mention_ts
                   GROUP BY entity_name ORDER BY total_mentions DESC LIMIT 20"""
            )
        except Exception:
            pass

        try:
            data["anomalies"] = _duckdb.query(
                "SELECT entity_name, z_score, observed_count, expected_mean, detected_at FROM anomaly_log ORDER BY detected_at DESC LIMIT 10"
            )
        except Exception:
            pass

        try:
            data["bursts"] = _duckdb.query(
                "SELECT entity_name, burst_strength, peak_rate, baseline_rate, detected_at FROM burst_log ORDER BY detected_at DESC LIMIT 10"
            )
        except Exception:
            pass

        try:
            data["cascades"] = _duckdb.query(
                "SELECT entity_name, source_count, source_types, span_hours, weighted_score, detected_at FROM cascade_log ORDER BY detected_at DESC LIMIT 10"
            )
        except Exception:
            pass
    else:
        # Fallback: pull from SENTINEL's running API (port 8000)
        try:
            import httpx
            async with httpx.AsyncClient(timeout=3) as client:
                resp = await client.get("http://localhost:8000/api/signals?limit=30")
                if resp.status_code == 200:
                    data["signals"] = resp.json()
                resp = await client.get("http://localhost:8000/api/entities?limit=20")
                if resp.status_code == 200:
                    data["top_entities"] = resp.json()
                resp = await client.get("http://localhost:8000/api/cascades?limit=10")
                if resp.status_code == 200:
                    data["cascades"] = resp.json()
        except Exception:
            pass  # SENTINEL API not available either — dashboard shows what it can

    # Knowledge graph stats (TPE-KG)
    try:
        graph_db_path = Path(_config.system.data_dir if _config else "./data") / "knowledge_graph.db"
        if graph_db_path.exists():
            import sqlite3
            gdb = sqlite3.connect(str(graph_db_path))
            node_count = gdb.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            edge_count = gdb.execute("SELECT COUNT(*) FROM edges").fetchone()[0]

            # Top nodes by PageRank (graph importance) with fallback to mentions
            top_nodes = []
            for row in gdb.execute(
                """SELECT canonical_name, entity_type, mention_count,
                          COALESCE(pagerank, 0.0) as pr, COALESCE(community_id, 0) as comm
                   FROM nodes ORDER BY pr DESC, mention_count DESC LIMIT 20"""
            ):
                top_nodes.append({
                    "name": row[0], "type": row[1], "mentions": row[2],
                    "pagerank": round(row[3], 6), "community": row[4],
                })

            # Community count
            try:
                n_communities = gdb.execute(
                    "SELECT COUNT(DISTINCT community_id) FROM nodes WHERE community_id > 0"
                ).fetchone()[0]
            except Exception:
                n_communities = 0

            # Average edge confidence
            try:
                avg_conf = gdb.execute(
                    "SELECT AVG(COALESCE(confidence, 0.5)) FROM edges"
                ).fetchone()[0] or 0.0
            except Exception:
                avg_conf = 0.0

            # Event count
            try:
                event_count = gdb.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            except Exception:
                event_count = 0

            gdb.close()
            data["graph"] = {
                "node_count": node_count,
                "edge_count": edge_count,
                "top_nodes": top_nodes,
                "community_count": n_communities,
                "avg_confidence": round(avg_conf, 3),
                "event_count": event_count,
            }
        else:
            data["graph"] = {"node_count": 0, "edge_count": 0, "top_nodes": []}
    except Exception:
        data["graph"] = {"node_count": 0, "edge_count": 0, "top_nodes": []}

    # LanceDB stats
    try:
        from sentinel.core.lancedb_client import LanceDBClient
        lance_path = Path(_config.lancedb.path if _config else "./data/lance")
        if lance_path.exists():
            import lancedb
            ldb = lancedb.connect(str(lance_path))
            lance_tables = {}
            for tname in ldb.table_names():
                try:
                    t = ldb.open_table(tname)
                    lance_tables[tname] = t.count_rows()
                except Exception:
                    lance_tables[tname] = 0
            data["lancedb"] = lance_tables
        else:
            data["lancedb"] = {}
    except Exception:
        data["lancedb"] = {}

    # Redis stats
    try:
        if _redis:
            info = await _redis.client.info("memory")
            data["redis"] = {
                "used_memory_mb": round(info.get("used_memory", 0) / 1048576, 1),
                "connected": True,
            }
        else:
            data["redis"] = {"connected": False}
    except Exception:
        data["redis"] = {"connected": False}

    # Source health (from signal aggregator)
    data["sources"] = {
        "hackernews": {"enabled": True, "interval": 120},
        "github": {"enabled": True, "interval": 300},
        "arxiv": {"enabled": True, "interval": 3600},
        "rss": {"enabled": True, "interval": 1800},
        "ct_monitor": {"enabled": True, "interval": 3600},
        "patents": {"enabled": True, "interval": 43200},
        "commoncrawl": {"enabled": True, "interval": 7200},
        "sitemap": {"enabled": True, "interval": 3600},
        "web_crawler": {"enabled": True, "interval": 0},
    }

    # System resources
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        data["system"] = {
            "cpu_percent": proc.cpu_percent(),
            "memory_mb": round(proc.memory_info().rss / 1048576),
            "threads": proc.num_threads(),
        }
    except Exception:
        data["system"] = {"cpu_percent": 0, "memory_mb": 0, "threads": 0}

    return data


# ─── WebSocket Live Feed ─────────────────────────────────────


@dashboard_app.websocket("/ws/live")
async def websocket_live_feed(websocket: WebSocket):
    """
    WebSocket endpoint that pushes live system state every 2 seconds.
    This is what makes the dashboard feel alive.
    """
    await websocket.accept()
    logger.info("dashboard_ws_connected")

    try:
        while True:
            snapshot = await _build_snapshot()

            # Serialize datetimes
            def serialize(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return str(obj)

            await websocket.send_text(json.dumps(snapshot, default=serialize))
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        logger.info("dashboard_ws_disconnected")
    except Exception as e:
        logger.error("dashboard_ws_error", error=str(e))
