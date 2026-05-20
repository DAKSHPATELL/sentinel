"""
SENTINEL REST API.
FastAPI server providing endpoints for signals, entities, graph, and system health.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import structlog
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from sentinel.config import get_config

logger = structlog.get_logger(__name__)

app = FastAPI(
    title="SENTINEL Intelligence API",
    description="Autonomous Web Intelligence & Signal Detection System",
    version="1.0.0",
)

# CORS
config = get_config()
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.interface.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# These get set by the CLI when the API server starts
_duckdb = None
_lance = None
_neo4j = None
_signal_aggregator = None
_report_generator = None


def set_dependencies(duckdb, lance, neo4j, signal_aggregator, report_generator):
    """Inject runtime dependencies from CLI startup."""
    global _duckdb, _lance, _neo4j, _signal_aggregator, _report_generator
    _duckdb = duckdb
    _lance = lance
    _neo4j = neo4j
    _signal_aggregator = signal_aggregator
    _report_generator = report_generator


# ─── Health ─────────────────────────────────────────────


@app.get("/health")
async def health():
    """System health check."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": config.system.version,
    }


# ─── Signals ─────────────────────────────────────────────


@app.get("/api/signals")
async def get_signals(
    limit: int = Query(default=50, ge=1, le=500),
    priority: Optional[str] = Query(default=None),
    signal_type: Optional[str] = Query(default=None),
):
    """Get recent signals with optional filtering."""
    if not _duckdb:
        raise HTTPException(503, "Database not initialized")

    try:
        query = "SELECT * FROM signal_log WHERE 1=1"
        params = []

        if priority:
            query += " AND priority = ?"
            params.append(priority)
        if signal_type:
            query += " AND signal_type = ?"
            params.append(signal_type)

        query += " ORDER BY detected_at DESC LIMIT ?"
        params.append(limit)

        return _duckdb.query(query, tuple(params))
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/signals/{signal_id}")
async def get_signal(signal_id: str):
    """Get a specific signal by ID."""
    if not _duckdb:
        raise HTTPException(503, "Database not initialized")

    try:
        rows = _duckdb.query(
            "SELECT * FROM signal_log WHERE signal_id = ?", (signal_id,)
        )
        if not rows:
            raise HTTPException(404, "Signal not found")
        return rows[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/signals/{signal_id}/feedback")
async def signal_feedback(signal_id: str, useful: bool):
    """Mark a signal as useful or not (human feedback loop)."""
    if not _signal_aggregator:
        raise HTTPException(503, "Signal aggregator not initialized")

    _signal_aggregator.mark_signal_useful(signal_id, useful)
    return {"status": "ok", "signal_id": signal_id, "useful": useful}


# ─── Entities ─────────────────────────────────────────────


@app.get("/api/entities")
async def get_entities(
    limit: int = Query(default=50, ge=1, le=500),
    entity_type: Optional[str] = Query(default=None),
):
    """Get top entities by mention count."""
    if not _duckdb:
        raise HTTPException(503, "Database not initialized")

    try:
        query = """
            SELECT entity_name, SUM(mention_count) as total_mentions,
                   MAX(hour_bucket) as last_seen
            FROM entity_mention_ts
        """
        params = []
        query += " GROUP BY entity_name ORDER BY total_mentions DESC LIMIT ?"
        params.append(limit)

        return _duckdb.query(query, tuple(params))
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/entities/{entity_name}/timeline")
async def entity_timeline(entity_name: str, hours: int = Query(default=72, ge=1, le=720)):
    """Get hourly mention timeline for an entity."""
    if not _duckdb:
        raise HTTPException(503, "Database not initialized")

    try:
        return _duckdb.query(
            f"""
            SELECT hour_bucket, mention_count, source_types, avg_relevance
            FROM entity_mention_ts
            WHERE entity_name = ?
              AND hour_bucket >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
            ORDER BY hour_bucket ASC
            """,
            (entity_name,),
        )
    except Exception as e:
        raise HTTPException(500, str(e))


# ─── Graph ─────────────────────────────────────────────


@app.get("/api/graph/stats")
async def graph_stats():
    """Get knowledge graph statistics."""
    if not _neo4j:
        return {"status": "neo4j_not_connected", "node_count": 0, "edge_count": 0}

    try:
        from sentinel.knowledge.graph_builder import GraphBuilder
        # Use a temporary builder just for stats
        builder = GraphBuilder(_neo4j, _lance)
        return await builder.get_graph_stats()
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/graph/entity/{entity_id}")
async def entity_neighbors(entity_id: str, hops: int = Query(default=2, ge=1, le=4)):
    """Get entity neighborhood in the knowledge graph."""
    if not _neo4j:
        raise HTTPException(503, "Neo4j not connected")

    try:
        from sentinel.knowledge.graph_builder import GraphBuilder
        builder = GraphBuilder(_neo4j, _lance)
        return await builder.get_entity_neighbors(entity_id, hops)
    except Exception as e:
        raise HTTPException(500, str(e))


# ─── Reports ─────────────────────────────────────────────


@app.get("/api/reports/generate")
async def generate_report(hours: int = Query(default=24, ge=1, le=168)):
    """Generate an intelligence report on demand."""
    if not _report_generator:
        raise HTTPException(503, "Report generator not initialized")

    try:
        report = _report_generator.generate(hours)
        return {"report": report, "hours": hours}
    except Exception as e:
        raise HTTPException(500, str(e))


# ─── Cascades ─────────────────────────────────────────────


@app.get("/api/cascades")
async def get_cascades(limit: int = Query(default=20, ge=1, le=100)):
    """Get recent cross-domain cascade events."""
    if not _duckdb:
        raise HTTPException(503, "Database not initialized")

    try:
        return _duckdb.query(
            """
            SELECT * FROM cascade_log
            ORDER BY detected_at DESC
            LIMIT ?
            """,
            (limit,),
        )
    except Exception as e:
        raise HTTPException(500, str(e))


# ─── Predictions ─────────────────────────────────────────


@app.get("/api/predictions")
async def get_predictions(limit: int = Query(default=20, ge=1, le=100)):
    """Get recent predictions from the predictive crawler."""
    if not _duckdb:
        raise HTTPException(503, "Database not initialized")

    try:
        return _duckdb.query(
            """
            SELECT * FROM predictions
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
    except Exception as e:
        raise HTTPException(500, str(e))
