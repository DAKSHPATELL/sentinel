#!/bin/bash
# SENTINEL Auto-Deploy Pipeline
# Exports graph data from SQLite and deploys to Vercel
# Run via launchd every 5 minutes

set -euo pipefail

SENTINEL_DIR="/Users/dakshpatel/infinite_research/sentinel"
WEBSITE_DIR="/Users/dakshpatel/konform_web"
VENV="$SENTINEL_DIR/.venv/bin/python3"
DATA_OUT="$WEBSITE_DIR/public/sentinel/data.json"
STATUS_FILE="$WEBSITE_DIR/public/sentinel/status.json"
LOG_FILE="$SENTINEL_DIR/logs/deploy.log"
VERCEL_BIN="/Users/dakshpatel/.npm-global/bin/vercel"
NODE_BIN="/opt/homebrew/bin"

export PATH="$NODE_BIN:$PATH"

mkdir -p "$(dirname "$LOG_FILE")"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"
}

log "=== Deploy cycle starting ==="

# Check if SENTINEL is running
SENTINEL_PID=$(pgrep -f "sentinel.cli start" 2>/dev/null || true)
if [ -z "$SENTINEL_PID" ]; then
  SENTINEL_STATUS="stopped"
  log "SENTINEL is not running"
else
  SENTINEL_STATUS="running"
  log "SENTINEL running (PID: $SENTINEL_PID)"
fi

# Export graph data from SQLite
DB_PATH="$SENTINEL_DIR/data/knowledge_graph.db"
if [ ! -f "$DB_PATH" ] || [ ! -s "$DB_PATH" ]; then
  log "ERROR: knowledge_graph.db not found or empty"
  exit 1
fi

log "Exporting graph data..."
"$VENV" -c "
import sqlite3, json, os, sys

db = sqlite3.connect('$DB_PATH')
db.row_factory = sqlite3.Row

nodes_raw = db.execute('SELECT * FROM nodes ORDER BY pagerank DESC').fetchall()
nodes = []
for n in nodes_raw:
    nodes.append({
        'id': n['id'],
        'canonical_name': n['canonical_name'],
        'entity_type': n['entity_type'],
        'mention_count': n['mention_count'],
        'pagerank': n['pagerank'],
        'community_id': n['community_id'] if 'community_id' in n.keys() else None,
        'first_seen': n['first_seen'] if 'first_seen' in n.keys() else None,
        'last_seen': n['last_seen'] if 'last_seen' in n.keys() else None,
    })

edges_raw = db.execute('SELECT * FROM edges').fetchall()
edges = []
for e in edges_raw:
    edges.append({
        'source_id': e['source_id'],
        'target_id': e['target_id'],
        'relationship_type': e['relationship_type'],
        'confidence': e['confidence'] if 'confidence' in e.keys() else 0.5,
        'evidence_count': e['evidence_count'] if 'evidence_count' in e.keys() else 1,
        'first_seen': e['first_seen'] if 'first_seen' in e.keys() else None,
        'last_seen': e['last_seen'] if 'last_seen' in e.keys() else None,
    })

edge_types = {}
for e in edges:
    t = e['relationship_type']
    edge_types[t] = edge_types.get(t, 0) + 1

hub_counts = {}
for e in edges:
    hub_counts[e['source_id']] = hub_counts.get(e['source_id'], 0) + 1
    hub_counts[e['target_id']] = hub_counts.get(e['target_id'], 0) + 1

node_map = {n['id']: n['canonical_name'] for n in nodes}
top_hubs = sorted(hub_counts.items(), key=lambda x: -x[1])[:15]
top_hubs_named = [{'name': node_map.get(h[0], h[0]), 'connections': h[1]} for h in top_hubs]
community_ids = set(n.get('community_id') for n in nodes if n.get('community_id') is not None)

from datetime import datetime, timezone
data = {
    'nodes': nodes,
    'edges': edges,
    'stats': {
        'total_nodes': len(nodes),
        'total_edges': len(edges),
        'communities': len(community_ids),
        'edge_types': edge_types,
        'top_hubs': top_hubs_named,
    },
    'exported_at': datetime.now(timezone.utc).isoformat(),
    'sentinel_status': '$SENTINEL_STATUS',
    'sentinel_pid': '$SENTINEL_PID' if '$SENTINEL_PID' else None,
}

with open('$DATA_OUT', 'w') as f:
    json.dump(data, f)

print(f'Exported {len(nodes)} nodes, {len(edges)} edges')
db.close()
" 2>&1 | while read line; do log "$line"; done

# Write status file
cat > "$STATUS_FILE" << STATUSEOF
{
  "sentinel_status": "$SENTINEL_STATUS",
  "sentinel_pid": "$SENTINEL_PID",
  "last_export": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "next_export": "$(date -u -v+5M '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u '+%Y-%m-%dT%H:%M:%SZ')"
}
STATUSEOF

log "Data exported, deploying to Vercel..."

# Deploy to Vercel
cd "$WEBSITE_DIR"
DEPLOY_OUTPUT=$("$VERCEL_BIN" --prod --yes 2>&1 | tail -5)
log "Deploy output: $DEPLOY_OUTPUT"

log "=== Deploy cycle complete ==="
