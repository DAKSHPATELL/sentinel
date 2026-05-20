# SENTINEL

**Autonomous Web Intelligence & Signal Detection System**

SENTINEL continuously crawls the web across multiple sources, extracts structured intelligence, builds a knowledge graph, detects anomalies and emerging signals, and alerts you to discoveries before they hit mainstream radar.

## Quick Start

```bash
# Prerequisites
brew install redis neo4j ollama docker
ollama pull qwen2.5:7b-instruct-q4_K_M

# Setup
cd sentinel
bash scripts/setup.sh

# Run
sentinel start
sentinel status
```

## Architecture

See `SENTINEL_PRD.md` for full architecture documentation.

## Development

```bash
pip install -e ".[dev]"
make test
make lint
```
