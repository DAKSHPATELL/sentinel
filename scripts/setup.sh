#!/usr/bin/env bash
set -euo pipefail

echo "=== SENTINEL Setup Script ==="

# Install Python dependencies
echo "[1/5] Installing Python dependencies..."
pip install -e ".[dev,full]"

# Pull and start Docker containers
echo "[2/5] Starting Docker containers..."
docker compose pull
docker compose up -d

# Download spaCy model
echo "[3/5] Downloading spaCy model..."
python -m spacy download en_core_web_trf || echo "WARNING: spaCy model download failed. Install manually: python -m spacy download en_core_web_trf"

# Install Playwright browsers
echo "[4/5] Installing Playwright chromium..."
playwright install chromium || echo "WARNING: Playwright install failed. Install manually: playwright install chromium"

# Create data directories
echo "[5/5] Creating data directories..."
mkdir -p data/{lance,duckdb,crawl_state,models,cache/html}

echo ""
echo "=== Setup Complete ==="
echo "Next steps:"
echo "  1. Pull Ollama model: ollama pull qwen2.5:7b-instruct-q4_K_M"
echo "  2. Start Ollama: ollama serve"
echo "  3. Start SENTINEL: sentinel start"
