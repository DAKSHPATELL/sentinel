#!/bin/bash
# ──────────────────────────────────────────────────────────
# SENTINEL — Launch Script
# Uses the project's virtual environment automatically.
# ──────────────────────────────────────────────────────────
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="${SCRIPT_DIR}/.venv"

if [ ! -d "$VENV" ]; then
    echo "Error: Virtual environment not found at ${VENV}"
    echo "Run: python3.13 -m venv .venv && .venv/bin/pip install -e '.[full]'"
    exit 1
fi

# Activate venv
export PATH="${VENV}/bin:${PATH}"

# Ensure Ollama is on PATH
export PATH="/usr/local/bin:${PATH}"

# Change to project directory
cd "$SCRIPT_DIR"

# Run sentinel CLI with all arguments
exec python -m sentinel.cli "$@"
