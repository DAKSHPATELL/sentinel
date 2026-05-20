#!/bin/bash
# ──────────────────────────────────────────────────────────
# SENTINEL Daemon Manager
# Manages the launchd-based 24/7 background service
# Uses sentinel CLI for graceful start/stop lifecycle
# ──────────────────────────────────────────────────────────

set -euo pipefail

PLIST_NAME="com.sentinel.daemon"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLIST_SRC="${PROJECT_DIR}/${PLIST_NAME}.plist"
PLIST_DST="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"
LOG_DIR="${PROJECT_DIR}/data/logs"
PID_FILE="${PROJECT_DIR}/data/sentinel.pid"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

usage() {
    echo -e "${CYAN}╔══════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   SENTINEL — Daemon Manager          ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════╝${NC}"
    echo ""
    echo "Usage: $0 {install|uninstall|start|stop|restart|status|logs}"
    echo ""
    echo "Commands:"
    echo "  install    Install SENTINEL as a background daemon (starts on login)"
    echo "  uninstall  Stop and remove the daemon completely"
    echo "  start      Start the daemon"
    echo "  stop       Gracefully stop SENTINEL (drains work, flushes data)"
    echo "  restart    Stop then start"
    echo "  status     Show daemon status with PID and resource usage"
    echo "  logs       Tail live daemon logs"
    echo ""
    echo "Direct CLI (no daemon):"
    echo "  sentinel start           Start in foreground"
    echo "  sentinel stop             Gracefully stop a running instance"
    echo "  sentinel stop --force     Force kill immediately"
    echo "  sentinel status           Show system status"
    exit 1
}

ensure_log_dir() {
    mkdir -p "$LOG_DIR"
}

install_daemon() {
    echo -e "${CYAN}Installing SENTINEL daemon...${NC}"
    ensure_log_dir

    # Stop any existing instance first
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
            echo -e "  Stopping existing instance (PID $PID)..."
            sentinel stop 2>/dev/null || kill "$PID" 2>/dev/null || true
            sleep 2
        fi
    fi

    # Unload old plist if exists
    launchctl unload "$PLIST_DST" 2>/dev/null || true

    # Copy plist to LaunchAgents
    cp "$PLIST_SRC" "$PLIST_DST"

    # Load the daemon
    launchctl load "$PLIST_DST"

    echo -e "${GREEN}✓ SENTINEL daemon installed and started${NC}"
    echo -e "  Logs:    ${LOG_DIR}/sentinel.stdout.log"
    echo -e "  Errors:  ${LOG_DIR}/sentinel.stderr.log"
    echo -e "  PID:     ${PID_FILE}"
    echo ""
    echo -e "  SENTINEL will now:"
    echo -e "    ${BOLD}•${NC} Run 24/7 in the background"
    echo -e "    ${BOLD}•${NC} Auto-restart on crash"
    echo -e "    ${BOLD}•${NC} Start automatically on login"
    echo -e "    ${BOLD}•${NC} Continuously crawl the web"
    echo ""
    echo -e "  To stop:  ${BOLD}sentinel stop${NC}  or  ${BOLD}$0 stop${NC}"
}

uninstall_daemon() {
    echo -e "${YELLOW}Uninstalling SENTINEL daemon...${NC}"

    # Gracefully stop first
    stop_daemon

    if [ -f "$PLIST_DST" ]; then
        launchctl unload "$PLIST_DST" 2>/dev/null || true
        rm -f "$PLIST_DST"
        echo -e "${GREEN}✓ SENTINEL daemon uninstalled${NC}"
    else
        echo -e "${YELLOW}Daemon was not installed${NC}"
    fi
}

start_daemon() {
    if [ ! -f "$PLIST_DST" ]; then
        echo -e "${RED}Daemon not installed. Run '$0 install' first.${NC}"
        exit 1
    fi

    # Check if already running
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
            echo -e "${YELLOW}SENTINEL is already running (PID $PID)${NC}"
            return
        fi
    fi

    ensure_log_dir
    launchctl start "$PLIST_NAME"
    sleep 3

    # Verify it started
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE" 2>/dev/null)
        echo -e "${GREEN}✓ SENTINEL daemon started (PID $PID)${NC}"
    else
        echo -e "${GREEN}✓ SENTINEL daemon starting...${NC}"
    fi
}

stop_daemon() {
    echo -e "${CYAN}Stopping SENTINEL gracefully...${NC}"

    # Method 1: Use sentinel CLI (sends SIGTERM, waits for graceful shutdown)
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
            echo -e "  Sending SIGTERM to PID $PID..."
            kill -TERM "$PID" 2>/dev/null || true

            # Wait up to 30 seconds for graceful shutdown
            for i in $(seq 1 30); do
                if ! kill -0 "$PID" 2>/dev/null; then
                    echo -e "${GREEN}  ✓ SENTINEL stopped gracefully (${i}s)${NC}"
                    rm -f "$PID_FILE"
                    return
                fi
                sleep 1
            done

            # Force kill if still running
            echo -e "${YELLOW}  Graceful shutdown timed out — force killing...${NC}"
            kill -9 "$PID" 2>/dev/null || true
            sleep 1
            rm -f "$PID_FILE"
            echo -e "${YELLOW}  ✓ SENTINEL force-killed${NC}"
            return
        fi
    fi

    # Method 2: launchctl stop (fallback)
    launchctl stop "$PLIST_NAME" 2>/dev/null || true
    echo -e "${YELLOW}  ✓ SENTINEL stopped${NC}"
}

restart_daemon() {
    stop_daemon
    sleep 2
    start_daemon
}

show_status() {
    echo -e "${CYAN}╔══════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   SENTINEL — System Status           ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════╝${NC}"
    echo ""

    # Daemon installation
    if [ -f "$PLIST_DST" ]; then
        echo -e "  Daemon:    ${GREEN}Installed${NC}"
    else
        echo -e "  Daemon:    ${RED}Not installed${NC}"
    fi

    # Process status
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
            echo -e "  Process:   ${GREEN}Running (PID $PID)${NC}"

            # Resource usage
            if command -v ps &>/dev/null; then
                CPU=$(ps -p "$PID" -o %cpu= 2>/dev/null | xargs)
                MEM=$(ps -p "$PID" -o %mem= 2>/dev/null | xargs)
                RSS=$(ps -p "$PID" -o rss= 2>/dev/null | xargs)
                ELAPSED=$(ps -p "$PID" -o etime= 2>/dev/null | xargs)
                RSS_MB=$((${RSS:-0} / 1024))
                echo -e "  CPU:       ${CPU:-?}%"
                echo -e "  Memory:    ${RSS_MB} MB (${MEM:-?}%)"
                echo -e "  Uptime:    ${ELAPSED:-?}"
            fi
        else
            echo -e "  Process:   ${YELLOW}Stale PID $PID (not running)${NC}"
            rm -f "$PID_FILE"
        fi
    else
        echo -e "  Process:   ${RED}Not running${NC}"
    fi

    # Redis
    if redis-cli ping &>/dev/null; then
        REDIS_MEM=$(redis-cli info memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '[:space:]')
        echo -e "  Redis:     ${GREEN}Running${NC} (${REDIS_MEM:-?})"
    else
        echo -e "  Redis:     ${RED}Not running${NC}"
    fi

    # Logs
    echo ""
    if [ -f "$LOG_DIR/sentinel.stdout.log" ]; then
        STDOUT_SIZE=$(du -h "$LOG_DIR/sentinel.stdout.log" | cut -f1)
        echo -e "  Stdout:    $STDOUT_SIZE"
    fi
    if [ -f "$LOG_DIR/sentinel.stderr.log" ]; then
        STDERR_SIZE=$(du -h "$LOG_DIR/sentinel.stderr.log" | cut -f1)
        echo -e "  Stderr:    $STDERR_SIZE"
    fi

    # Data directory
    if [ -d "${PROJECT_DIR}/data" ]; then
        DATA_SIZE=$(du -sh "${PROJECT_DIR}/data" 2>/dev/null | cut -f1)
        echo -e "  Data dir:  $DATA_SIZE"
    fi
}

tail_logs() {
    ensure_log_dir
    echo -e "${CYAN}SENTINEL Logs (Ctrl+C to stop)${NC}"
    echo ""
    tail -f "$LOG_DIR/sentinel.stdout.log" "$LOG_DIR/sentinel.stderr.log" 2>/dev/null || \
        echo -e "${YELLOW}No log files yet. Start the daemon first.${NC}"
}

# ── Main ─────────────────────────────────────────────
case "${1:-}" in
    install)   install_daemon ;;
    uninstall) uninstall_daemon ;;
    start)     start_daemon ;;
    stop)      stop_daemon ;;
    restart)   restart_daemon ;;
    status)    show_status ;;
    logs)      tail_logs ;;
    *)         usage ;;
esac
