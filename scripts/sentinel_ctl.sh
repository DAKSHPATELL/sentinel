#!/bin/bash
# SENTINEL Control Script
# Usage: sentinel_ctl.sh [start|stop|status|deploy]

set -euo pipefail

SENTINEL_DIR="/Users/dakshpatel/infinite_research/sentinel"
VENV_PYTHON="$SENTINEL_DIR/.venv/bin/python3"
PID_FILE="$SENTINEL_DIR/data/sentinel.pid"
PLIST_NAME="com.mach13.sentinel-deploy"
PLIST_PATH="$HOME/Library/LaunchAgents/$PLIST_NAME.plist"

case "${1:-status}" in
  start)
    # Start SENTINEL pipeline
    EXISTING_PID=$(pgrep -f "sentinel.cli start" 2>/dev/null || true)
    if [ -n "$EXISTING_PID" ]; then
      echo "SENTINEL already running (PID: $EXISTING_PID)"
    else
      echo "Starting SENTINEL pipeline..."
      cd "$SENTINEL_DIR"
      # Use caffeinate to prevent sleep while SENTINEL runs
      caffeinate -s -w $$ &
      nohup "$VENV_PYTHON" -m sentinel.cli start > "$SENTINEL_DIR/logs/sentinel.log" 2>&1 &
      SENTINEL_PID=$!
      echo "$SENTINEL_PID" > "$PID_FILE"
      echo "SENTINEL started (PID: $SENTINEL_PID)"
    fi

    # Start the auto-deploy cycle
    if launchctl list | grep -q "$PLIST_NAME"; then
      echo "Auto-deploy already scheduled"
    else
      echo "Scheduling auto-deploy (every 5 minutes)..."
      launchctl load "$PLIST_PATH" 2>/dev/null || echo "Load plist first: sentinel_ctl.sh install"
    fi
    ;;

  stop)
    # Stop SENTINEL pipeline
    EXISTING_PID=$(pgrep -f "sentinel.cli start" 2>/dev/null || true)
    if [ -n "$EXISTING_PID" ]; then
      echo "Stopping SENTINEL (PID: $EXISTING_PID)..."
      kill "$EXISTING_PID" 2>/dev/null || true
      sleep 2
      # Force kill if still running
      kill -9 "$EXISTING_PID" 2>/dev/null || true
      rm -f "$PID_FILE"
      echo "SENTINEL stopped"
    else
      echo "SENTINEL is not running"
    fi

    # Run one final export to update status on website
    echo "Running final export to update website status..."
    bash "$SENTINEL_DIR/scripts/export_and_deploy.sh" 2>/dev/null || true
    ;;

  status)
    EXISTING_PID=$(pgrep -f "sentinel.cli start" 2>/dev/null || true)
    if [ -n "$EXISTING_PID" ]; then
      echo "SENTINEL: RUNNING (PID: $EXISTING_PID)"
      CPU=$(ps -p "$EXISTING_PID" -o %cpu= 2>/dev/null || echo "?")
      MEM=$(ps -p "$EXISTING_PID" -o %mem= 2>/dev/null || echo "?")
      echo "  CPU: ${CPU}% | MEM: ${MEM}%"
    else
      echo "SENTINEL: STOPPED"
    fi

    if launchctl list 2>/dev/null | grep -q "$PLIST_NAME"; then
      echo "Auto-deploy: ACTIVE (every 5 minutes)"
    else
      echo "Auto-deploy: INACTIVE"
    fi
    ;;

  deploy)
    echo "Running manual deploy..."
    bash "$SENTINEL_DIR/scripts/export_and_deploy.sh"
    ;;

  install)
    echo "Installing launchd plist..."
    mkdir -p "$HOME/Library/LaunchAgents"
    cat > "$PLIST_PATH" << 'PLISTEOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.mach13.sentinel-deploy</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/dakshpatel/infinite_research/sentinel/scripts/export_and_deploy.sh</string>
    </array>
    <key>StartInterval</key>
    <integer>300</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/dakshpatel/infinite_research/sentinel/logs/launchd_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/dakshpatel/infinite_research/sentinel/logs/launchd_stderr.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/Users/dakshpatel/.npm-global/bin</string>
    </dict>
</dict>
</plist>
PLISTEOF
    echo "Plist installed at $PLIST_PATH"
    echo "Loading..."
    launchctl load "$PLIST_PATH"
    echo "Auto-deploy scheduled (every 5 minutes)"
    ;;

  uninstall)
    echo "Uninstalling launchd plist..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    rm -f "$PLIST_PATH"
    echo "Auto-deploy uninstalled"
    ;;

  *)
    echo "Usage: sentinel_ctl.sh [start|stop|status|deploy|install|uninstall]"
    exit 1
    ;;
esac
