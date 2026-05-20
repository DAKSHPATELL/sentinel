"""
SENTINEL PID file management.
Tracks the running SENTINEL daemon process for clean start/stop lifecycle.
"""
from __future__ import annotations

import os
import signal
import sys
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)

DEFAULT_PID_PATH = Path("data/sentinel.pid")


def write_pid(pid_path: Path = DEFAULT_PID_PATH) -> None:
    """Write current process PID to file."""
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()))
    logger.info("pid_written", pid=os.getpid(), path=str(pid_path))


def read_pid(pid_path: Path = DEFAULT_PID_PATH) -> int | None:
    """Read PID from file, return None if not found or invalid."""
    try:
        if not pid_path.exists():
            return None
        pid = int(pid_path.read_text().strip())
        return pid
    except (ValueError, OSError):
        return None


def remove_pid(pid_path: Path = DEFAULT_PID_PATH) -> None:
    """Remove PID file."""
    try:
        if pid_path.exists():
            pid_path.unlink()
            logger.info("pid_removed", path=str(pid_path))
    except OSError:
        pass


def is_running(pid_path: Path = DEFAULT_PID_PATH) -> bool:
    """Check if the SENTINEL process recorded in the PID file is actually running."""
    pid = read_pid(pid_path)
    if pid is None:
        return False
    try:
        # Signal 0 checks if process exists without actually sending a signal
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        # Process died but PID file wasn't cleaned up — stale PID
        remove_pid(pid_path)
        return False
    except PermissionError:
        # Process exists but we can't signal it (different user)
        return True


def stop_process(pid_path: Path = DEFAULT_PID_PATH, timeout: int = 30) -> bool:
    """
    Send SIGTERM to the running SENTINEL process and wait for it to exit.

    Args:
        pid_path: Path to PID file.
        timeout: Max seconds to wait before force-killing.

    Returns:
        True if process was stopped, False if no process was running.
    """
    import time

    pid = read_pid(pid_path)
    if pid is None:
        return False

    if not is_running(pid_path):
        remove_pid(pid_path)
        return False

    # Send SIGTERM (graceful shutdown)
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        remove_pid(pid_path)
        return False

    # Wait for process to exit
    for _ in range(timeout * 10):  # Check every 100ms
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except ProcessLookupError:
            remove_pid(pid_path)
            return True

    # Still alive after timeout — force kill
    try:
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.5)
    except ProcessLookupError:
        pass

    remove_pid(pid_path)
    return True
