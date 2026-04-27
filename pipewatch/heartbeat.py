"""Heartbeat tracking: record and verify periodic pipeline liveness signals."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_heartbeat_db(db_path: str) -> None:
    """Create the heartbeats table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS heartbeats (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline  TEXT NOT NULL,
                timestamp REAL NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hb_pipeline ON heartbeats(pipeline)"
        )


@dataclass
class HeartbeatResult:
    pipeline: str
    last_seen: Optional[float]  # epoch seconds, None if never seen
    threshold_seconds: float
    is_alive: bool

    def __str__(self) -> str:
        if self.last_seen is None:
            age = "never seen"
        else:
            age = f"{time.time() - self.last_seen:.0f}s ago"
        status = "ALIVE" if self.is_alive else "DEAD"
        return f"[{status}] {self.pipeline} — last heartbeat {age} (threshold {self.threshold_seconds:.0f}s)"


def record_heartbeat(db_path: str, pipeline: str, ts: Optional[float] = None) -> None:
    """Record a heartbeat for *pipeline* at *ts* (defaults to now)."""
    ts = ts if ts is not None else time.time()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO heartbeats (pipeline, timestamp) VALUES (?, ?)",
            (pipeline, ts),
        )


def _most_recent(db_path: str, pipeline: str) -> Optional[float]:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT timestamp FROM heartbeats WHERE pipeline = ? ORDER BY timestamp DESC LIMIT 1",
            (pipeline,),
        ).fetchone()
    return row["timestamp"] if row else None


def check_heartbeat(db_path: str, pipeline: str, threshold_seconds: float) -> HeartbeatResult:
    """Return a HeartbeatResult indicating whether the pipeline is still alive."""
    if threshold_seconds <= 0:
        raise ValueError("threshold_seconds must be positive")
    last_seen = _most_recent(db_path, pipeline)
    if last_seen is None:
        is_alive = False
    else:
        is_alive = (time.time() - last_seen) <= threshold_seconds
    return HeartbeatResult(
        pipeline=pipeline,
        last_seen=last_seen,
        threshold_seconds=threshold_seconds,
        is_alive=is_alive,
    )


def check_all_heartbeats(
    db_path: str,
    specs: List[dict],
) -> List[HeartbeatResult]:
    """Check heartbeats for a list of {pipeline, threshold_seconds} specs."""
    return [check_heartbeat(db_path, s["pipeline"], s["threshold_seconds"]) for s in specs]
