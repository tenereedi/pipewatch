"""Checkpoint tracking — record and compare pipeline run milestones."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

_DEFAULT_DB = Path(".pipewatch_checkpoints.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_checkpoint_db(db_path: Path = _DEFAULT_DB) -> None:
    """Create the checkpoints table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline  TEXT    NOT NULL,
                label     TEXT    NOT NULL,
                timestamp REAL    NOT NULL
            )
            """
        )
        conn.commit()


@dataclass
class Checkpoint:
    pipeline: str
    label: str
    timestamp: float

    def __str__(self) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        return f"[{self.pipeline}] {self.label} @ {ts}"


def record_checkpoint(
    pipeline: str,
    label: str,
    db_path: Path = _DEFAULT_DB,
    *,
    timestamp: Optional[float] = None,
) -> Checkpoint:
    """Persist a named checkpoint for a pipeline."""
    ts = timestamp if timestamp is not None else time.time()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO checkpoints (pipeline, label, timestamp) VALUES (?, ?, ?)",
            (pipeline, label, ts),
        )
        conn.commit()
    return Checkpoint(pipeline=pipeline, label=label, timestamp=ts)


def load_checkpoints(
    pipeline: str,
    db_path: Path = _DEFAULT_DB,
    limit: int = 50,
) -> List[Checkpoint]:
    """Return the most recent checkpoints for a pipeline, newest first."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT pipeline, label, timestamp
            FROM checkpoints
            WHERE pipeline = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (pipeline, limit),
        ).fetchall()
    return [Checkpoint(pipeline=r["pipeline"], label=r["label"], timestamp=r["timestamp"]) for r in rows]


def latest_checkpoint(pipeline: str, db_path: Path = _DEFAULT_DB) -> Optional[Checkpoint]:
    """Return the single most recent checkpoint for a pipeline, or None."""
    results = load_checkpoints(pipeline, db_path=db_path, limit=1)
    return results[0] if results else None


def clear_checkpoints(pipeline: str, db_path: Path = _DEFAULT_DB) -> int:
    """Delete all checkpoints for a pipeline. Returns number of rows removed."""
    with _connect(db_path) as conn:
        cur = conn.execute("DELETE FROM checkpoints WHERE pipeline = ?", (pipeline,))
        conn.commit()
        return cur.rowcount
