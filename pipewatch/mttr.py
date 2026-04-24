"""Mean Time To Recovery (MTTR) tracker for pipeline checks."""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

from pipewatch.checks import CheckResult


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_mttr_db(db_path: str) -> None:
    """Create the mttr_events table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mttr_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline    TEXT    NOT NULL,
                failed_at   REAL    NOT NULL,
                recovered_at REAL
            )
            """
        )
        conn.commit()


@dataclass
class MTTRSummary:
    pipeline: str
    incident_count: int
    mean_seconds: Optional[float]

    def __str__(self) -> str:
        if self.mean_seconds is None:
            return f"{self.pipeline}: no completed incidents"
        mins = self.mean_seconds / 60
        return (
            f"{self.pipeline}: {self.incident_count} incident(s), "
            f"MTTR={mins:.1f} min"
        )


def record_result(db_path: str, result: CheckResult) -> None:
    """Open or close an incident based on the latest check result."""
    now = time.time()
    with _connect(db_path) as conn:
        open_row = conn.execute(
            "SELECT id FROM mttr_events WHERE pipeline=? AND recovered_at IS NULL",
            (result.pipeline,),
        ).fetchone()

        if not result.is_healthy and open_row is None:
            # New failure — open an incident
            conn.execute(
                "INSERT INTO mttr_events (pipeline, failed_at) VALUES (?, ?)",
                (result.pipeline, now),
            )
        elif result.is_healthy and open_row is not None:
            # Recovery — close the open incident
            conn.execute(
                "UPDATE mttr_events SET recovered_at=? WHERE id=?",
                (now, open_row["id"]),
            )
        conn.commit()


def compute_mttr(db_path: str, pipeline: str) -> MTTRSummary:
    """Compute MTTR for a single pipeline from closed incidents."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT failed_at, recovered_at
            FROM mttr_events
            WHERE pipeline=? AND recovered_at IS NOT NULL
            """,
            (pipeline,),
        ).fetchall()

    if not rows:
        return MTTRSummary(pipeline=pipeline, incident_count=0, mean_seconds=None)

    durations = [r["recovered_at"] - r["failed_at"] for r in rows]
    return MTTRSummary(
        pipeline=pipeline,
        incident_count=len(durations),
        mean_seconds=sum(durations) / len(durations),
    )
