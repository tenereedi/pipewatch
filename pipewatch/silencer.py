"""Silence (suppress) alerts for specific pipelines for a given duration."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Optional


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_silencer_db(db_path: str) -> None:
    """Create the silences table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS silences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline TEXT NOT NULL,
                until REAL NOT NULL,
                reason TEXT
            )
            """
        )
        conn.commit()


@dataclass
class Silence:
    pipeline: str
    until: float
    reason: Optional[str] = None

    def is_active(self) -> bool:
        return time.time() < self.until

    def __str__(self) -> str:
        active = "active" if self.is_active() else "expired"
        until_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.until))
        return f"[{active}] {self.pipeline} silenced until {until_str}" + (
            f" ({self.reason})" if self.reason else ""
        )


def add_silence(db_path: str, pipeline: str, duration_seconds: int, reason: Optional[str] = None) -> Silence:
    """Silence a pipeline for the given number of seconds."""
    until = time.time() + duration_seconds
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO silences (pipeline, until, reason) VALUES (?, ?, ?)",
            (pipeline, until, reason),
        )
        conn.commit()
    return Silence(pipeline=pipeline, until=until, reason=reason)


def is_silenced(db_path: str, pipeline: str) -> bool:
    """Return True if the pipeline currently has an active silence."""
    now = time.time()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM silences WHERE pipeline = ? AND until > ? LIMIT 1",
            (pipeline, now),
        ).fetchone()
    return row is not None


def list_silences(db_path: str, include_expired: bool = False) -> list[Silence]:
    """Return all silences, optionally filtering out expired ones."""
    with _connect(db_path) as conn:
        if include_expired:
            rows = conn.execute("SELECT pipeline, until, reason FROM silences ORDER BY until DESC").fetchall()
        else:
            rows = conn.execute(
                "SELECT pipeline, until, reason FROM silences WHERE until > ? ORDER BY until DESC",
                (time.time(),),
            ).fetchall()
    return [Silence(pipeline=r["pipeline"], until=r["until"], reason=r["reason"]) for r in rows]


def clear_silences(db_path: str, pipeline: Optional[str] = None) -> int:
    """Remove silences. If pipeline is given, only remove that pipeline's silences."""
    with _connect(db_path) as conn:
        if pipeline:
            cur = conn.execute("DELETE FROM silences WHERE pipeline = ?", (pipeline,))
        else:
            cur = conn.execute("DELETE FROM silences")
        conn.commit()
    return cur.rowcount
