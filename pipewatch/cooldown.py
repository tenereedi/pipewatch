"""Cooldown tracking: prevent repeated alerts for a pipeline within a time window."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

_DEFAULT_DB = Path(".pipewatch_cooldown.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_cooldown_db(db_path: Path = _DEFAULT_DB) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cooldowns (
                pipeline TEXT PRIMARY KEY,
                last_alerted_at REAL NOT NULL
            )
            """
        )


@dataclass
class CooldownPolicy:
    pipeline: str
    window_seconds: int

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")

    def is_cooling_down(self, db_path: Path = _DEFAULT_DB) -> bool:
        """Return True if the pipeline is still within its cooldown window."""
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT last_alerted_at FROM cooldowns WHERE pipeline = ?",
                (self.pipeline,),
            ).fetchone()
        if row is None:
            return False
        elapsed = time.time() - row["last_alerted_at"]
        return elapsed < self.window_seconds

    def record_alert(self, db_path: Path = _DEFAULT_DB) -> None:
        """Mark that an alert was just dispatched for this pipeline."""
        now = time.time()
        with _connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO cooldowns (pipeline, last_alerted_at)
                VALUES (?, ?)
                ON CONFLICT(pipeline) DO UPDATE SET last_alerted_at = excluded.last_alerted_at
                """,
                (self.pipeline, now),
            )

    def reset(self, db_path: Path = _DEFAULT_DB) -> None:
        """Clear the cooldown record for this pipeline."""
        with _connect(db_path) as conn:
            conn.execute(
                "DELETE FROM cooldowns WHERE pipeline = ?", (self.pipeline,)
            )

    def __str__(self) -> str:
        return f"CooldownPolicy(pipeline={self.pipeline!r}, window={self.window_seconds}s)"
