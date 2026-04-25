"""Alert throttling: limit how often alerts fire for a given pipeline."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_DEFAULT_DB = Path(".pipewatch_throttle.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_throttle_db(db_path: Path = _DEFAULT_DB) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS throttle_log (
                pipeline TEXT NOT NULL,
                alert_type TEXT NOT NULL DEFAULT 'default',
                fired_at REAL NOT NULL,
                PRIMARY KEY (pipeline, alert_type)
            )
            """
        )
        conn.commit()


@dataclass
class ThrottlePolicy:
    pipeline: str
    cooldown_seconds: int = 3600
    alert_type: str = "default"

    def __post_init__(self) -> None:
        if self.cooldown_seconds <= 0:
            raise ValueError("cooldown_seconds must be positive")


def is_throttled(
    policy: ThrottlePolicy,
    db_path: Path = _DEFAULT_DB,
    now: Optional[float] = None,
) -> bool:
    """Return True if an alert for this pipeline was recently fired."""
    now = now or time.time()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT fired_at FROM throttle_log WHERE pipeline = ? AND alert_type = ?",
            (policy.pipeline, policy.alert_type),
        ).fetchone()
    if row is None:
        return False
    return (now - row["fired_at"]) < policy.cooldown_seconds


def record_alert(
    policy: ThrottlePolicy,
    db_path: Path = _DEFAULT_DB,
    now: Optional[float] = None,
) -> None:
    """Record that an alert was fired right now."""
    now = now or time.time()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO throttle_log (pipeline, alert_type, fired_at)
            VALUES (?, ?, ?)
            ON CONFLICT(pipeline, alert_type) DO UPDATE SET fired_at = excluded.fired_at
            """,
            (policy.pipeline, policy.alert_type, now),
        )
        conn.commit()


def clear_throttle(
    pipeline: str,
    alert_type: str = "default",
    db_path: Path = _DEFAULT_DB,
) -> None:
    """Remove throttle record so the next alert fires immediately."""
    with _connect(db_path) as conn:
        conn.execute(
            "DELETE FROM throttle_log WHERE pipeline = ? AND alert_type = ?",
            (pipeline, alert_type),
        )
        conn.commit()
