"""Rate limiting for alerts and notifications to prevent alert fatigue."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DEFAULT_DB = Path(".pipewatch_ratelimit.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_ratelimit_db(db_path: Path = DEFAULT_DB) -> None:
    """Create the rate limit table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_ratelimit (
                pipeline TEXT NOT NULL,
                check_type TEXT NOT NULL,
                last_sent REAL NOT NULL,
                PRIMARY KEY (pipeline, check_type)
            )
            """
        )


@dataclass
class RateLimitPolicy:
    cooldown_seconds: int = 300  # 5 minutes default

    def __post_init__(self) -> None:
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be non-negative")


def is_rate_limited(
    pipeline: str,
    check_type: str,
    policy: RateLimitPolicy,
    db_path: Path = DEFAULT_DB,
    now: Optional[float] = None,
) -> bool:
    """Return True if an alert for this pipeline+check_type is still in cooldown."""
    now = now if now is not None else time.time()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT last_sent FROM alert_ratelimit WHERE pipeline=? AND check_type=?",
            (pipeline, check_type),
        ).fetchone()
    if row is None:
        return False
    return (now - row["last_sent"]) < policy.cooldown_seconds


def record_alert_sent(
    pipeline: str,
    check_type: str,
    db_path: Path = DEFAULT_DB,
    now: Optional[float] = None,
) -> None:
    """Record that an alert was just sent for pipeline+check_type."""
    now = now if now is not None else time.time()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO alert_ratelimit (pipeline, check_type, last_sent)
            VALUES (?, ?, ?)
            ON CONFLICT(pipeline, check_type) DO UPDATE SET last_sent=excluded.last_sent
            """,
            (pipeline, check_type, now),
        )


def clear_ratelimit(db_path: Path = DEFAULT_DB) -> None:
    """Remove all rate limit records (useful for testing or manual reset)."""
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM alert_ratelimit")
