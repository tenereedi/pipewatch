"""Retention policy: prune history records older than a configured age."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from pipewatch.history import _connect


@dataclass
class RetentionPolicy:
    max_age_days: int
    pipeline: Optional[str] = None  # None means apply to all pipelines

    def __post_init__(self) -> None:
        if self.max_age_days < 1:
            raise ValueError("max_age_days must be at least 1")

    def cutoff_timestamp(self) -> str:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.max_age_days)
        return cutoff.isoformat()


def prune_history(db_path: str, policy: RetentionPolicy) -> int:
    """Delete records older than policy.max_age_days.

    Returns the number of rows deleted.
    """
    cutoff = policy.cutoff_timestamp()
    conn: sqlite3.Connection = _connect(db_path)
    try:
        if policy.pipeline:
            cur = conn.execute(
                "DELETE FROM results WHERE timestamp < ? AND pipeline = ?",
                (cutoff, policy.pipeline),
            )
        else:
            cur = conn.execute(
                "DELETE FROM results WHERE timestamp < ?",
                (cutoff,),
            )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def prune_all(db_path: str, policies: list[RetentionPolicy]) -> dict[str, int]:
    """Apply a list of retention policies and return a summary of deleted rows.

    Keys are pipeline names (or '__all__' for global policies).
    """
    summary: dict[str, int] = {}
    for policy in policies:
        deleted = prune_history(db_path, policy)
        key = policy.pipeline or "__all__"
        summary[key] = summary.get(key, 0) + deleted
    return summary
