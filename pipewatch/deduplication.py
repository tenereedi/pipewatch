"""Deduplication module: suppress repeated alerts for the same failing pipeline."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from pipewatch.checks import CheckResult

DEFAULT_DB = Path(".pipewatch_dedup.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_dedup_db(db_path: Path = DEFAULT_DB) -> None:
    """Create the deduplication table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dedup_log (
                pipeline TEXT NOT NULL,
                check_name TEXT NOT NULL,
                first_seen REAL NOT NULL,
                last_seen REAL NOT NULL,
                count INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (pipeline, check_name)
            )
            """
        )


@dataclass
class DedupEntry:
    pipeline: str
    check_name: str
    first_seen: float
    last_seen: float
    count: int

    def age_seconds(self) -> float:
        return time.time() - self.first_seen


def record_failure(
    result: CheckResult,
    db_path: Path = DEFAULT_DB,
) -> DedupEntry:
    """Record a failing result; upsert the dedup log row and return the entry."""
    now = time.time()
    with _connect(db_path) as conn:
        existing = conn.execute(
            "SELECT * FROM dedup_log WHERE pipeline = ? AND check_name = ?",
            (result.pipeline_name, result.check_name),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE dedup_log SET last_seen = ?, count = count + 1
                WHERE pipeline = ? AND check_name = ?
                """,
                (now, result.pipeline_name, result.check_name),
            )
            return DedupEntry(
                pipeline=existing["pipeline"],
                check_name=existing["check_name"],
                first_seen=existing["first_seen"],
                last_seen=now,
                count=existing["count"] + 1,
            )
        conn.execute(
            "INSERT INTO dedup_log (pipeline, check_name, first_seen, last_seen, count) VALUES (?, ?, ?, ?, 1)",
            (result.pipeline_name, result.check_name, now, now),
        )
        return DedupEntry(
            pipeline=result.pipeline_name,
            check_name=result.check_name,
            first_seen=now,
            last_seen=now,
            count=1,
        )


def is_duplicate(
    result: CheckResult,
    min_count: int = 2,
    db_path: Path = DEFAULT_DB,
) -> bool:
    """Return True if this failure has already been seen at least min_count times."""
    row = _connect(db_path).execute(
        "SELECT count FROM dedup_log WHERE pipeline = ? AND check_name = ?",
        (result.pipeline_name, result.check_name),
    ).fetchone()
    if row is None:
        return False
    return row["count"] >= min_count


def clear_resolved(
    results: List[CheckResult],
    db_path: Path = DEFAULT_DB,
) -> None:
    """Remove dedup entries for pipelines whose checks are now healthy."""
    healthy = [(r.pipeline_name, r.check_name) for r in results if r.is_healthy()]
    if not healthy:
        return
    with _connect(db_path) as conn:
        conn.executemany(
            "DELETE FROM dedup_log WHERE pipeline = ? AND check_name = ?",
            healthy,
        )
