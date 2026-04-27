"""Quota enforcement: track and cap the number of failures per pipeline within a rolling window."""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

_DEFAULT_DB = Path("pipewatch_quota.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_quota_db(db_path: Path = _DEFAULT_DB) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quota_events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline  TEXT    NOT NULL,
                timestamp REAL    NOT NULL
            )
            """
        )
        conn.commit()


@dataclass
class QuotaPolicy:
    pipeline: str
    max_failures: int
    window_seconds: int

    def __post_init__(self) -> None:
        if self.max_failures < 1:
            raise ValueError("max_failures must be >= 1")
        if self.window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")

    def cutoff(self, now: Optional[float] = None) -> float:
        return (now or time.time()) - self.window_seconds


@dataclass
class QuotaResult:
    pipeline: str
    failures_in_window: int
    max_failures: int
    exceeded: bool

    def __str__(self) -> str:
        status = "EXCEEDED" if self.exceeded else "ok"
        return (
            f"[quota:{status}] {self.pipeline} — "
            f"{self.failures_in_window}/{self.max_failures} failures in window"
        )


def record_failure(pipeline: str, db_path: Path = _DEFAULT_DB, now: Optional[float] = None) -> None:
    ts = now or time.time()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO quota_events (pipeline, timestamp) VALUES (?, ?)",
            (pipeline, ts),
        )
        conn.commit()


def evaluate_quota(
    policy: QuotaPolicy,
    db_path: Path = _DEFAULT_DB,
    now: Optional[float] = None,
) -> QuotaResult:
    cutoff = policy.cutoff(now)
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM quota_events WHERE pipeline = ? AND timestamp >= ?",
            (policy.pipeline, cutoff),
        ).fetchone()
    count = row["cnt"] if row else 0
    return QuotaResult(
        pipeline=policy.pipeline,
        failures_in_window=count,
        max_failures=policy.max_failures,
        exceeded=count > policy.max_failures,
    )


def evaluate_all_quotas(
    policies: List[QuotaPolicy],
    db_path: Path = _DEFAULT_DB,
    now: Optional[float] = None,
) -> List[QuotaResult]:
    return [evaluate_quota(p, db_path=db_path, now=now) for p in policies]
