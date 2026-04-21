"""Escalation policy: track consecutive failures and escalate alerts after a threshold."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from pipewatch.checks import CheckResult

DEFAULT_DB = Path(".pipewatch_escalation.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_escalation_db(db_path: Path = DEFAULT_DB) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS escalation_state (
                pipeline TEXT PRIMARY KEY,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_updated REAL NOT NULL
            )
            """
        )


@dataclass
class EscalationPolicy:
    threshold: int = 3  # consecutive failures before escalation

    def __post_init__(self) -> None:
        if self.threshold < 1:
            raise ValueError("threshold must be >= 1")


@dataclass
class EscalationResult:
    pipeline: str
    consecutive_failures: int
    threshold: int
    escalated: bool

    def __str__(self) -> str:
        status = "ESCALATED" if self.escalated else "ok"
        return (
            f"[{status}] {self.pipeline}: "
            f"{self.consecutive_failures}/{self.threshold} consecutive failures"
        )


def update_and_check(
    pipeline: str,
    results: List[CheckResult],
    policy: EscalationPolicy,
    db_path: Path = DEFAULT_DB,
) -> EscalationResult:
    """Update failure streak for a pipeline and return escalation status."""
    all_healthy = all(r.is_healthy() for r in results)
    now = time.time()

    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT consecutive_failures FROM escalation_state WHERE pipeline = ?",
            (pipeline,),
        ).fetchone()

        current = row["consecutive_failures"] if row else 0
        new_count = 0 if all_healthy else current + 1

        conn.execute(
            """
            INSERT INTO escalation_state (pipeline, consecutive_failures, last_updated)
            VALUES (?, ?, ?)
            ON CONFLICT(pipeline) DO UPDATE SET
                consecutive_failures = excluded.consecutive_failures,
                last_updated = excluded.last_updated
            """,
            (pipeline, new_count, now),
        )

    return EscalationResult(
        pipeline=pipeline,
        consecutive_failures=new_count,
        threshold=policy.threshold,
        escalated=new_count >= policy.threshold,
    )


def check_all_escalations(
    results_by_pipeline: dict,
    policy: EscalationPolicy,
    db_path: Path = DEFAULT_DB,
) -> List[EscalationResult]:
    """Run escalation checks for all pipelines."""
    return [
        update_and_check(pipeline, results, policy, db_path)
        for pipeline, results in results_by_pipeline.items()
    ]
