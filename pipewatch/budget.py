"""Check budget module: tracks how many checks have fired in a time window."""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_budget_db(db_path: str) -> None:
    """Create the budget tracking table if it doesn't exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS check_budget (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline TEXT NOT NULL,
                fired_at REAL NOT NULL
            )
            """
        )
        conn.commit()


@dataclass
class BudgetPolicy:
    max_checks: int
    window_seconds: int

    def __post_init__(self) -> None:
        if self.max_checks < 1:
            raise ValueError("max_checks must be >= 1")
        if self.window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")

    def cutoff(self) -> float:
        return time.time() - self.window_seconds


@dataclass
class BudgetResult:
    pipeline: str
    fired_in_window: int
    max_checks: int
    window_seconds: int
    budget_exceeded: bool

    def __str__(self) -> str:
        status = "EXCEEDED" if self.budget_exceeded else "OK"
        return (
            f"[{status}] {self.pipeline}: "
            f"{self.fired_in_window}/{self.max_checks} checks "
            f"in last {self.window_seconds}s"
        )


def record_check(db_path: str, pipeline: str) -> None:
    """Record that a check fired for the given pipeline."""
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO check_budget (pipeline, fired_at) VALUES (?, ?)",
            (pipeline, time.time()),
        )
        conn.commit()


def evaluate_budget(
    db_path: str, pipeline: str, policy: BudgetPolicy
) -> BudgetResult:
    """Count checks fired in window and compare against policy."""
    cutoff = policy.cutoff()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM check_budget WHERE pipeline=? AND fired_at>=?",
            (pipeline, cutoff),
        ).fetchone()
    fired = row["cnt"] if row else 0
    return BudgetResult(
        pipeline=pipeline,
        fired_in_window=fired,
        max_checks=policy.max_checks,
        window_seconds=policy.window_seconds,
        budget_exceeded=fired > policy.max_checks,
    )


def check_all_budgets(
    db_path: str, pipelines: List[str], policy: BudgetPolicy
) -> List[BudgetResult]:
    """Evaluate budget for every pipeline in the list."""
    return [evaluate_budget(db_path, p, policy) for p in pipelines]
