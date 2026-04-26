"""Cadence tracking: detect pipelines that have stopped running on schedule."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_recent, _connect


@dataclass
class CadencePolicy:
    pipeline: str
    expected_interval_seconds: int
    grace_seconds: int = 60

    def __post_init__(self) -> None:
        if self.expected_interval_seconds <= 0:
            raise ValueError("expected_interval_seconds must be positive")
        if self.grace_seconds < 0:
            raise ValueError("grace_seconds must be non-negative")


@dataclass
class CadenceResult:
    pipeline: str
    last_seen: Optional[float]
    expected_interval_seconds: int
    overdue_by_seconds: float
    is_overdue: bool

    def __str__(self) -> str:
        if self.last_seen is None:
            return f"{self.pipeline}: never seen (overdue)"
        status = "OVERDUE" if self.is_overdue else "OK"
        if self.is_overdue:
            return (
                f"{self.pipeline}: [{status}] overdue by "
                f"{self.overdue_by_seconds:.0f}s "
                f"(expected every {self.expected_interval_seconds}s)"
            )
        return f"{self.pipeline}: [{status}] running on schedule"


def _most_recent_run(db_path: str, pipeline: str) -> Optional[float]:
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT timestamp FROM results WHERE pipeline = ? ORDER BY timestamp DESC LIMIT 1",
        (pipeline,),
    ).fetchone()
    conn.close()
    return row[0] if row else None


def check_cadence(policy: CadencePolicy, db_path: str, now: Optional[float] = None) -> CadenceResult:
    """Check whether a pipeline has run within its expected cadence."""
    if now is None:
        now = time.time()

    last_seen = _most_recent_run(db_path, policy.pipeline)
    deadline = (policy.expected_interval_seconds + policy.grace_seconds)

    if last_seen is None:
        return CadenceResult(
            pipeline=policy.pipeline,
            last_seen=None,
            expected_interval_seconds=policy.expected_interval_seconds,
            overdue_by_seconds=float("inf"),
            is_overdue=True,
        )

    elapsed = now - last_seen
    overdue_by = max(0.0, elapsed - deadline)
    is_overdue = elapsed > deadline

    return CadenceResult(
        pipeline=policy.pipeline,
        last_seen=last_seen,
        expected_interval_seconds=policy.expected_interval_seconds,
        overdue_by_seconds=overdue_by,
        is_overdue=is_overdue,
    )


def check_all_cadences(
    policies: List[CadencePolicy], db_path: str, now: Optional[float] = None
) -> List[CadenceResult]:
    """Run cadence checks for all provided policies."""
    return [check_cadence(p, db_path, now=now) for p in policies]
