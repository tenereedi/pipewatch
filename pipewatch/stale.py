"""Stale pipeline detection — flags pipelines that have not reported results recently."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_recent


@dataclass
class StalenessResult:
    pipeline: str
    last_seen: Optional[float]  # epoch seconds, or None if never seen
    threshold_seconds: int
    is_stale: bool

    def __str__(self) -> str:
        if self.last_seen is None:
            age_str = "never seen"
        else:
            age = int(time.time() - self.last_seen)
            age_str = f"{age}s ago"
        status = "STALE" if self.is_stale else "OK"
        return f"[{status}] {self.pipeline} — last seen: {age_str} (threshold: {self.threshold_seconds}s)"


def _most_recent_timestamp(db_path: str, pipeline: str) -> Optional[float]:
    """Return the epoch timestamp of the most recent result for *pipeline*, or None."""
    rows = load_recent(db_path, pipeline=pipeline, limit=1)
    if not rows:
        return None
    return rows[0].timestamp


def check_staleness(
    db_path: str,
    pipeline: str,
    threshold_seconds: int = 300,
) -> StalenessResult:
    """Check whether *pipeline* has gone stale (no results within *threshold_seconds*)."""
    if threshold_seconds <= 0:
        raise ValueError("threshold_seconds must be positive")

    last_seen = _most_recent_timestamp(db_path, pipeline)
    if last_seen is None:
        is_stale = False  # never seen — not stale, just unknown
    else:
        is_stale = (time.time() - last_seen) > threshold_seconds

    return StalenessResult(
        pipeline=pipeline,
        last_seen=last_seen,
        threshold_seconds=threshold_seconds,
        is_stale=is_stale,
    )


def check_all_staleness(
    db_path: str,
    pipelines: List[str],
    threshold_seconds: int = 300,
) -> List[StalenessResult]:
    """Run staleness checks for every pipeline in *pipelines*."""
    return [
        check_staleness(db_path, p, threshold_seconds=threshold_seconds)
        for p in pipelines
    ]
