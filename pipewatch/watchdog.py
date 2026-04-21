"""Watchdog module: detect pipelines that have gone silent (no recent check results)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import load_recent


@dataclass
class WatchdogResult:
    pipeline_name: str
    last_seen: Optional[datetime]
    silence_threshold_seconds: int
    is_silent: bool

    def __str__(self) -> str:
        if self.last_seen is None:
            return f"[SILENT] {self.pipeline_name}: never recorded"
        age = int(
            (datetime.now(tz=timezone.utc) - self.last_seen).total_seconds()
        )
        status = "SILENT" if self.is_silent else "OK"
        return (
            f"[{status}] {self.pipeline_name}: "
            f"last seen {age}s ago (threshold {self.silence_threshold_seconds}s)"
        )


def _most_recent_timestamp(db_path: str, pipeline_name: str) -> Optional[datetime]:
    """Return the UTC datetime of the most recent result for a pipeline."""
    rows = load_recent(db_path, pipeline_name=pipeline_name, limit=1)
    if not rows:
        return None
    ts = rows[0].timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def check_watchdog(
    db_path: str,
    pipeline_name: str,
    silence_threshold_seconds: int = 300,
) -> WatchdogResult:
    """Check whether a single pipeline has gone silent."""
    if silence_threshold_seconds <= 0:
        raise ValueError("silence_threshold_seconds must be positive")

    last_seen = _most_recent_timestamp(db_path, pipeline_name)
    if last_seen is None:
        return WatchdogResult(
            pipeline_name=pipeline_name,
            last_seen=None,
            silence_threshold_seconds=silence_threshold_seconds,
            is_silent=True,
        )

    now = datetime.now(tz=timezone.utc)
    age_seconds = (now - last_seen).total_seconds()
    return WatchdogResult(
        pipeline_name=pipeline_name,
        last_seen=last_seen,
        silence_threshold_seconds=silence_threshold_seconds,
        is_silent=age_seconds > silence_threshold_seconds,
    )


def check_all_watchdogs(
    db_path: str,
    pipeline_names: List[str],
    silence_threshold_seconds: int = 300,
) -> List[WatchdogResult]:
    """Run watchdog checks for multiple pipelines."""
    return [
        check_watchdog(db_path, name, silence_threshold_seconds)
        for name in pipeline_names
    ]


def any_silent(results: List[WatchdogResult]) -> bool:
    """Return True if at least one pipeline is silent."""
    return any(r.is_silent for r in results)
