"""Replay historical check results to simulate past pipeline states."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.checks import CheckResult
from pipewatch.history import load_recent


@dataclass
class ReplayWindow:
    """A slice of historical results for a given pipeline."""

    pipeline: str
    results: List[CheckResult]

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def healthy_count(self) -> int:
        return sum(1 for r in self.results if r.is_healthy)

    @property
    def failure_count(self) -> int:
        return self.total - self.healthy_count

    def __str__(self) -> str:
        return (
            f"ReplayWindow(pipeline={self.pipeline!r}, "
            f"total={self.total}, healthy={self.healthy_count}, "
            f"failures={self.failure_count})"
        )


def load_replay_window(
    db_path: str,
    pipeline: str,
    limit: int = 50,
) -> ReplayWindow:
    """Load the most recent *limit* results for *pipeline* from history."""
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")
    results = load_recent(db_path, pipeline=pipeline, limit=limit)
    return ReplayWindow(pipeline=pipeline, results=results)


def replay_summary(window: ReplayWindow) -> str:
    """Return a human-readable summary of a replay window."""
    if window.total == 0:
        return f"[{window.pipeline}] No historical data available."
    rate = window.healthy_count / window.total * 100
    return (
        f"[{window.pipeline}] Replayed {window.total} result(s): "
        f"{window.healthy_count} healthy, {window.failure_count} failed "
        f"({rate:.1f}% success rate)"
    )


def replay_all(
    db_path: str,
    pipelines: List[str],
    limit: int = 50,
) -> List[ReplayWindow]:
    """Load replay windows for every pipeline in *pipelines*."""
    return [load_replay_window(db_path, p, limit=limit) for p in pipelines]
