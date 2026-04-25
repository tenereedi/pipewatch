"""Periodic rollup summaries: aggregate check results into time-bucketed windows."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_recent


@dataclass
class RollupBucket:
    pipeline: str
    window_label: str   # e.g. "1h", "6h", "24h"
    total: int
    healthy: int
    unhealthy: int

    @property
    def health_rate(self) -> float:
        return self.healthy / self.total if self.total > 0 else 1.0

    def __str__(self) -> str:
        pct = self.health_rate * 100
        bar = "#" * int(pct / 10) + "-" * (10 - int(pct / 10))
        return (
            f"{self.pipeline:<30} [{self.window_label:>3}]  "
            f"[{bar}] {pct:5.1f}%  "
            f"{self.healthy}/{self.total} healthy"
        )


_WINDOWS = {
    "1h": 3600,
    "6h": 21600,
    "24h": 86400,
}


def compute_rollup(
    db_path: str,
    pipeline: Optional[str] = None,
    window: str = "1h",
) -> List[RollupBucket]:
    """Return rollup buckets for each pipeline found in the given time window."""
    if window not in _WINDOWS:
        raise ValueError(f"Unknown window '{window}'. Choose from: {list(_WINDOWS)}.")

    seconds = _WINDOWS[window]
    since = time.time() - seconds
    results = load_recent(db_path, pipeline=pipeline, limit=10_000)

    # filter to window
    results = [r for r in results if r.timestamp >= since]

    # group by pipeline
    groups: dict[str, list] = {}
    for r in results:
        groups.setdefault(r.pipeline, []).append(r)

    buckets = []
    for pipe, items in sorted(groups.items()):
        healthy = sum(1 for r in items if r.healthy)
        buckets.append(
            RollupBucket(
                pipeline=pipe,
                window_label=window,
                total=len(items),
                healthy=healthy,
                unhealthy=len(items) - healthy,
            )
        )
    return buckets


def print_rollup(buckets: List[RollupBucket]) -> None:
    if not buckets:
        print("No data for the requested rollup window.")
        return
    print(f"{'Pipeline':<30} {'Window':>5}  {'Health Bar':>14}  Details")
    print("-" * 72)
    for b in buckets:
        print(b)
