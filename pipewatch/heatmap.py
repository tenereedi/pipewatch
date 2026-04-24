"""Failure heatmap: shows failure counts bucketed by hour-of-day across pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import datetime

from pipewatch.history import load_recent


@dataclass
class HeatmapRow:
    pipeline: str
    # buckets[h] = failure count for hour h (0-23)
    buckets: List[int] = field(default_factory=lambda: [0] * 24)

    @property
    def peak_hour(self) -> int:
        return self.buckets.index(max(self.buckets))

    @property
    def total_failures(self) -> int:
        return sum(self.buckets)

    def __str__(self) -> str:
        bar = "".join(_heat_char(v) for v in self.buckets)
        return f"{self.pipeline:<30} [{bar}]  peak={self.peak_hour:02d}h  failures={self.total_failures}"


def _heat_char(count: int) -> str:
    if count == 0:
        return "."
    if count <= 2:
        return "░"
    if count <= 5:
        return "▒"
    if count <= 10:
        return "▓"
    return "█"


def build_heatmap(
    db_path: str,
    pipeline: Optional[str] = None,
    limit: int = 500,
) -> List[HeatmapRow]:
    """Load recent history and bucket failures by hour-of-day per pipeline."""
    results = load_recent(db_path, pipeline=pipeline, limit=limit)
    rows: Dict[str, HeatmapRow] = {}

    for r in results:
        if r.is_healthy:
            continue
        name = r.pipeline
        if name not in rows:
            rows[name] = HeatmapRow(pipeline=name)
        try:
            dt = datetime.datetime.fromtimestamp(r.timestamp)
            rows[name].buckets[dt.hour] += 1
        except (OSError, ValueError, OverflowError):
            pass

    return sorted(rows.values(), key=lambda r: r.total_failures, reverse=True)


def print_heatmap(rows: List[HeatmapRow]) -> None:
    if not rows:
        print("No failure data available for heatmap.")
        return
    header = f"{'Pipeline':<30} [{'Hour (00-23)':^24}]  peak    failures"
    print(header)
    print("-" * len(header))
    for row in rows:
        print(row)
