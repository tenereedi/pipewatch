"""Daily/periodic digest report summarizing pipeline health across all sources."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.history import load_recent
from pipewatch.trending import compute_trend, TrendSummary


@dataclass
class DigestEntry:
    pipeline: str
    total_checks: int
    healthy: int
    unhealthy: int
    failure_rate: float
    trend: TrendSummary

    def __str__(self) -> str:
        trend_str = str(self.trend)
        return (
            f"{self.pipeline}: {self.healthy}/{self.total_checks} healthy "
            f"({self.failure_rate:.0%} failure) [{trend_str}]"
        )


def build_digest(db_path: str, hours: int = 24, pipeline: Optional[str] = None) -> List[DigestEntry]:
    """Build digest entries for all pipelines (or a single one) over the last `hours`."""
    results = load_recent(db_path, hours=hours, pipeline=pipeline)
    if not results:
        return []

    pipelines: dict[str, list] = {}
    for r in results:
        pipelines.setdefault(r.pipeline, []).append(r)

    entries = []
    for name, checks in sorted(pipelines.items()):
        total = len(checks)
        healthy = sum(1 for r in checks if r.ok)
        unhealthy = total - healthy
        failure_rate = unhealthy / total if total else 0.0
        trend = compute_trend(db_path, name, hours=hours)
        entries.append(DigestEntry(
            pipeline=name,
            total_checks=total,
            healthy=healthy,
            unhealthy=unhealthy,
            failure_rate=failure_rate,
            trend=trend,
        ))
    return entries


def print_digest(db_path: str, hours: int = 24, pipeline: Optional[str] = None) -> None:
    """Print a formatted digest report to stdout."""
    entries = build_digest(db_path, hours=hours, pipeline=pipeline)
    print(f"\n=== PipeWatch Digest (last {hours}h) ===")
    if not entries:
        print("  No data available.")
        return
    for entry in entries:
        marker = "✗" if entry.failure_rate > 0 else "✓"
        print(f"  [{marker}] {entry}")
    total_pipelines = len(entries)
    degraded = sum(1 for e in entries if e.failure_rate > 0)
    print(f"\nSummary: {total_pipelines} pipelines, {degraded} degraded.\n")
