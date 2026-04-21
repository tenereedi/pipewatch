"""Correlation: detect pipelines that tend to fail together."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from pipewatch.history import load_recent


@dataclass
class CorrelationPair:
    pipeline_a: str
    pipeline_b: str
    co_failure_count: int
    total_windows: int

    @property
    def co_failure_rate(self) -> float:
        if self.total_windows == 0:
            return 0.0
        return self.co_failure_count / self.total_windows

    def __str__(self) -> str:
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b}: "
            f"{self.co_failure_count}/{self.total_windows} windows "
            f"({self.co_failure_rate:.0%} co-failure rate)"
        )


def _bucket_by_minute(results) -> Dict[str, set]:
    """Group result pipeline names by truncated-to-minute timestamp bucket."""
    buckets: Dict[str, set] = {}
    for r in results:
        bucket = str(r.timestamp)[:16]  # "YYYY-MM-DD HH:MM"
        buckets.setdefault(bucket, set())
        if not r.healthy:
            buckets[bucket].add(r.pipeline)
    return buckets


def compute_correlations(
    db_path: str,
    limit: int = 200,
    min_rate: float = 0.5,
) -> List[CorrelationPair]:
    """Return pipeline pairs whose co-failure rate meets *min_rate*."""
    results = load_recent(db_path, limit=limit)
    buckets = _bucket_by_minute(results)

    pair_failures: Dict[Tuple[str, str], int] = {}
    total_windows = len(buckets)

    for failed_set in buckets.values():
        names = sorted(failed_set)
        for i, a in enumerate(names):
            for b in names[i + 1 :]:
                key = (a, b)
                pair_failures[key] = pair_failures.get(key, 0) + 1

    pairs: List[CorrelationPair] = []
    for (a, b), count in pair_failures.items():
        rate = count / total_windows if total_windows else 0.0
        if rate >= min_rate:
            pairs.append(CorrelationPair(a, b, count, total_windows))

    pairs.sort(key=lambda p: p.co_failure_rate, reverse=True)
    return pairs


def print_correlations(pairs: List[CorrelationPair]) -> None:
    if not pairs:
        print("No correlated failures found.")
        return
    print(f"{'Pipeline A':<25} {'Pipeline B':<25} {'Co-failures':>12} {'Rate':>8}")
    print("-" * 74)
    for p in pairs:
        print(
            f"{p.pipeline_a:<25} {p.pipeline_b:<25} "
            f"{p.co_failure_count:>12} {p.co_failure_rate:>7.0%}"
        )
