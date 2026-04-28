"""Velocity tracking: measures how quickly failure rates are changing over time."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_recent


@dataclass
class VelocityResult:
    pipeline: str
    window_size: int
    early_failure_rate: float
    recent_failure_rate: float
    delta: float  # positive = getting worse, negative = improving

    def __str__(self) -> str:
        direction = "↑ worsening" if self.delta > 0.05 else ("↓ improving" if self.delta < -0.05 else "→ stable")
        return (
            f"{self.pipeline}: early={self.early_failure_rate:.0%} "
            f"recent={self.recent_failure_rate:.0%} "
            f"delta={self.delta:+.0%} [{direction}]"
        )

    @property
    def is_accelerating(self) -> bool:
        """True when failure rate is meaningfully increasing."""
        return self.delta > 0.10


def _failure_rate(results: list) -> float:
    if not results:
        return 0.0
    failures = sum(1 for r in results if not r.healthy)
    return failures / len(results)


def compute_velocity(
    pipeline: str,
    db_path: str,
    window: int = 40,
    min_samples: int = 10,
) -> Optional[VelocityResult]:
    """Split recent history into two halves and compare failure rates."""
    rows = load_recent(db_path, pipeline=pipeline, limit=window)
    if len(rows) < min_samples:
        return None

    mid = len(rows) // 2
    # load_recent returns newest-first; older half is the tail
    recent_half = rows[:mid]
    early_half = rows[mid:]

    early_rate = _failure_rate(early_half)
    recent_rate = _failure_rate(recent_half)
    delta = recent_rate - early_rate

    return VelocityResult(
        pipeline=pipeline,
        window_size=len(rows),
        early_failure_rate=early_rate,
        recent_failure_rate=recent_rate,
        delta=delta,
    )


def compute_all_velocities(
    db_path: str,
    pipelines: List[str],
    window: int = 40,
    min_samples: int = 10,
) -> List[VelocityResult]:
    results = []
    for name in pipelines:
        v = compute_velocity(name, db_path, window=window, min_samples=min_samples)
        if v is not None:
            results.append(v)
    return results
