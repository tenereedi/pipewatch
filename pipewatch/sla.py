"""SLA tracking: check whether pipelines meet uptime/success-rate targets."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_recent
from pipewatch.checks import CheckResult


@dataclass
class SLAPolicy:
    pipeline: str
    target_rate: float          # 0.0 – 1.0, e.g. 0.99 for 99 %
    window_minutes: int = 60

    def __post_init__(self) -> None:
        if not (0.0 < self.target_rate <= 1.0):
            raise ValueError("target_rate must be between 0 (exclusive) and 1 (inclusive)")
        if self.window_minutes <= 0:
            raise ValueError("window_minutes must be positive")


@dataclass
class SLAResult:
    policy: SLAPolicy
    total: int
    healthy: int
    actual_rate: float
    met: bool

    def __str__(self) -> str:
        status = "OK" if self.met else "BREACH"
        return (
            f"[{status}] {self.policy.pipeline}: "
            f"{self.actual_rate:.1%} actual vs {self.policy.target_rate:.1%} target "
            f"({self.healthy}/{self.total} healthy in last {self.policy.window_minutes}m)"
        )


def check_sla(policy: SLAPolicy, db_path: str) -> SLAResult:
    """Evaluate a single SLA policy against stored history."""
    limit = policy.window_minutes * 10          # generous upper bound on samples
    rows: List[CheckResult] = load_recent(
        db_path, pipeline=policy.pipeline, limit=limit
    )

    # Filter to the time window
    import time
    cutoff = time.time() - policy.window_minutes * 60
    rows = [r for r in rows if r.timestamp >= cutoff]

    total = len(rows)
    healthy = sum(1 for r in rows if r.healthy)
    actual_rate = (healthy / total) if total > 0 else 1.0
    met = actual_rate >= policy.target_rate

    return SLAResult(
        policy=policy,
        total=total,
        healthy=healthy,
        actual_rate=actual_rate,
        met=met,
    )


def check_all_slas(
    policies: List[SLAPolicy], db_path: str
) -> List[SLAResult]:
    """Evaluate every SLA policy and return all results."""
    return [check_sla(p, db_path) for p in policies]


def any_sla_breached(results: List[SLAResult]) -> bool:
    return any(not r.met for r in results)
