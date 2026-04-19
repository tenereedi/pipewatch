"""Baseline comparison: detect when pipelines deviate from their historical norm."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from pipewatch.history import load_recent
from pipewatch.checks import CheckResult


@dataclass
class BaselineReport:
    pipeline: str
    expected_success_rate: float  # 0.0 - 1.0
    current_success_rate: float
    sample_size: int
    deviation: float  # current - expected

    def is_degraded(self, threshold: float = 0.10) -> bool:
        """Return True if success rate dropped more than threshold below baseline."""
        return self.deviation < -threshold

    def __str__(self) -> str:
        status = "DEGRADED" if self.is_degraded() else "OK"
        return (
            f"[{status}] {self.pipeline}: "
            f"baseline={self.expected_success_rate:.0%} "
            f"current={self.current_success_rate:.0%} "
            f"(n={self.sample_size})"
        )


def compute_baseline(
    pipeline: str,
    db_path: str,
    baseline_window: int = 100,
    current_window: int = 20,
) -> Optional[BaselineReport]:
    """Compare recent results against a longer historical baseline."""
    baseline_rows = load_recent(db_path, pipeline=pipeline, limit=baseline_window)
    if len(baseline_rows) < current_window:
        return None

    current_rows = baseline_rows[:current_window]
    baseline_total = len(baseline_rows)
    current_total = len(current_rows)

    baseline_rate = sum(1 for r in baseline_rows if r["healthy"]) / baseline_total
    current_rate = sum(1 for r in current_rows if r["healthy"]) / current_total

    return BaselineReport(
        pipeline=pipeline,
        expected_success_rate=baseline_rate,
        current_success_rate=current_rate,
        sample_size=baseline_total,
        deviation=current_rate - baseline_rate,
    )


def check_all_baselines(
    pipelines: list[str],
    db_path: str,
    baseline_window: int = 100,
    current_window: int = 20,
) -> list[BaselineReport]:
    reports = []
    for name in pipelines:
        report = compute_baseline(name, db_path, baseline_window, current_window)
        if report is not None:
            reports.append(report)
    return reports
