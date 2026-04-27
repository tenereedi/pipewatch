"""Drift detection: compare current pipeline health rates against a stable baseline window."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_recent
from pipewatch.checks import CheckResult


_DEFAULT_BASELINE_LIMIT = 50
_DEFAULT_CURRENT_LIMIT = 10
_DEFAULT_DRIFT_THRESHOLD = 0.20  # 20 percentage-point drop triggers drift


@dataclass
class DriftResult:
    pipeline: str
    baseline_rate: float  # healthy fraction over baseline window
    current_rate: float   # healthy fraction over current window
    drift: float          # baseline_rate - current_rate  (positive = degraded)
    threshold: float
    flagged: bool

    def __str__(self) -> str:
        direction = "↓ DRIFT" if self.flagged else "OK"
        return (
            f"{self.pipeline}: baseline={self.baseline_rate:.1%}  "
            f"current={self.current_rate:.1%}  "
            f"drift={self.drift:+.1%}  [{direction}]"
        )


def _health_rate(results: List[CheckResult]) -> Optional[float]:
    if not results:
        return None
    return sum(1 for r in results if r.is_healthy()) / len(results)


def detect_drift(
    db_path: str,
    pipeline: str,
    *,
    baseline_limit: int = _DEFAULT_BASELINE_LIMIT,
    current_limit: int = _DEFAULT_CURRENT_LIMIT,
    threshold: float = _DEFAULT_DRIFT_THRESHOLD,
) -> Optional[DriftResult]:
    """Return a DriftResult for *pipeline*, or None if there is not enough data."""
    all_results = load_recent(db_path, pipeline=pipeline, limit=baseline_limit)
    if len(all_results) < baseline_limit // 2:
        return None

    current_results = all_results[:current_limit]
    baseline_results = all_results[current_limit:]

    current_rate = _health_rate(current_results)
    baseline_rate = _health_rate(baseline_results)

    if current_rate is None or baseline_rate is None:
        return None

    drift = baseline_rate - current_rate
    return DriftResult(
        pipeline=pipeline,
        baseline_rate=baseline_rate,
        current_rate=current_rate,
        drift=drift,
        threshold=threshold,
        flagged=drift >= threshold,
    )


def detect_all_drifts(
    db_path: str,
    pipelines: List[str],
    **kwargs,
) -> List[DriftResult]:
    """Run drift detection across all *pipelines*, skipping those with insufficient data."""
    results = []
    for name in pipelines:
        r = detect_drift(db_path, name, **kwargs)
        if r is not None:
            results.append(r)
    return results


def has_any_drift(results: List[DriftResult]) -> bool:
    return any(r.flagged for r in results)
