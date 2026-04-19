"""Anomaly detection: flag pipelines whose recent failure rate spikes above historical baseline."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from pipewatch.history import load_recent


@dataclass
class AnomalyResult:
    pipeline: str
    recent_failure_rate: float
    baseline_failure_rate: float
    threshold: float
    is_anomaly: bool

    def __str__(self) -> str:
        status = "ANOMALY" if self.is_anomaly else "OK"
        return (
            f"[{status}] {self.pipeline}: recent={self.recent_failure_rate:.0%} "
            f"baseline={self.baseline_failure_rate:.0%} threshold=+{self.threshold:.0%}"
        )


def _failure_rate(results: list) -> float:
    if not results:
        return 0.0
    return sum(1 for r in results if not r["healthy"]) / len(results)


def detect_anomaly(
    pipeline: str,
    db_path: str,
    recent_window: int = 5,
    baseline_window: int = 30,
    threshold: float = 0.3,
) -> Optional[AnomalyResult]:
    """Return AnomalyResult if enough data exists, else None."""
    all_results = load_recent(db_path, pipeline=pipeline, limit=baseline_window)
    if len(all_results) < recent_window + 1:
        return None

    recent = all_results[:recent_window]
    baseline = all_results[recent_window:]

    recent_rate = _failure_rate(recent)
    baseline_rate = _failure_rate(baseline)
    spike = recent_rate - baseline_rate
    is_anomaly = spike >= threshold

    return AnomalyResult(
        pipeline=pipeline,
        recent_failure_rate=recent_rate,
        baseline_failure_rate=baseline_rate,
        threshold=threshold,
        is_anomaly=is_anomaly,
    )


def detect_all_anomalies(
    pipelines: list[str],
    db_path: str,
    recent_window: int = 5,
    baseline_window: int = 30,
    threshold: float = 0.3,
) -> list[AnomalyResult]:
    results = []
    for p in pipelines:
        r = detect_anomaly(p, db_path, recent_window, baseline_window, threshold)
        if r is not None:
            results.append(r)
    return results
