"""Failure rate forecasting based on recent history trends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_recent


@dataclass
class ForecastResult:
    pipeline: str
    window_size: int
    recent_failure_rate: float  # last half of window
    early_failure_rate: float   # first half of window
    trend_delta: float          # recent - early (positive = worsening)
    predicted_failure_rate: float

    def __str__(self) -> str:
        direction = "↑ worsening" if self.trend_delta > 0.05 else (
            "↓ improving" if self.trend_delta < -0.05 else "→ stable"
        )
        return (
            f"{self.pipeline}: predicted={self.predicted_failure_rate:.1%} "
            f"({direction}, delta={self.trend_delta:+.1%})"
        )

    @property
    def is_at_risk(self) -> bool:
        return self.predicted_failure_rate > 0.3 or self.trend_delta > 0.15


def _failure_rate(results: list) -> float:
    if not results:
        return 0.0
    return sum(1 for r in results if not r.healthy) / len(results)


def forecast_pipeline(
    pipeline: str,
    db_path: str,
    window: int = 40,
    min_records: int = 10,
) -> Optional[ForecastResult]:
    """Forecast failure rate for a single pipeline using linear extrapolation."""
    records = load_recent(db_path, pipeline=pipeline, limit=window)
    if len(records) < min_records:
        return None

    mid = len(records) // 2
    early = records[mid:]   # older (load_recent returns newest first)
    recent = records[:mid]  # newer

    early_rate = _failure_rate(early)
    recent_rate = _failure_rate(recent)
    delta = recent_rate - early_rate
    predicted = max(0.0, min(1.0, recent_rate + delta))

    return ForecastResult(
        pipeline=pipeline,
        window_size=len(records),
        recent_failure_rate=recent_rate,
        early_failure_rate=early_rate,
        trend_delta=delta,
        predicted_failure_rate=predicted,
    )


def forecast_all(
    db_path: str,
    pipelines: List[str],
    window: int = 40,
    min_records: int = 10,
) -> List[ForecastResult]:
    """Run forecasts for all given pipeline names."""
    results = []
    for name in pipelines:
        r = forecast_pipeline(name, db_path, window=window, min_records=min_records)
        if r is not None:
            results.append(r)
    return results
