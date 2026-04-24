"""Tests for pipewatch/forecast.py"""

from __future__ import annotations

import time
import sqlite3
import pytest

from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.forecast import forecast_pipeline, forecast_all, ForecastResult


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "history.db")
    init_db(db)
    return db


def _r(pipeline: str, healthy: bool, ts: float | None = None) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=ts or time.time(),
    )


def _populate(db: str, pipeline: str, pattern: list[bool]) -> None:
    """Save results with evenly spaced timestamps so ordering is deterministic."""
    base = time.time() - len(pattern) * 10
    results = [
        _r(pipeline, healthy, ts=base + i * 10)
        for i, healthy in enumerate(pattern)
    ]
    save_results(db, results)


def test_forecast_not_enough_data(tmp_db):
    _populate(tmp_db, "pipe-a", [True, False, True])  # only 3 records
    result = forecast_pipeline("pipe-a", tmp_db, window=40, min_records=10)
    assert result is None


def test_forecast_all_healthy(tmp_db):
    _populate(tmp_db, "pipe-a", [True] * 20)
    result = forecast_pipeline("pipe-a", tmp_db, window=20, min_records=10)
    assert result is not None
    assert result.predicted_failure_rate == pytest.approx(0.0)
    assert result.trend_delta == pytest.approx(0.0)
    assert not result.is_at_risk


def test_forecast_worsening_trend(tmp_db):
    # early: all healthy, recent: all failing  => strong positive delta
    pattern = [True] * 20 + [False] * 20
    _populate(tmp_db, "pipe-b", pattern)
    result = forecast_pipeline("pipe-b", tmp_db, window=40, min_records=10)
    assert result is not None
    assert result.trend_delta > 0.4
    assert result.is_at_risk


def test_forecast_improving_trend(tmp_db):
    # early: all failing, recent: all healthy => negative delta
    pattern = [False] * 20 + [True] * 20
    _populate(tmp_db, "pipe-c", pattern)
    result = forecast_pipeline("pipe-c", tmp_db, window=40, min_records=10)
    assert result is not None
    assert result.trend_delta < -0.4
    assert not result.is_at_risk


def test_forecast_all_returns_multiple(tmp_db):
    _populate(tmp_db, "alpha", [True] * 20)
    _populate(tmp_db, "beta", [False] * 20)
    forecasts = forecast_all(tmp_db, ["alpha", "beta"], window=20, min_records=10)
    assert len(forecasts) == 2
    names = {f.pipeline for f in forecasts}
    assert names == {"alpha", "beta"}


def test_forecast_all_skips_insufficient(tmp_db):
    _populate(tmp_db, "sparse", [True] * 3)
    _populate(tmp_db, "rich", [True] * 20)
    forecasts = forecast_all(tmp_db, ["sparse", "rich"], window=20, min_records=10)
    assert len(forecasts) == 1
    assert forecasts[0].pipeline == "rich"


def test_forecast_result_str(tmp_db):
    _populate(tmp_db, "pipe-x", [True] * 20)
    result = forecast_pipeline("pipe-x", tmp_db, window=20, min_records=10)
    assert result is not None
    s = str(result)
    assert "pipe-x" in s
    assert "predicted" in s
