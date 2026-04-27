"""Tests for pipewatch.drift."""

from __future__ import annotations

import time
import pytest

from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.drift import (
    detect_drift,
    detect_all_drifts,
    has_any_drift,
    DriftResult,
)


@pytest.fixture()
def tmp_db(tmp_path):
    path = str(tmp_path / "history.db")
    init_db(path)
    return path


def _r(name: str, healthy: bool, ts: float | None = None) -> CheckResult:
    return CheckResult(
        pipeline=name,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=ts or time.time(),
    )


def _populate(db, name, pattern, base_ts=None):
    """Insert results matching *pattern* (True/False list) oldest-first."""
    base = base_ts or time.time()
    results = [
        _r(name, ok, ts=base - (len(pattern) - i) * 60)
        for i, ok in enumerate(pattern)
    ]
    save_results(db, results)


def test_detect_drift_not_enough_data(tmp_db):
    _populate(tmp_db, "pipe", [True] * 10)  # fewer than baseline_limit // 2
    result = detect_drift(tmp_db, "pipe", baseline_limit=50, current_limit=10)
    assert result is None


def test_detect_drift_no_drift(tmp_db):
    # All healthy — no drift expected
    _populate(tmp_db, "pipe", [True] * 50)
    result = detect_drift(tmp_db, "pipe", baseline_limit=50, current_limit=10, threshold=0.20)
    assert result is not None
    assert not result.flagged
    assert result.drift < 0.20


def test_detect_drift_flagged(tmp_db):
    # Baseline mostly healthy, current all failing
    baseline = [True] * 40
    current = [False] * 10
    _populate(tmp_db, "pipe", baseline + current)  # oldest first
    result = detect_drift(tmp_db, "pipe", baseline_limit=50, current_limit=10, threshold=0.20)
    assert result is not None
    assert result.flagged
    assert result.drift >= 0.20


def test_drift_result_str_contains_pipeline(tmp_db):
    _populate(tmp_db, "mypipe", [True] * 40 + [False] * 10)
    result = detect_drift(tmp_db, "mypipe", baseline_limit=50, current_limit=10)
    assert result is not None
    assert "mypipe" in str(result)


def test_detect_all_drifts_skips_insufficient(tmp_db):
    _populate(tmp_db, "rich", [True] * 50)
    _populate(tmp_db, "sparse", [True] * 5)
    results = detect_all_drifts(tmp_db, ["rich", "sparse"], baseline_limit=50, current_limit=10)
    names = [r.pipeline for r in results]
    assert "rich" in names
    assert "sparse" not in names


def test_has_any_drift_true():
    dr = DriftResult("p", 0.9, 0.5, 0.4, 0.2, flagged=True)
    assert has_any_drift([dr])


def test_has_any_drift_false():
    dr = DriftResult("p", 0.9, 0.85, 0.05, 0.2, flagged=False)
    assert not has_any_drift([dr])
