"""Tests for pipewatch.anomaly."""
import pytest
from pipewatch.anomaly import detect_anomaly, detect_all_anomalies, AnomalyResult
from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
import tempfile, os


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def _r(pipeline: str, healthy: bool) -> CheckResult:
    return CheckResult(pipeline=pipeline, check="http", healthy=healthy, message="")


def _populate(db, pipeline, pattern):
    """pattern: list of booleans (True=healthy)."""
    for healthy in pattern:
        save_results(db, [_r(pipeline, healthy)])


def test_detect_anomaly_not_enough_data(tmp_db):
    _populate(tmp_db, "p1", [True, False])  # only 2 rows, need >=6
    result = detect_anomaly("p1", tmp_db, recent_window=5, baseline_window=30)
    assert result is None


def test_detect_anomaly_no_anomaly(tmp_db):
    # baseline mostly failing, recent also failing — no spike
    pattern = [False] * 4 + [False] * 10
    _populate(tmp_db, "pipe", pattern)
    result = detect_anomaly("pipe", tmp_db, recent_window=4, baseline_window=20, threshold=0.3)
    assert result is not None
    assert not result.is_anomaly


def test_detect_anomaly_is_anomaly(tmp_db):
    # recent: all fail (5/5), baseline: all healthy (10/10) → spike = 1.0
    pattern = [False] * 5 + [True] * 10
    _populate(tmp_db, "pipe2", pattern)
    result = detect_anomaly("pipe2", tmp_db, recent_window=5, baseline_window=20, threshold=0.3)
    assert result is not None
    assert result.is_anomaly
    assert result.recent_failure_rate == 1.0
    assert result.baseline_failure_rate == 0.0


def test_anomaly_result_str_anomaly(tmp_db):
    pattern = [False] * 5 + [True] * 10
    _populate(tmp_db, "pipe3", pattern)
    result = detect_anomaly("pipe3", tmp_db, recent_window=5, baseline_window=20, threshold=0.3)
    assert "ANOMALY" in str(result)
    assert "pipe3" in str(result)


def test_anomaly_result_str_ok(tmp_db):
    pattern = [True] * 5 + [True] * 10
    _populate(tmp_db, "pipe4", pattern)
    result = detect_anomaly("pipe4", tmp_db, recent_window=5, baseline_window=20, threshold=0.3)
    assert result is not None
    assert "OK" in str(result)


def test_detect_all_anomalies(tmp_db):
    _populate(tmp_db, "a", [False] * 5 + [True] * 10)
    _populate(tmp_db, "b", [True] * 5 + [True] * 10)
    results = detect_all_anomalies(["a", "b"], tmp_db, recent_window=5, baseline_window=20, threshold=0.3)
    assert len(results) == 2
    anomalies = [r for r in results if r.is_anomaly]
    assert len(anomalies) == 1
    assert anomalies[0].pipeline == "a"
