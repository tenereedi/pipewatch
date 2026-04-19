"""Tests for pipewatch.trending module."""
import pytest
from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.trending import compute_trend, has_any_trending_down, TrendSummary


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def _r(pipeline, check_type, healthy, message="ok"):
    return CheckResult(pipeline=pipeline, check_type=check_type,
                       healthy=healthy, message=message)


def test_compute_trend_empty(tmp_db):
    result = compute_trend(tmp_db)
    assert result == []


def test_compute_trend_all_healthy(tmp_db):
    results = [_r("pipe1", "http", True) for _ in range(10)]
    save_results(tmp_db, results)
    summaries = compute_trend(tmp_db, pipeline="pipe1")
    assert len(summaries) == 1
    s = summaries[0]
    assert s.failures == 0
    assert s.failure_rate == 0.0
    assert not s.trending_down


def test_compute_trend_high_failure_rate(tmp_db):
    results = ([_r("pipe2", "freshness", False)] * 8 +
               [_r("pipe2", "freshness", True)] * 2)
    save_results(tmp_db, results)
    summaries = compute_trend(tmp_db, pipeline="pipe2")
    assert len(summaries) == 1
    s = summaries[0]
    assert s.failure_rate == pytest.approx(0.8)
    assert s.trending_down


def test_compute_trend_threshold_boundary(tmp_db):
    results = ([_r("pipe3", "http", False)] * 4 +
               [_r("pipe3", "http", True)] * 6)
    save_results(tmp_db, results)
    summaries = compute_trend(tmp_db, pipeline="pipe3", threshold=0.5)
    assert not summaries[0].trending_down
    summaries2 = compute_trend(tmp_db, pipeline="pipe3", threshold=0.3)
    assert summaries2[0].trending_down


def test_has_any_trending_down_false():
    s = TrendSummary("p", "http", 10, 1, 0.1, False)
    assert not has_any_trending_down([s])


def test_has_any_trending_down_true():
    s = TrendSummary("p", "http", 10, 6, 0.6, True)
    assert has_any_trending_down([s])


def test_trend_summary_str_contains_pipeline():
    s = TrendSummary("mypipe", "http", 10, 5, 0.5, True)
    assert "mypipe" in str(s)
    assert "TRENDING DOWN" in str(s)
