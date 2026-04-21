"""Tests for pipewatch.replay."""

from __future__ import annotations

import pytest

from pipewatch.checks import CheckResult
from pipewatch.history import init_db, save_results
from pipewatch.replay import (
    ReplayWindow,
    load_replay_window,
    replay_all,
    replay_summary,
)


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "test_history.db")
    init_db(db)
    return db


def _r(name: str, healthy: bool, pipeline: str = "pipe1") -> CheckResult:
    return CheckResult(
        name=name,
        pipeline=pipeline,
        status="ok" if healthy else "fail",
        message="ok" if healthy else "error",
    )


def test_load_replay_window_empty(tmp_db):
    window = load_replay_window(tmp_db, "missing", limit=10)
    assert window.pipeline == "missing"
    assert window.total == 0
    assert window.healthy_count == 0
    assert window.failure_count == 0


def test_load_replay_window_returns_results(tmp_db):
    results = [_r("http", True), _r("freshness", False)]
    save_results(tmp_db, results)
    window = load_replay_window(tmp_db, "pipe1", limit=10)
    assert window.total == 2
    assert window.healthy_count == 1
    assert window.failure_count == 1


def test_load_replay_window_respects_limit(tmp_db):
    results = [_r(f"check_{i}", True) for i in range(20)]
    save_results(tmp_db, results)
    window = load_replay_window(tmp_db, "pipe1", limit=5)
    assert window.total <= 5


def test_invalid_limit_raises(tmp_db):
    with pytest.raises(ValueError, match="limit must be >= 1"):
        load_replay_window(tmp_db, "pipe1", limit=0)


def test_replay_summary_empty():
    window = ReplayWindow(pipeline="p", results=[])
    summary = replay_summary(window)
    assert "No historical data" in summary


def test_replay_summary_with_data(tmp_db):
    results = [_r("a", True), _r("b", True), _r("c", False)]
    save_results(tmp_db, results)
    window = load_replay_window(tmp_db, "pipe1", limit=10)
    summary = replay_summary(window)
    assert "pipe1" in summary
    assert "3 result" in summary
    assert "2 healthy" in summary


def test_replay_all_multiple_pipelines(tmp_db):
    save_results(tmp_db, [_r("x", True, "alpha"), _r("y", False, "beta")])
    windows = replay_all(tmp_db, ["alpha", "beta"], limit=10)
    assert len(windows) == 2
    names = {w.pipeline for w in windows}
    assert names == {"alpha", "beta"}


def test_window_str():
    window = ReplayWindow(pipeline="mypipe", results=[])
    assert "mypipe" in str(window)
