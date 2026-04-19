"""Tests for pipewatch.baseline and pipewatch.cli_baseline."""

import pytest
import sqlite3
from pathlib import Path
from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.baseline import compute_baseline, check_all_baselines, BaselineReport
from pipewatch.cli_baseline import handle_baseline
import argparse


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def _r(pipeline: str, healthy: bool) -> CheckResult:
    return CheckResult(pipeline=pipeline, check="http", healthy=healthy, message="")


def _populate(db, pipeline, healthy_count, unhealthy_count):
    results = [_r(pipeline, True)] * healthy_count + [_r(pipeline, False)] * unhealthy_count
    save_results(db, results)


def test_compute_baseline_not_enough_data(tmp_db):
    _populate(tmp_db, "pipe_a", 5, 0)
    report = compute_baseline("pipe_a", tmp_db, baseline_window=100, current_window=20)
    assert report is None


def test_compute_baseline_all_healthy(tmp_db):
    _populate(tmp_db, "pipe_a", 100, 0)
    report = compute_baseline("pipe_a", tmp_db, baseline_window=100, current_window=20)
    assert report is not None
    assert report.expected_success_rate == pytest.approx(1.0)
    assert report.current_success_rate == pytest.approx(1.0)
    assert not report.is_degraded()


def test_compute_baseline_degraded(tmp_db):
    # First 20 (most recent) are failures, rest healthy
    save_results(tmp_db, [_r("pipe_b", False)] * 20)
    save_results(tmp_db, [_r("pipe_b", True)] * 80)
    report = compute_baseline("pipe_b", tmp_db, baseline_window=100, current_window=20)
    assert report is not None
    assert report.is_degraded(threshold=0.10)
    assert "DEGRADED" in str(report)


def test_check_all_baselines_filters_missing(tmp_db):
    _populate(tmp_db, "pipe_c", 100, 0)
    reports = check_all_baselines(["pipe_c", "nonexistent"], tmp_db)
    assert len(reports) == 1
    assert reports[0].pipeline == "pipe_c"


def test_handle_baseline_no_history(tmp_db, capsys):
    args = argparse.Namespace(
        db=tmp_db, pipeline=None, baseline_window=100, current_window=20, threshold=0.10
    )
    result = handle_baseline(args)
    assert result is True
    captured = capsys.readouterr()
    assert "No pipeline history" in captured.out


def test_handle_baseline_degraded_returns_false(tmp_db):
    save_results(tmp_db, [_r("pipe_d", False)] * 20)
    save_results(tmp_db, [_r("pipe_d", True)] * 80)
    args = argparse.Namespace(
        db=tmp_db, pipeline="pipe_d", baseline_window=100, current_window=20, threshold=0.10
    )
    result = handle_baseline(args)
    assert result is False
