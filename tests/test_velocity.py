"""Tests for pipewatch.velocity."""
from __future__ import annotations

import time
import sqlite3
import tempfile
import os
import pytest

from pipewatch.checks import CheckResult
from pipewatch.history import init_db, save_results
from pipewatch.velocity import (
    VelocityResult,
    compute_velocity,
    compute_all_velocities,
)


@pytest.fixture()
def tmp_db(tmp_path):
    path = str(tmp_path / "test.db")
    init_db(path)
    return path


def _r(pipeline: str, healthy: bool, ts: float | None = None) -> CheckResult:
    r = CheckResult(pipeline=pipeline, healthy=healthy, message="ok" if healthy else "fail")
    if ts is not None:
        object.__setattr__(r, "timestamp", ts)
    return r


def _populate(db, pipeline, pattern, base_ts=None):
    """Insert results according to a list of booleans (True=healthy)."""
    now = base_ts or time.time()
    results = [
        _r(pipeline, healthy, ts=now - (len(pattern) - i) * 60)
        for i, healthy in enumerate(pattern)
    ]
    save_results(db, results)


def test_compute_velocity_not_enough_data(tmp_db):
    _populate(tmp_db, "pipe", [True, False, True])  # only 3 rows
    result = compute_velocity("pipe", tmp_db, window=40, min_samples=10)
    assert result is None


def test_compute_velocity_all_healthy(tmp_db):
    _populate(tmp_db, "pipe", [True] * 20)
    result = compute_velocity("pipe", tmp_db, window=20, min_samples=10)
    assert result is not None
    assert result.early_failure_rate == 0.0
    assert result.recent_failure_rate == 0.0
    assert result.delta == pytest.approx(0.0)
    assert not result.is_accelerating


def test_compute_velocity_worsening(tmp_db):
    # early half: all healthy; recent half: all failing
    pattern = [True] * 10 + [False] * 10
    _populate(tmp_db, "pipe", pattern)
    result = compute_velocity("pipe", tmp_db, window=20, min_samples=10)
    assert result is not None
    assert result.delta > 0
    assert result.is_accelerating


def test_compute_velocity_improving(tmp_db):
    # early half: all failing; recent half: all healthy
    pattern = [False] * 10 + [True] * 10
    _populate(tmp_db, "pipe", pattern)
    result = compute_velocity("pipe", tmp_db, window=20, min_samples=10)
    assert result is not None
    assert result.delta < 0
    assert not result.is_accelerating


def test_velocity_result_str_contains_pipeline(tmp_db):
    _populate(tmp_db, "my-pipe", [True] * 10 + [False] * 10)
    result = compute_velocity("my-pipe", tmp_db, window=20, min_samples=10)
    assert result is not None
    assert "my-pipe" in str(result)
    assert "worsening" in str(result)


def test_compute_all_velocities_skips_insufficient(tmp_db):
    _populate(tmp_db, "big", [True] * 20)
    _populate(tmp_db, "small", [True] * 3)
    results = compute_all_velocities(tmp_db, ["big", "small"], window=20, min_samples=10)
    assert len(results) == 1
    assert results[0].pipeline == "big"


def test_compute_all_velocities_empty_pipelines(tmp_db):
    results = compute_all_velocities(tmp_db, [], window=20, min_samples=10)
    assert results == []
