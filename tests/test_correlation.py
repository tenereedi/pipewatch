"""Tests for pipewatch.correlation."""

import os
import sqlite3
import tempfile
import time
from dataclasses import dataclass

import pytest

from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.correlation import (
    CorrelationPair,
    compute_correlations,
    print_correlations,
)


@pytest.fixture
def tmp_db(tmp_path):
    path = str(tmp_path / "test.db")
    init_db(path)
    return path


def _r(pipeline: str, healthy: bool, ts: str) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=ts,
    )


def _populate(db_path, rows):
    for r in rows:
        save_results(db_path, [r])


# ---------------------------------------------------------------------------


def test_compute_correlations_empty(tmp_db):
    pairs = compute_correlations(tmp_db)
    assert pairs == []


def test_compute_correlations_no_shared_failures(tmp_db):
    _populate(
        tmp_db,
        [
            _r("alpha", False, "2024-01-01 10:00:00"),
            _r("beta", False, "2024-01-01 10:01:00"),
        ],
    )
    # Different minute buckets → never co-fail
    pairs = compute_correlations(tmp_db, min_rate=0.5)
    assert pairs == []


def test_compute_correlations_detects_pair(tmp_db):
    ts = "2024-01-01 10:00:00"
    _populate(
        tmp_db,
        [
            _r("alpha", False, ts),
            _r("beta", False, ts),
        ],
    )
    pairs = compute_correlations(tmp_db, min_rate=0.5)
    assert len(pairs) == 1
    assert pairs[0].pipeline_a == "alpha"
    assert pairs[0].pipeline_b == "beta"
    assert pairs[0].co_failure_count == 1


def test_compute_correlations_min_rate_filters_results(tmp_db):
    """Pairs below min_rate threshold should be excluded from results."""
    ts1 = "2024-01-01 10:00:00"
    ts2 = "2024-01-01 10:01:00"
    # alpha and beta co-fail once out of two windows (50% rate)
    _populate(
        tmp_db,
        [
            _r("alpha", False, ts1),
            _r("beta", False, ts1),
            _r("alpha", False, ts2),
            _r("beta", True, ts2),
        ],
    )
    # With a high threshold, the pair should be excluded
    pairs_strict = compute_correlations(tmp_db, min_rate=0.9)
    assert pairs_strict == []

    # With a lower threshold, the pair should be included
    pairs_lenient = compute_correlations(tmp_db, min_rate=0.3)
    assert len(pairs_lenient) == 1


def test_co_failure_rate_calculation():
    p = CorrelationPair("a", "b", co_failure_count=3, total_windows=4)
    assert p.co_failure_rate == pytest.approx(0.75)


def test_co_failure_rate_zero_windows():
    p = CorrelationPair("a", "b", co_failure_count=0, total_windows=0)
    assert p.co_failure_rate == 0.0


def test_correlation_pair_str():
    p = CorrelationPair("svc-a", "svc-b", co_failure_count=2, total_windows=4)
    text = str(p)
    assert "svc-a" in text
    assert "svc-b" in text
    assert "50%" in text


def test_print_correlations_empty(capsys):
    print_correlations([])
    out = capsys.readouterr().out
    assert "No correlated" in out


def test_print_correlations_shows_rows(capsys):
    pairs = [CorrelationPair("alpha", "beta", 3, 4)]
    print_correlations(pairs)
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out
    assert "75%" in out
