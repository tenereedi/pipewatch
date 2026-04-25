"""Tests for pipewatch.rollup and pipewatch.cli_rollup."""

from __future__ import annotations

import time
import pytest

from pipewatch.checks import CheckResult
from pipewatch.history import init_db, save_results
from pipewatch.rollup import compute_rollup, print_rollup, RollupBucket


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def _r(pipeline: str, healthy: bool, age: float = 0.0) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=time.time() - age,
    )


def _populate(db, pipeline, statuses, age=0.0):
    results = [_r(pipeline, s, age) for s in statuses]
    save_results(db, results)


# ---------------------------------------------------------------------------
# compute_rollup
# ---------------------------------------------------------------------------

def test_compute_rollup_empty(tmp_db):
    buckets = compute_rollup(tmp_db, window="1h")
    assert buckets == []


def test_compute_rollup_all_healthy(tmp_db):
    _populate(tmp_db, "pipe-a", [True, True, True])
    buckets = compute_rollup(tmp_db, window="1h")
    assert len(buckets) == 1
    b = buckets[0]
    assert b.pipeline == "pipe-a"
    assert b.total == 3
    assert b.healthy == 3
    assert b.unhealthy == 0
    assert b.health_rate == pytest.approx(1.0)


def test_compute_rollup_mixed(tmp_db):
    _populate(tmp_db, "pipe-b", [True, False, False, True])
    buckets = compute_rollup(tmp_db, window="1h")
    b = buckets[0]
    assert b.healthy == 2
    assert b.unhealthy == 2
    assert b.health_rate == pytest.approx(0.5)


def test_compute_rollup_excludes_old_results(tmp_db):
    # results older than 1 h should be excluded
    _populate(tmp_db, "pipe-c", [False, False], age=7200)  # 2 h ago
    _populate(tmp_db, "pipe-c", [True], age=60)            # 1 min ago
    buckets = compute_rollup(tmp_db, window="1h")
    assert len(buckets) == 1
    assert buckets[0].total == 1
    assert buckets[0].healthy == 1


def test_compute_rollup_invalid_window(tmp_db):
    with pytest.raises(ValueError, match="Unknown window"):
        compute_rollup(tmp_db, window="99h")


def test_compute_rollup_filter_by_pipeline(tmp_db):
    _populate(tmp_db, "alpha", [True, False])
    _populate(tmp_db, "beta", [True, True])
    buckets = compute_rollup(tmp_db, pipeline="alpha", window="1h")
    assert len(buckets) == 1
    assert buckets[0].pipeline == "alpha"


# ---------------------------------------------------------------------------
# RollupBucket.__str__
# ---------------------------------------------------------------------------

def test_rollup_bucket_str():
    b = RollupBucket(pipeline="my-pipe", window_label="6h", total=10, healthy=8, unhealthy=2)
    s = str(b)
    assert "my-pipe" in s
    assert "6h" in s
    assert "80.0%" in s


# ---------------------------------------------------------------------------
# print_rollup (smoke)
# ---------------------------------------------------------------------------

def test_print_rollup_empty(capsys):
    print_rollup([])
    out = capsys.readouterr().out
    assert "No data" in out


def test_print_rollup_shows_rows(capsys, tmp_db):
    _populate(tmp_db, "pipe-x", [True, False])
    buckets = compute_rollup(tmp_db, window="1h")
    print_rollup(buckets)
    out = capsys.readouterr().out
    assert "pipe-x" in out
