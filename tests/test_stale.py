"""Tests for pipewatch.stale."""
from __future__ import annotations

import time
import pytest

from pipewatch.checks import CheckResult
from pipewatch.history import init_db, save_results
from pipewatch.stale import (
    StalenessResult,
    check_staleness,
    check_all_staleness,
)


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "history.db")
    init_db(db)
    return db


def _r(pipeline: str, healthy: bool = True, ts: float | None = None) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=ts if ts is not None else time.time(),
    )


def test_invalid_threshold_raises(tmp_db):
    with pytest.raises(ValueError):
        check_staleness(tmp_db, "pipe", threshold_seconds=0)


def test_never_seen_is_not_stale(tmp_db):
    result = check_staleness(tmp_db, "ghost", threshold_seconds=60)
    assert result.pipeline == "ghost"
    assert result.last_seen is None
    assert result.is_stale is False


def test_recent_result_is_not_stale(tmp_db):
    save_results(tmp_db, [_r("pipe-a", ts=time.time())])
    result = check_staleness(tmp_db, "pipe-a", threshold_seconds=300)
    assert result.is_stale is False


def test_old_result_is_stale(tmp_db):
    old_ts = time.time() - 600  # 10 minutes ago
    save_results(tmp_db, [_r("pipe-b", ts=old_ts)])
    result = check_staleness(tmp_db, "pipe-b", threshold_seconds=300)
    assert result.is_stale is True
    assert result.last_seen == pytest.approx(old_ts, abs=1)


def test_check_all_staleness_mixed(tmp_db):
    now = time.time()
    save_results(tmp_db, [_r("fresh", ts=now)])
    save_results(tmp_db, [_r("stale", ts=now - 1000)])

    results = check_all_staleness(tmp_db, ["fresh", "stale", "ghost"], threshold_seconds=300)
    by_name = {r.pipeline: r for r in results}

    assert by_name["fresh"].is_stale is False
    assert by_name["stale"].is_stale is True
    assert by_name["ghost"].is_stale is False  # never seen


def test_staleness_result_str_never_seen():
    r = StalenessResult(pipeline="p", last_seen=None, threshold_seconds=60, is_stale=False)
    assert "never seen" in str(r)
    assert "p" in str(r)


def test_staleness_result_str_stale():
    r = StalenessResult(
        pipeline="p",
        last_seen=time.time() - 400,
        threshold_seconds=300,
        is_stale=True,
    )
    assert "STALE" in str(r)
