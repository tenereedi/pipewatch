"""Tests for pipewatch.watchdog."""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.watchdog import (
    WatchdogResult,
    check_watchdog,
    check_all_watchdogs,
    any_silent,
)


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "history.db")
    init_db(db)
    return db


def _r(name: str, healthy: bool = True, ts: datetime | None = None) -> CheckResult:
    return CheckResult(
        pipeline_name=name,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=ts or datetime.now(tz=timezone.utc),
    )


# ---------------------------------------------------------------------------


def test_invalid_threshold_raises(tmp_db):
    with pytest.raises(ValueError):
        check_watchdog(tmp_db, "pipe", silence_threshold_seconds=0)


def test_never_seen_is_silent(tmp_db):
    result = check_watchdog(tmp_db, "ghost", silence_threshold_seconds=60)
    assert result.is_silent is True
    assert result.last_seen is None
    assert "never recorded" in str(result)


def test_recent_result_is_not_silent(tmp_db):
    save_results(tmp_db, [_r("pipe_a")])
    result = check_watchdog(tmp_db, "pipe_a", silence_threshold_seconds=300)
    assert result.is_silent is False
    assert result.last_seen is not None
    assert "OK" in str(result)


def test_old_result_is_silent(tmp_db):
    old_ts = datetime.now(tz=timezone.utc) - timedelta(seconds=600)
    save_results(tmp_db, [_r("", ts=old_ts)])
    result = check_watchdog(tmp_db, "pipe_b", silence_threshold_seconds=300)
    assert result.is_silent is True
    assert "SILENT" in str(result)


def test_check_all_watchdogs_returns_one_per_pipeline(tmp_db):
    save_results(tmp_db, [_r("alpha")])
    results = check_all_watchdogs(tmp_db, ["alpha", "beta"], silence_threshold_seconds=300)
    assert len(results) == 2
    names = {r.pipeline_name for r in results}
    assert names == {"alpha", "beta"}


def test_any_silent_true_when_one_silent(tmp_db):
    save_results(tmp_db, [_r("live")])
    results = check_all_watchdogs(tmp_db, ["live", "missing"], silence_threshold_seconds=300)
    assert any_silent(results) is True


def test_any_silent_false_when_all_active(tmp_db):
    save_results(tmp_db, [_r("p1"), _r("p2")])
    results = check_all_watchdogs(tmp_db, ["p1", "p2"], silence_threshold_seconds=300)
    assert any_silent(results) is False
