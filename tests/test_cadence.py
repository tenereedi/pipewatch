"""Tests for pipewatch.cadence."""

from __future__ import annotations

import time
import pytest

from pipewatch.cadence import (
    CadencePolicy,
    CadenceResult,
    check_cadence,
    check_all_cadences,
)
from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "history.db")
    init_db(db)
    return db


def _r(pipeline: str, healthy: bool, ts: float) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=ts,
    )


def test_invalid_interval_raises():
    with pytest.raises(ValueError, match="expected_interval_seconds"):
        CadencePolicy(pipeline="p", expected_interval_seconds=0)


def test_invalid_grace_raises():
    with pytest.raises(ValueError, match="grace_seconds"):
        CadencePolicy(pipeline="p", expected_interval_seconds=60, grace_seconds=-1)


def test_never_seen_is_overdue(tmp_db):
    policy = CadencePolicy(pipeline="missing", expected_interval_seconds=300)
    result = check_cadence(policy, db_path=tmp_db)
    assert result.is_overdue
    assert result.last_seen is None
    assert "never seen" in str(result)


def test_recent_run_is_not_overdue(tmp_db):
    now = time.time()
    save_results([_r("pipe_a", True, now - 10)], db_path=tmp_db)
    policy = CadencePolicy(pipeline="pipe_a", expected_interval_seconds=300)
    result = check_cadence(policy, db_path=tmp_db, now=now)
    assert not result.is_overdue
    assert result.overdue_by_seconds == 0.0
    assert "OK" in str(result)


def test_overdue_run_detected(tmp_db):
    now = time.time()
    # last run was 700s ago, interval=300, grace=60 → deadline=360 → overdue by ~340s
    save_results([_r("pipe_b", True, now - 700)], db_path=tmp_db)
    policy = CadencePolicy(pipeline="pipe_b", expected_interval_seconds=300, grace_seconds=60)
    result = check_cadence(policy, db_path=tmp_db, now=now)
    assert result.is_overdue
    assert result.overdue_by_seconds > 300
    assert "OVERDUE" in str(result)


def test_within_grace_period_not_overdue(tmp_db):
    now = time.time()
    # last run 350s ago, interval=300, grace=60 → deadline=360 → still within grace
    save_results([_r("pipe_c", False, now - 350)], db_path=tmp_db)
    policy = CadencePolicy(pipeline="pipe_c", expected_interval_seconds=300, grace_seconds=60)
    result = check_cadence(policy, db_path=tmp_db, now=now)
    assert not result.is_overdue


def test_check_all_cadences_returns_all(tmp_db):
    now = time.time()
    save_results([_r("p1", True, now - 10)], db_path=tmp_db)
    policies = [
        CadencePolicy(pipeline="p1", expected_interval_seconds=300),
        CadencePolicy(pipeline="p2", expected_interval_seconds=300),
    ]
    results = check_all_cadences(policies, db_path=tmp_db, now=now)
    assert len(results) == 2
    assert not results[0].is_overdue
    assert results[1].is_overdue
