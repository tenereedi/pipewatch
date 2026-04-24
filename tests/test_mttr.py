"""Tests for pipewatch.mttr."""
import time
import pytest

from pipewatch.checks import CheckResult
from pipewatch.mttr import (
    MTTRSummary,
    compute_mttr,
    init_mttr_db,
    record_result,
)


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "mttr.db")
    init_mttr_db(db)
    return db


def _r(pipeline: str, healthy: bool) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        passed=healthy,
        message="ok" if healthy else "fail",
    )


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

def test_init_db_creates_file(tmp_path):
    db = str(tmp_path / "new_mttr.db")
    init_mttr_db(db)
    import os
    assert os.path.exists(db)


# ---------------------------------------------------------------------------
# record_result
# ---------------------------------------------------------------------------

def test_failure_opens_incident(tmp_db):
    record_result(tmp_db, _r("pipe-a", False))
    summary = compute_mttr(tmp_db, "pipe-a")
    # Incident is open (not yet recovered) — no completed incidents
    assert summary.incident_count == 0
    assert summary.mean_seconds is None


def test_recovery_closes_incident(tmp_db):
    record_result(tmp_db, _r("pipe-a", False))
    time.sleep(0.05)
    record_result(tmp_db, _r("pipe-a", True))
    summary = compute_mttr(tmp_db, "pipe-a")
    assert summary.incident_count == 1
    assert summary.mean_seconds is not None
    assert summary.mean_seconds >= 0.0


def test_healthy_result_without_open_incident_is_noop(tmp_db):
    record_result(tmp_db, _r("pipe-b", True))
    summary = compute_mttr(tmp_db, "pipe-b")
    assert summary.incident_count == 0


def test_second_failure_does_not_open_duplicate(tmp_db):
    record_result(tmp_db, _r("pipe-c", False))
    record_result(tmp_db, _r("pipe-c", False))  # still failing
    time.sleep(0.05)
    record_result(tmp_db, _r("pipe-c", True))
    summary = compute_mttr(tmp_db, "pipe-c")
    # Only one incident should have been opened and closed
    assert summary.incident_count == 1


def test_multiple_incidents_averaged(tmp_db):
    for _ in range(3):
        record_result(tmp_db, _r("pipe-d", False))
        time.sleep(0.02)
        record_result(tmp_db, _r("pipe-d", True))
    summary = compute_mttr(tmp_db, "pipe-d")
    assert summary.incident_count == 3
    assert summary.mean_seconds is not None


# ---------------------------------------------------------------------------
# MTTRSummary.__str__
# ---------------------------------------------------------------------------

def test_str_no_incidents():
    s = MTTRSummary(pipeline="p", incident_count=0, mean_seconds=None)
    assert "no completed" in str(s)


def test_str_with_incidents():
    s = MTTRSummary(pipeline="p", incident_count=2, mean_seconds=120.0)
    text = str(s)
    assert "2 incident" in text
    assert "2.0 min" in text
