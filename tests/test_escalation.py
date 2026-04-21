"""Tests for pipewatch.escalation."""

import pytest
from pathlib import Path

from pipewatch.escalation import (
    EscalationPolicy,
    EscalationResult,
    check_all_escalations,
    init_escalation_db,
    update_and_check,
)
from pipewatch.checks import CheckResult


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "escalation.db"
    init_escalation_db(db)
    return db


def _r(healthy: bool, name: str = "check") -> CheckResult:
    return CheckResult(
        pipeline="pipe",
        check_name=name,
        passed=healthy,
        message="ok" if healthy else "fail",
    )


def test_invalid_threshold_raises():
    with pytest.raises(ValueError):
        EscalationPolicy(threshold=0)


def test_no_failures_resets_streak(tmp_db):
    policy = EscalationPolicy(threshold=2)
    result = update_and_check("pipe1", [_r(True)], policy, tmp_db)
    assert result.consecutive_failures == 0
    assert result.escalated is False


def test_single_failure_does_not_escalate(tmp_db):
    policy = EscalationPolicy(threshold=3)
    result = update_and_check("pipe1", [_r(False)], policy, tmp_db)
    assert result.consecutive_failures == 1
    assert result.escalated is False


def test_consecutive_failures_escalate(tmp_db):
    policy = EscalationPolicy(threshold=3)
    for _ in range(3):
        result = update_and_check("pipe1", [_r(False)], policy, tmp_db)
    assert result.consecutive_failures == 3
    assert result.escalated is True


def test_recovery_resets_streak(tmp_db):
    policy = EscalationPolicy(threshold=2)
    update_and_check("pipe1", [_r(False)], policy, tmp_db)
    update_and_check("pipe1", [_r(False)], policy, tmp_db)
    # recovery
    result = update_and_check("pipe1", [_r(True)], policy, tmp_db)
    assert result.consecutive_failures == 0
    assert result.escalated is False


def test_escalation_result_str_escalated(tmp_db):
    policy = EscalationPolicy(threshold=1)
    result = update_and_check("pipe1", [_r(False)], policy, tmp_db)
    assert "ESCALATED" in str(result)
    assert "pipe1" in str(result)


def test_escalation_result_str_ok(tmp_db):
    policy = EscalationPolicy(threshold=3)
    result = update_and_check("pipe1", [_r(True)], policy, tmp_db)
    assert "ok" in str(result)


def test_check_all_escalations(tmp_db):
    policy = EscalationPolicy(threshold=2)
    pipelines = {
        "pipeA": [_r(False)],
        "pipeB": [_r(True)],
    }
    results = check_all_escalations(pipelines, policy, tmp_db)
    assert len(results) == 2
    by_name = {r.pipeline: r for r in results}
    assert by_name["pipeA"].consecutive_failures == 1
    assert by_name["pipeB"].consecutive_failures == 0
