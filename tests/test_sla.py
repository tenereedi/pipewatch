"""Tests for pipewatch.sla and pipewatch.cli_sla."""
from __future__ import annotations

import time
import sqlite3
import pytest
from unittest.mock import patch, MagicMock

from pipewatch.sla import SLAPolicy, SLAResult, check_sla, check_all_slas, any_sla_breached
from pipewatch.checks import CheckResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _r(pipeline: str, healthy: bool, age_seconds: float = 5.0) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=time.time() - age_seconds,
    )


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "h.db")
    from pipewatch.history import init_db
    init_db(db)
    return db


def _populate(db, results):
    from pipewatch.history import save_results
    save_results(db, results)


# ---------------------------------------------------------------------------
# SLAPolicy validation
# ---------------------------------------------------------------------------

def test_invalid_target_rate_zero_raises():
    with pytest.raises(ValueError):
        SLAPolicy(pipeline="p", target_rate=0.0)


def test_invalid_target_rate_above_one_raises():
    with pytest.raises(ValueError):
        SLAPolicy(pipeline="p", target_rate=1.1)


def test_invalid_window_raises():
    with pytest.raises(ValueError):
        SLAPolicy(pipeline="p", target_rate=0.9, window_minutes=0)


# ---------------------------------------------------------------------------
# check_sla
# ---------------------------------------------------------------------------

def test_check_sla_no_history_defaults_to_met(tmp_db):
    policy = SLAPolicy(pipeline="missing", target_rate=0.99)
    result = check_sla(policy, tmp_db)
    assert result.met is True
    assert result.total == 0
    assert result.actual_rate == 1.0


def test_check_sla_all_healthy_meets_target(tmp_db):
    _populate(tmp_db, [_r("p1", True) for _ in range(10)])
    policy = SLAPolicy(pipeline="p1", target_rate=0.95)
    result = check_sla(policy, tmp_db)
    assert result.met is True
    assert result.healthy == result.total


def test_check_sla_too_many_failures_breaches(tmp_db):
    results = [_r("p2", True)] * 8 + [_r("p2", False)] * 2
    _populate(tmp_db, results)
    policy = SLAPolicy(pipeline="p2", target_rate=0.99)
    result = check_sla(policy, tmp_db)
    assert result.met is False
    assert result.actual_rate == pytest.approx(0.8)


def test_check_sla_str_contains_status(tmp_db):
    _populate(tmp_db, [_r("p3", False)] * 5)
    policy = SLAPolicy(pipeline="p3", target_rate=0.9)
    result = check_sla(policy, tmp_db)
    assert "BREACH" in str(result)
    assert "p3" in str(result)


# ---------------------------------------------------------------------------
# check_all_slas / any_sla_breached
# ---------------------------------------------------------------------------

def test_check_all_slas_returns_one_per_policy(tmp_db):
    policies = [
        SLAPolicy(pipeline="a", target_rate=0.9),
        SLAPolicy(pipeline="b", target_rate=0.9),
    ]
    results = check_all_slas(policies, tmp_db)
    assert len(results) == 2


def test_any_sla_breached_false_when_all_met(tmp_db):
    _populate(tmp_db, [_r("ok", True)] * 10)
    policies = [SLAPolicy(pipeline="ok", target_rate=0.8)]
    results = check_all_slas(policies, tmp_db)
    assert any_sla_breached(results) is False


def test_any_sla_breached_true_when_one_fails(tmp_db):
    _populate(tmp_db, [_r("bad", False)] * 10)
    policies = [SLAPolicy(pipeline="bad", target_rate=0.5)]
    results = check_all_slas(policies, tmp_db)
    assert any_sla_breached(results) is True
