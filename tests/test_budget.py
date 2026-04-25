"""Tests for pipewatch.budget."""
from __future__ import annotations

import time
import pytest

from pipewatch.budget import (
    BudgetPolicy,
    BudgetResult,
    check_all_budgets,
    evaluate_budget,
    init_budget_db,
    record_check,
)


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "budget.db")
    init_budget_db(db)
    return db


def test_invalid_max_checks_raises():
    with pytest.raises(ValueError, match="max_checks"):
        BudgetPolicy(max_checks=0, window_seconds=60)


def test_invalid_window_raises():
    with pytest.raises(ValueError, match="window_seconds"):
        BudgetPolicy(max_checks=10, window_seconds=0)


def test_no_records_returns_zero(tmp_db):
    policy = BudgetPolicy(max_checks=5, window_seconds=60)
    result = evaluate_budget(tmp_db, "pipe_a", policy)
    assert result.fired_in_window == 0
    assert not result.budget_exceeded


def test_record_and_evaluate(tmp_db):
    policy = BudgetPolicy(max_checks=3, window_seconds=60)
    for _ in range(3):
        record_check(tmp_db, "pipe_a")
    result = evaluate_budget(tmp_db, "pipe_a", policy)
    assert result.fired_in_window == 3
    assert not result.budget_exceeded  # exactly at limit


def test_budget_exceeded(tmp_db):
    policy = BudgetPolicy(max_checks=2, window_seconds=60)
    for _ in range(3):
        record_check(tmp_db, "pipe_a")
    result = evaluate_budget(tmp_db, "pipe_a", policy)
    assert result.budget_exceeded


def test_records_outside_window_ignored(tmp_db):
    import sqlite3
    # Manually insert an old record
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        "INSERT INTO check_budget (pipeline, fired_at) VALUES (?, ?)",
        ("pipe_a", time.time() - 7200),
    )
    conn.commit()
    conn.close()

    policy = BudgetPolicy(max_checks=5, window_seconds=3600)
    result = evaluate_budget(tmp_db, "pipe_a", policy)
    assert result.fired_in_window == 0


def test_check_all_budgets_multiple_pipelines(tmp_db):
    policy = BudgetPolicy(max_checks=1, window_seconds=60)
    record_check(tmp_db, "pipe_a")
    record_check(tmp_db, "pipe_a")
    record_check(tmp_db, "pipe_b")
    results = check_all_budgets(tmp_db, ["pipe_a", "pipe_b"], policy)
    assert len(results) == 2
    exceeded = {r.pipeline: r.budget_exceeded for r in results}
    assert exceeded["pipe_a"] is True
    assert exceeded["pipe_b"] is False


def test_budget_result_str_ok(tmp_db):
    policy = BudgetPolicy(max_checks=10, window_seconds=60)
    result = evaluate_budget(tmp_db, "pipe_x", policy)
    assert "OK" in str(result)
    assert "pipe_x" in str(result)


def test_budget_result_str_exceeded(tmp_db):
    policy = BudgetPolicy(max_checks=1, window_seconds=60)
    record_check(tmp_db, "pipe_x")
    record_check(tmp_db, "pipe_x")
    result = evaluate_budget(tmp_db, "pipe_x", policy)
    assert "EXCEEDED" in str(result)
