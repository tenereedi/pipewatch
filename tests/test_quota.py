"""Tests for pipewatch.quota."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipewatch.quota import (
    QuotaPolicy,
    QuotaResult,
    evaluate_all_quotas,
    evaluate_quota,
    init_quota_db,
    record_failure,
)


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "quota.db"
    init_quota_db(db)
    return db


def test_invalid_max_failures_raises() -> None:
    with pytest.raises(ValueError, match="max_failures"):
        QuotaPolicy(pipeline="p", max_failures=0, window_seconds=60)


def test_invalid_window_raises() -> None:
    with pytest.raises(ValueError, match="window_seconds"):
        QuotaPolicy(pipeline="p", max_failures=3, window_seconds=0)


def test_no_records_returns_zero(tmp_db: Path) -> None:
    policy = QuotaPolicy(pipeline="pipe-a", max_failures=5, window_seconds=3600)
    result = evaluate_quota(policy, db_path=tmp_db)
    assert result.failures_in_window == 0
    assert not result.exceeded


def test_record_and_evaluate(tmp_db: Path) -> None:
    now = time.time()
    policy = QuotaPolicy(pipeline="pipe-a", max_failures=2, window_seconds=3600)
    record_failure("pipe-a", db_path=tmp_db, now=now)
    record_failure("pipe-a", db_path=tmp_db, now=now + 1)
    result = evaluate_quota(policy, db_path=tmp_db, now=now + 2)
    assert result.failures_in_window == 2
    assert not result.exceeded  # exactly at limit, not over


def test_quota_exceeded(tmp_db: Path) -> None:
    now = time.time()
    policy = QuotaPolicy(pipeline="pipe-b", max_failures=2, window_seconds=3600)
    for i in range(3):
        record_failure("pipe-b", db_path=tmp_db, now=now + i)
    result = evaluate_quota(policy, db_path=tmp_db, now=now + 10)
    assert result.exceeded
    assert "EXCEEDED" in str(result)


def test_old_failures_outside_window_ignored(tmp_db: Path) -> None:
    now = time.time()
    policy = QuotaPolicy(pipeline="pipe-c", max_failures=2, window_seconds=60)
    # record failures well outside the window
    record_failure("pipe-c", db_path=tmp_db, now=now - 120)
    record_failure("pipe-c", db_path=tmp_db, now=now - 90)
    result = evaluate_quota(policy, db_path=tmp_db, now=now)
    assert result.failures_in_window == 0
    assert not result.exceeded


def test_evaluate_all_quotas(tmp_db: Path) -> None:
    now = time.time()
    record_failure("alpha", db_path=tmp_db, now=now)
    policies = [
        QuotaPolicy(pipeline="alpha", max_failures=1, window_seconds=3600),
        QuotaPolicy(pipeline="beta", max_failures=5, window_seconds=3600),
    ]
    results = evaluate_all_quotas(policies, db_path=tmp_db, now=now + 1)
    assert len(results) == 2
    alpha = next(r for r in results if r.pipeline == "alpha")
    beta = next(r for r in results if r.pipeline == "beta")
    assert not alpha.exceeded  # 1 failure == max_failures (not strictly over)
    assert not beta.exceeded


def test_quota_result_str_ok() -> None:
    r = QuotaResult(pipeline="p", failures_in_window=1, max_failures=5, exceeded=False)
    assert "ok" in str(r)
    assert "p" in str(r)
