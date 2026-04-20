"""Tests for pipewatch.retention and pipewatch.cli_retention."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.checks import CheckResult
from pipewatch.history import init_db, save_results
from pipewatch.retention import RetentionPolicy, prune_all, prune_history
from pipewatch.cli_retention import handle_retention


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def _r(pipeline: str, check: str, healthy: bool, days_ago: int) -> CheckResult:
    ts = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return CheckResult(pipeline=pipeline, check=check, healthy=healthy, message="ok", timestamp=ts)


def test_invalid_max_age_raises():
    with pytest.raises(ValueError):
        RetentionPolicy(max_age_days=0)


def test_prune_removes_old_records(tmp_db):
    save_results(tmp_db, [_r("p1", "http", True, days_ago=10)])
    save_results(tmp_db, [_r("p1", "http", True, days_ago=1)])

    deleted = prune_history(tmp_db, RetentionPolicy(max_age_days=5))

    assert deleted == 1


def test_prune_keeps_recent_records(tmp_db):
    save_results(tmp_db, [_r("p1", "http", True, days_ago=2)])

    deleted = prune_history(tmp_db, RetentionPolicy(max_age_days=5))

    assert deleted == 0


def test_prune_scoped_to_pipeline(tmp_db):
    save_results(tmp_db, [_r("p1", "http", True, days_ago=10)])
    save_results(tmp_db, [_r("p2", "http", True, days_ago=10)])

    deleted = prune_history(tmp_db, RetentionPolicy(max_age_days=5, pipeline="p1"))

    assert deleted == 1


def test_prune_all_aggregates(tmp_db):
    save_results(tmp_db, [_r("p1", "http", True, days_ago=10)])
    save_results(tmp_db, [_r("p2", "http", True, days_ago=10)])

    summary = prune_all(tmp_db, [
        RetentionPolicy(max_age_days=5, pipeline="p1"),
        RetentionPolicy(max_age_days=5, pipeline="p2"),
    ])

    assert summary["p1"] == 1
    assert summary["p2"] == 1


def test_handle_retention_prints_summary(tmp_db, capsys):
    save_results(tmp_db, [_r("pipe", "http", True, days_ago=20)])

    args = argparse.Namespace(max_age_days=7, pipeline=None, db=tmp_db)
    handle_retention(args)

    out = capsys.readouterr().out
    assert "Pruned 1 record" in out
    assert "all pipelines" in out


def test_handle_retention_invalid_age_prints_error(capsys):
    args = argparse.Namespace(max_age_days=0, pipeline=None, db=":memory:")
    handle_retention(args)

    out = capsys.readouterr().out
    assert "Invalid policy" in out
