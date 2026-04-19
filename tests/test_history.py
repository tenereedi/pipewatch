"""Tests for pipewatch.history module."""

import pytest
from pathlib import Path
from pipewatch.checks import CheckResult
from pipewatch.history import init_db, save_results, load_recent, clear_history


@pytest.fixture
def tmp_db(tmp_path):
    return tmp_path / "test_history.db"


def _make_result(pipeline="pipe_a", healthy=True, check_type="http", message="ok"):
    return CheckResult(pipeline=pipeline, check_type=check_type, healthy=healthy, message=message)


def test_init_db_creates_file(tmp_db):
    init_db(tmp_db)
    assert tmp_db.exists()


def test_save_and_load_results(tmp_db):
    results = [_make_result(), _make_result(pipeline="pipe_b", healthy=False, message="fail")]
    save_results(results, db_path=tmp_db)
    rows = load_recent(db_path=tmp_db)
    assert len(rows) == 2


def test_load_recent_filter_by_pipeline(tmp_db):
    save_results([_make_result(pipeline="alpha"), _make_result(pipeline="beta")], db_path=tmp_db)
    rows = load_recent(pipeline="alpha", db_path=tmp_db)
    assert all(r["pipeline"] == "alpha" for r in rows)
    assert len(rows) == 1


def test_load_recent_limit(tmp_db):
    save_results([_make_result() for _ in range(10)], db_path=tmp_db)
    rows = load_recent(limit=4, db_path=tmp_db)
    assert len(rows) == 4


def test_clear_history(tmp_db):
    save_results([_make_result(), _make_result()], db_path=tmp_db)
    removed = clear_history(db_path=tmp_db)
    assert removed == 2
    assert load_recent(db_path=tmp_db) == []


def test_save_results_with_details(tmp_db):
    r = CheckResult(pipeline="p", check_type="freshness", healthy=False, message="stale", details={"age_seconds": 900})
    save_results([r], db_path=tmp_db)
    rows = load_recent(db_path=tmp_db)
    import json
    assert json.loads(rows[0]["details"])["age_seconds"] == 900


def test_empty_db_returns_empty_list(tmp_db):
    rows = load_recent(db_path=tmp_db)
    assert rows == []
