"""Tests for pipewatch.history_reporter module."""

import pytest
from pathlib import Path
from pipewatch.checks import CheckResult
from pipewatch.history import save_results
from pipewatch.history_reporter import print_history, history_summary


@pytest.fixture
def tmp_db(tmp_path):
    return tmp_path / "reporter_test.db"


def _save(db, pipeline="pipe", healthy=True):
    save_results(
        [CheckResult(pipeline=pipeline, check_type="http", healthy=healthy, message="ok" if healthy else "fail")],
        db_path=db,
    )


def test_print_history_empty(tmp_db, capsys):
    print_history(db_path=tmp_db)
    captured = capsys.readouterr()
    assert "No history found" in captured.out


def test_print_history_shows_rows(tmp_db, capsys):
    _save(tmp_db, pipeline="alpha", healthy=True)
    _save(tmp_db, pipeline="beta", healthy=False)
    print_history(db_path=tmp_db)
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out


def test_print_history_filter(tmp_db, capsys):
    _save(tmp_db, pipeline="only", healthy=True)
    _save(tmp_db, pipeline="other", healthy=False)
    print_history(pipeline="only", db_path=tmp_db)
    out = capsys.readouterr().out
    assert "only" in out
    assert "other" not in out


def test_history_summary_counts(tmp_db):
    _save(tmp_db, healthy=True)
    _save(tmp_db, healthy=True)
    _save(tmp_db, healthy=False)
    summary = history_summary(db_path=tmp_db)
    assert summary["total"] == 3
    assert summary["passed"] == 2
    assert summary["failed"] == 1


def test_history_summary_empty(tmp_db):
    summary = history_summary(db_path=tmp_db)
    assert summary == {"total": 0, "passed": 0, "failed": 0}
