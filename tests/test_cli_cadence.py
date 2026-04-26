"""Tests for pipewatch.cli_cadence."""

from __future__ import annotations

import argparse
import time
import pytest

from pipewatch.cli_cadence import handle_cadence
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
        message="ok",
        timestamp=ts,
    )


def _args(db: str, pipelines=None, grace: int = 60) -> argparse.Namespace:
    return argparse.Namespace(db=db, pipelines=pipelines or [], grace=grace)


def test_no_pipelines_returns_false(tmp_db):
    result = handle_cadence(_args(tmp_db, pipelines=[]))
    assert result is False


def test_invalid_spec_returns_false(tmp_db):
    result = handle_cadence(_args(tmp_db, pipelines=["bad-spec"]))
    assert result is False


def test_all_on_schedule_returns_true(tmp_db, capsys):
    now = time.time()
    save_results([_r("pipe_x", True, now - 30)], db_path=tmp_db)
    result = handle_cadence(_args(tmp_db, pipelines=["pipe_x:300"]))
    assert result is True
    out = capsys.readouterr().out
    assert "pipe_x" in out
    assert "OK" in out


def test_overdue_pipeline_returns_false(tmp_db, capsys):
    now = time.time()
    save_results([_r("pipe_y", True, now - 1000)], db_path=tmp_db)
    result = handle_cadence(_args(tmp_db, pipelines=["pipe_y:300"], grace=30))
    assert result is False
    out = capsys.readouterr().out
    assert "OVERDUE" in out


def test_mixed_pipelines_returns_false(tmp_db, capsys):
    now = time.time()
    save_results([_r("ok_pipe", True, now - 10)], db_path=tmp_db)
    # never_pipe has no records
    result = handle_cadence(
        _args(tmp_db, pipelines=["ok_pipe:300", "never_pipe:300"])
    )
    assert result is False
