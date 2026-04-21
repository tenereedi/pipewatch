"""Tests for pipewatch.cli_replay."""

from __future__ import annotations

import argparse

import pytest

from pipewatch.checks import CheckResult
from pipewatch.cli_replay import add_replay_subcommand, handle_replay
from pipewatch.history import init_db, save_results


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "test_history.db")
    init_db(db)
    return db


def _r(name: str, healthy: bool, pipeline: str = "pipe1") -> CheckResult:
    return CheckResult(
        name=name,
        pipeline=pipeline,
        status="ok" if healthy else "fail",
        message="all good" if healthy else "broken",
    )


def _args(tmp_db, pipelines=None, limit=50):
    ns = argparse.Namespace(
        db=tmp_db,
        pipelines=pipelines or [],
        limit=limit,
        func=handle_replay,
    )
    return ns


def test_handle_replay_no_pipelines(tmp_db, capsys):
    handle_replay(_args(tmp_db, pipelines=[]))
    out = capsys.readouterr().out
    assert "No pipelines specified" in out


def test_handle_replay_shows_summary(tmp_db, capsys):
    save_results(tmp_db, [_r("http", True), _r("freshness", False)])
    handle_replay(_args(tmp_db, pipelines=["pipe1"]))
    out = capsys.readouterr().out
    assert "pipe1" in out
    assert "OK" in out or "FAIL" in out


def test_handle_replay_empty_pipeline(tmp_db, capsys):
    handle_replay(_args(tmp_db, pipelines=["ghost"]))
    out = capsys.readouterr().out
    assert "No historical data" in out


def test_add_replay_subcommand_registers():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_replay_subcommand(sub)
    args = parser.parse_args(["replay", "mypipe", "--limit", "10"])
    assert args.pipelines == ["mypipe"]
    assert args.limit == 10
