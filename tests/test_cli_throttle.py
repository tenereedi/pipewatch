"""Tests for pipewatch.cli_throttle."""

import argparse
import time
from pathlib import Path

import pytest

from pipewatch.throttle import init_throttle_db, record_alert, ThrottlePolicy
from pipewatch.cli_throttle import handle_throttle


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "throttle.db"
    init_throttle_db(db)
    return db


def _args(**kwargs) -> argparse.Namespace:
    defaults = {"throttle_cmd": None, "pipeline": "p", "alert_type": "default", "cooldown": 3600}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_no_subcommand_returns_false(tmp_db: Path, capsys):
    args = _args(db=str(tmp_db))
    result = handle_throttle(args)
    assert result is False
    out = capsys.readouterr().out
    assert "Usage" in out


def test_check_not_throttled_returns_true(tmp_db: Path, capsys):
    args = _args(throttle_cmd="check", pipeline="my-pipe", db=str(tmp_db))
    result = handle_throttle(args)
    assert result is True
    out = capsys.readouterr().out
    assert "not throttled" in out


def test_check_throttled_returns_false(tmp_db: Path, capsys):
    policy = ThrottlePolicy(pipeline="my-pipe", cooldown_seconds=3600)
    record_alert(policy, db_path=tmp_db)
    args = _args(throttle_cmd="check", pipeline="my-pipe", db=str(tmp_db))
    result = handle_throttle(args)
    assert result is False
    out = capsys.readouterr().out
    assert "THROTTLED" in out


def test_record_subcommand_returns_true(tmp_db: Path, capsys):
    args = _args(throttle_cmd="record", pipeline="pipe-x", db=str(tmp_db))
    result = handle_throttle(args)
    assert result is True
    out = capsys.readouterr().out
    assert "Recorded" in out


def test_clear_subcommand_returns_true(tmp_db: Path, capsys):
    policy = ThrottlePolicy(pipeline="pipe-y", cooldown_seconds=3600)
    record_alert(policy, db_path=tmp_db)
    args = _args(throttle_cmd="clear", pipeline="pipe-y", db=str(tmp_db))
    result = handle_throttle(args)
    assert result is True
    out = capsys.readouterr().out
    assert "Cleared" in out
