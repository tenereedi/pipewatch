"""Tests for pipewatch.cli_budget."""
from __future__ import annotations

import argparse
import pytest

from pipewatch.budget import init_budget_db, record_check
from pipewatch.cli_budget import handle_budget


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "budget.db")
    init_budget_db(db)
    return db


def _args(**kwargs) -> argparse.Namespace:
    defaults = {
        "budget_cmd": None,
        "db": ":memory:",
        "pipeline": None,
        "pipelines": [],
        "max_checks": 10,
        "window": 60,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_no_subcommand_returns_false(capsys):
    args = _args(budget_cmd=None)
    result = handle_budget(args)
    assert result is False
    captured = capsys.readouterr()
    assert "No budget subcommand" in captured.err


def test_record_subcommand_returns_true(tmp_db, capsys):
    args = _args(budget_cmd="record", pipeline="pipe_a", db=tmp_db)
    result = handle_budget(args)
    assert result is True
    captured = capsys.readouterr()
    assert "pipe_a" in captured.out


def test_check_no_violations_returns_true(tmp_db, capsys):
    args = _args(budget_cmd="check", pipelines=["pipe_a"], db=tmp_db, max_checks=5, window=60)
    result = handle_budget(args)
    assert result is True


def test_check_exceeded_returns_false(tmp_db, capsys):
    for _ in range(5):
        record_check(tmp_db, "pipe_a")
    args = _args(budget_cmd="check", pipelines=["pipe_a"], db=tmp_db, max_checks=2, window=60)
    result = handle_budget(args)
    assert result is False
    captured = capsys.readouterr()
    assert "EXCEEDED" in captured.out


def test_check_invalid_policy_returns_false(tmp_db, capsys):
    args = _args(budget_cmd="check", pipelines=["pipe_a"], db=tmp_db, max_checks=0, window=60)
    result = handle_budget(args)
    assert result is False
    captured = capsys.readouterr()
    assert "Invalid policy" in captured.err
