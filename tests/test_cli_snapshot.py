"""Tests for pipewatch.cli_snapshot."""
from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_snapshot import add_snapshot_subcommand, handle_snapshot
from pipewatch.checks import CheckResult
from pipewatch.snapshot import Snapshot, save_snapshot


def _make_args(action: str, snap_file: str, config: str = "pipewatch/example_config.yaml") -> argparse.Namespace:
    return argparse.Namespace(action=action, snapshot_file=snap_file, config=config)


def _r(pipeline: str, check: str, healthy: bool) -> CheckResult:
    return CheckResult(pipeline=pipeline, check=check, healthy=healthy, detail="")


@patch("pipewatch.cli_snapshot.load")
@patch("pipewatch.cli_snapshot.run_all_checks")
def test_capture_saves_snapshot(mock_run, mock_load, tmp_path, capsys):
    snap_file = str(tmp_path / "snap.json")
    mock_load.return_value = MagicMock()
    mock_run.return_value = [_r("p", "http", True)]
    handle_snapshot(_make_args("capture", snap_file))
    out = capsys.readouterr().out
    assert "Saved" in out
    assert Path(snap_file).exists()


@patch("pipewatch.cli_snapshot.load")
@patch("pipewatch.cli_snapshot.run_all_checks")
def test_diff_no_previous_snapshot(mock_run, mock_load, tmp_path, capsys):
    snap_file = str(tmp_path / "missing.json")
    mock_load.return_value = MagicMock()
    mock_run.return_value = [_r("p", "http", True)]
    handle_snapshot(_make_args("diff", snap_file))
    out = capsys.readouterr().out
    assert "No previous snapshot" in out


@patch("pipewatch.cli_snapshot.load")
@patch("pipewatch.cli_snapshot.run_all_checks")
def test_diff_with_changes(mock_run, mock_load, tmp_path, capsys):
    snap_file = tmp_path / "snap.json"
    save_snapshot([_r("p", "http", True)], snap_file)
    mock_load.return_value = MagicMock()
    mock_run.return_value = [_r("p", "http", False)]
    handle_snapshot(_make_args("diff", str(snap_file)))
    out = capsys.readouterr().out
    assert "Changes detected" in out


@patch("pipewatch.cli_snapshot.load")
@patch("pipewatch.cli_snapshot.run_all_checks")
def test_diff_no_changes(mock_run, mock_load, tmp_path, capsys):
    snap_file = tmp_path / "snap.json"
    save_snapshot([_r("p", "http", True)], snap_file)
    mock_load.return_value = MagicMock()
    mock_run.return_value = [_r("p", "http", True)]
    handle_snapshot(_make_args("diff", str(snap_file)))
    out = capsys.readouterr().out
    assert "No changes" in out


def test_add_snapshot_subcommand_registers():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_snapshot_subcommand(subs)
    args = parser.parse_args(["snapshot", "capture"])
    assert args.action == "capture"
