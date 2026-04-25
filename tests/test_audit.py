"""Tests for pipewatch.audit and pipewatch.cli_audit."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipewatch.audit import (
    AuditEntry,
    clear_audit_log,
    init_audit_db,
    load_audit_log,
    record_action,
)
from pipewatch.cli_audit import handle_audit


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "audit.db"
    init_audit_db(db)
    return db


def test_init_db_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "new_audit.db"
    assert not db.exists()
    init_audit_db(db)
    assert db.exists()


def test_record_action_returns_entry(tmp_db: Path) -> None:
    entry = record_action("run", detail="pipeline=foo", db_path=tmp_db)
    assert isinstance(entry, AuditEntry)
    assert entry.command == "run"
    assert entry.detail == "pipeline=foo"
    assert entry.timestamp <= time.time()


def test_load_audit_log_returns_entries(tmp_db: Path) -> None:
    record_action("run", detail="a", db_path=tmp_db)
    record_action("check", detail="b", db_path=tmp_db)
    entries = load_audit_log(db_path=tmp_db)
    assert len(entries) == 2


def test_load_audit_log_newest_first(tmp_db: Path) -> None:
    record_action("first", db_path=tmp_db)
    time.sleep(0.01)
    record_action("second", db_path=tmp_db)
    entries = load_audit_log(db_path=tmp_db)
    assert entries[0].command == "second"


def test_load_audit_log_command_filter(tmp_db: Path) -> None:
    record_action("run", db_path=tmp_db)
    record_action("check", db_path=tmp_db)
    entries = load_audit_log(db_path=tmp_db, command_filter="run")
    assert all(e.command == "run" for e in entries)


def test_clear_audit_log_removes_all(tmp_db: Path) -> None:
    record_action("run", db_path=tmp_db)
    record_action("run", db_path=tmp_db)
    removed = clear_audit_log(db_path=tmp_db)
    assert removed == 2
    assert load_audit_log(db_path=tmp_db) == []


def test_audit_entry_str(tmp_db: Path) -> None:
    entry = record_action("deploy", detail="env=prod", db_path=tmp_db)
    s = str(entry)
    assert "deploy" in s
    assert "env=prod" in s


# --- CLI handler tests ---

class _Args:
    audit_cmd: str = "show"
    limit: int = 10
    command: str | None = None
    db: str = ""


def test_handle_audit_show_empty(tmp_db: Path, capsys: pytest.CaptureFixture) -> None:
    args = _Args()
    args.db = str(tmp_db)
    result = handle_audit(args)  # type: ignore[arg-type]
    assert result is True
    assert "No audit" in capsys.readouterr().out


def test_handle_audit_record_and_show(tmp_db: Path, capsys: pytest.CaptureFixture) -> None:
    args = _Args()
    args.audit_cmd = "record"
    args.db = str(tmp_db)
    args.command = "test-cmd"  # type: ignore[assignment]
    args.detail = "x=1"  # type: ignore[assignment]
    handle_audit(args)  # type: ignore[arg-type]
    args.audit_cmd = "show"
    handle_audit(args)  # type: ignore[arg-type]
    out = capsys.readouterr().out
    assert "test-cmd" in out


def test_handle_audit_no_subcommand(tmp_db: Path, capsys: pytest.CaptureFixture) -> None:
    args = _Args()
    args.audit_cmd = None  # type: ignore[assignment]
    args.db = str(tmp_db)
    result = handle_audit(args)  # type: ignore[arg-type]
    assert result is False
