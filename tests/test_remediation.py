"""Tests for pipewatch.remediation."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipewatch.remediation import (
    init_remediation_db,
    set_hint,
    get_hint,
    list_hints,
    RemediationHint,
)


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "rem.db"
    init_remediation_db(db)
    return db


def test_init_db_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "new_rem.db"
    assert not db.exists()
    init_remediation_db(db)
    assert db.exists()


def test_set_hint_returns_object(tmp_db: Path) -> None:
    h = set_hint("pipe_a", "http", "Check the endpoint URL.", db_path=tmp_db)
    assert isinstance(h, RemediationHint)
    assert h.pipeline == "pipe_a"
    assert h.check_type == "http"
    assert h.hint == "Check the endpoint URL."
    assert h.added_at <= time.time()


def test_get_hint_returns_stored(tmp_db: Path) -> None:
    set_hint("pipe_b", "freshness", "Verify cron schedule.", db_path=tmp_db)
    h = get_hint("pipe_b", "freshness", db_path=tmp_db)
    assert h is not None
    assert h.hint == "Verify cron schedule."


def test_get_hint_missing_returns_none(tmp_db: Path) -> None:
    result = get_hint("nonexistent", "http", db_path=tmp_db)
    assert result is None


def test_set_hint_replaces_existing(tmp_db: Path) -> None:
    set_hint("pipe_c", "http", "Old hint.", db_path=tmp_db)
    set_hint("pipe_c", "http", "New hint.", db_path=tmp_db)
    h = get_hint("pipe_c", "http", db_path=tmp_db)
    assert h is not None
    assert h.hint == "New hint."
    all_hints = list_hints(pipeline="pipe_c", db_path=tmp_db)
    assert len(all_hints) == 1


def test_list_hints_empty(tmp_db: Path) -> None:
    assert list_hints(db_path=tmp_db) == []


def test_list_hints_returns_all(tmp_db: Path) -> None:
    set_hint("alpha", "http", "hint A", db_path=tmp_db)
    set_hint("beta", "freshness", "hint B", db_path=tmp_db)
    hints = list_hints(db_path=tmp_db)
    assert len(hints) == 2
    names = {h.pipeline for h in hints}
    assert names == {"alpha", "beta"}


def test_list_hints_filter_by_pipeline(tmp_db: Path) -> None:
    set_hint("alpha", "http", "hint A", db_path=tmp_db)
    set_hint("alpha", "freshness", "hint B", db_path=tmp_db)
    set_hint("beta", "http", "hint C", db_path=tmp_db)
    hints = list_hints(pipeline="alpha", db_path=tmp_db)
    assert len(hints) == 2
    assert all(h.pipeline == "alpha" for h in hints)


def test_hint_str(tmp_db: Path) -> None:
    h = set_hint("mypipe", "http", "Restart the service.", db_path=tmp_db)
    assert "mypipe" in str(h)
    assert "http" in str(h)
    assert "Restart the service." in str(h)
