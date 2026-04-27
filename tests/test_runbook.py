"""Tests for pipewatch.runbook."""

from __future__ import annotations

import pytest
from pathlib import Path

from pipewatch.runbook import (
    RunbookEntry,
    delete_runbook,
    get_runbook,
    init_runbook_db,
    list_runbooks,
    set_runbook,
)


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "runbook.db"
    init_runbook_db(db)
    return db


def test_init_db_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "rb.db"
    assert not db.exists()
    init_runbook_db(db)
    assert db.exists()


def test_set_runbook_returns_entry(tmp_db: Path) -> None:
    entry = set_runbook("orders", "Orders runbook", "https://wiki/orders", db_path=tmp_db)
    assert isinstance(entry, RunbookEntry)
    assert entry.pipeline == "orders"
    assert entry.url == "https://wiki/orders"


def test_get_runbook_returns_entry(tmp_db: Path) -> None:
    set_runbook("inventory", "Inventory", "https://wiki/inv", notes="check DB", db_path=tmp_db)
    entry = get_runbook("inventory", db_path=tmp_db)
    assert entry is not None
    assert entry.title == "Inventory"
    assert entry.notes == "check DB"


def test_get_runbook_missing_returns_none(tmp_db: Path) -> None:
    assert get_runbook("nonexistent", db_path=tmp_db) is None


def test_set_runbook_upserts(tmp_db: Path) -> None:
    set_runbook("pipe", "Old Title", "https://old", db_path=tmp_db)
    set_runbook("pipe", "New Title", "https://new", db_path=tmp_db)
    entry = get_runbook("pipe", db_path=tmp_db)
    assert entry is not None
    assert entry.title == "New Title"
    assert entry.url == "https://new"


def test_list_runbooks_empty(tmp_db: Path) -> None:
    assert list_runbooks(db_path=tmp_db) == []


def test_list_runbooks_returns_all(tmp_db: Path) -> None:
    set_runbook("alpha", "Alpha", "https://a", db_path=tmp_db)
    set_runbook("beta", "Beta", "https://b", db_path=tmp_db)
    entries = list_runbooks(db_path=tmp_db)
    names = [e.pipeline for e in entries]
    assert "alpha" in names
    assert "beta" in names


def test_delete_runbook_returns_true(tmp_db: Path) -> None:
    set_runbook("pipe", "T", "https://x", db_path=tmp_db)
    assert delete_runbook("pipe", db_path=tmp_db) is True
    assert get_runbook("pipe", db_path=tmp_db) is None


def test_delete_runbook_missing_returns_false(tmp_db: Path) -> None:
    assert delete_runbook("ghost", db_path=tmp_db) is False


def test_runbook_entry_str_with_notes() -> None:
    e = RunbookEntry(pipeline="p", title="T", url="https://u", notes="see docs")
    assert "see docs" in str(e)
    assert "https://u" in str(e)


def test_runbook_entry_str_without_notes() -> None:
    e = RunbookEntry(pipeline="p", title="T", url="https://u")
    s = str(e)
    assert "https://u" in s
    assert "(" not in s
