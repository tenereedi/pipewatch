"""Tests for pipewatch.ownership."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipewatch.ownership import (
    OwnershipRecord,
    get_owner,
    init_ownership_db,
    list_owners,
    remove_owner,
    set_owner,
)


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "ownership.db"
    init_ownership_db(db)
    return db


def test_init_db_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "new_ownership.db"
    assert not db.exists()
    init_ownership_db(db)
    assert db.exists()


def test_set_owner_returns_record(tmp_db: Path) -> None:
    rec = set_owner("pipe_a", "alice", contact="alice@example.com", db_path=tmp_db)
    assert isinstance(rec, OwnershipRecord)
    assert rec.pipeline == "pipe_a"
    assert rec.owner == "alice"
    assert rec.contact == "alice@example.com"
    assert rec.updated_at <= time.time()


def test_get_owner_returns_record(tmp_db: Path) -> None:
    set_owner("pipe_b", "team-data", db_path=tmp_db)
    rec = get_owner("pipe_b", db_path=tmp_db)
    assert rec is not None
    assert rec.owner == "team-data"


def test_get_owner_missing_returns_none(tmp_db: Path) -> None:
    assert get_owner("nonexistent", db_path=tmp_db) is None


def test_set_owner_upserts(tmp_db: Path) -> None:
    set_owner("pipe_c", "alice", db_path=tmp_db)
    set_owner("pipe_c", "bob", contact="bob@example.com", db_path=tmp_db)
    rec = get_owner("pipe_c", db_path=tmp_db)
    assert rec is not None
    assert rec.owner == "bob"
    assert rec.contact == "bob@example.com"


def test_list_owners_empty(tmp_db: Path) -> None:
    assert list_owners(db_path=tmp_db) == []


def test_list_owners_returns_all(tmp_db: Path) -> None:
    set_owner("pipe_x", "alice", db_path=tmp_db)
    set_owner("pipe_y", "bob", db_path=tmp_db)
    records = list_owners(db_path=tmp_db)
    assert len(records) == 2
    names = {r.pipeline for r in records}
    assert names == {"pipe_x", "pipe_y"}


def test_remove_owner_returns_true(tmp_db: Path) -> None:
    set_owner("pipe_d", "carol", db_path=tmp_db)
    result = remove_owner("pipe_d", db_path=tmp_db)
    assert result is True
    assert get_owner("pipe_d", db_path=tmp_db) is None


def test_remove_owner_missing_returns_false(tmp_db: Path) -> None:
    assert remove_owner("ghost", db_path=tmp_db) is False


def test_str_with_contact(tmp_db: Path) -> None:
    rec = set_owner("pipe_e", "dave", contact="dave@corp.io", db_path=tmp_db)
    assert "dave@corp.io" in str(rec)
    assert "pipe_e" in str(rec)


def test_str_without_contact(tmp_db: Path) -> None:
    rec = set_owner("pipe_f", "eve", db_path=tmp_db)
    assert "<" not in str(rec)
    assert "pipe_f" in str(rec)
