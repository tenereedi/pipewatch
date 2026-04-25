"""Tests for pipewatch.checkpoint."""

import time
from pathlib import Path

import pytest

from pipewatch.checkpoint import (
    Checkpoint,
    clear_checkpoints,
    init_checkpoint_db,
    latest_checkpoint,
    load_checkpoints,
    record_checkpoint,
)


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "checkpoints.db"
    init_checkpoint_db(db)
    return db


def test_init_db_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "cp.db"
    assert not db.exists()
    init_checkpoint_db(db)
    assert db.exists()


def test_record_checkpoint_returns_object(tmp_db: Path) -> None:
    cp = record_checkpoint("pipe_a", "start", db_path=tmp_db)
    assert isinstance(cp, Checkpoint)
    assert cp.pipeline == "pipe_a"
    assert cp.label == "start"
    assert cp.timestamp > 0


def test_record_checkpoint_custom_timestamp(tmp_db: Path) -> None:
    ts = 1_700_000_000.0
    cp = record_checkpoint("pipe_b", "end", db_path=tmp_db, timestamp=ts)
    assert cp.timestamp == ts


def test_load_checkpoints_returns_newest_first(tmp_db: Path) -> None:
    t0 = time.time()
    record_checkpoint("pipe_c", "alpha", db_path=tmp_db, timestamp=t0)
    record_checkpoint("pipe_c", "beta", db_path=tmp_db, timestamp=t0 + 10)
    record_checkpoint("pipe_c", "gamma", db_path=tmp_db, timestamp=t0 + 20)

    results = load_checkpoints("pipe_c", db_path=tmp_db)
    assert len(results) == 3
    assert results[0].label == "gamma"
    assert results[1].label == "beta"
    assert results[2].label == "alpha"


def test_load_checkpoints_empty_when_none(tmp_db: Path) -> None:
    results = load_checkpoints("nonexistent", db_path=tmp_db)
    assert results == []


def test_load_checkpoints_limit(tmp_db: Path) -> None:
    t0 = time.time()
    for i in range(10):
        record_checkpoint("pipe_d", f"step_{i}", db_path=tmp_db, timestamp=t0 + i)
    results = load_checkpoints("pipe_d", db_path=tmp_db, limit=3)
    assert len(results) == 3


def test_latest_checkpoint_returns_most_recent(tmp_db: Path) -> None:
    t0 = time.time()
    record_checkpoint("pipe_e", "first", db_path=tmp_db, timestamp=t0)
    record_checkpoint("pipe_e", "second", db_path=tmp_db, timestamp=t0 + 5)
    latest = latest_checkpoint("pipe_e", db_path=tmp_db)
    assert latest is not None
    assert latest.label == "second"


def test_latest_checkpoint_none_when_missing(tmp_db: Path) -> None:
    assert latest_checkpoint("ghost", db_path=tmp_db) is None


def test_clear_checkpoints_removes_records(tmp_db: Path) -> None:
    t0 = time.time()
    record_checkpoint("pipe_f", "a", db_path=tmp_db, timestamp=t0)
    record_checkpoint("pipe_f", "b", db_path=tmp_db, timestamp=t0 + 1)
    removed = clear_checkpoints("pipe_f", db_path=tmp_db)
    assert removed == 2
    assert load_checkpoints("pipe_f", db_path=tmp_db) == []


def test_checkpoint_str_format(tmp_db: Path) -> None:
    ts = 1_700_000_000.0
    cp = record_checkpoint("pipe_g", "deploy", db_path=tmp_db, timestamp=ts)
    text = str(cp)
    assert "pipe_g" in text
    assert "deploy" in text
