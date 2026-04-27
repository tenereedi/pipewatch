"""Tests for pipewatch.heartbeat."""

from __future__ import annotations

import time

import pytest

from pipewatch.heartbeat import (
    HeartbeatResult,
    check_all_heartbeats,
    check_heartbeat,
    init_heartbeat_db,
    record_heartbeat,
)


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "hb.db")
    init_heartbeat_db(db)
    return db


def test_init_db_creates_file(tmp_path):
    db = str(tmp_path / "hb.db")
    init_heartbeat_db(db)
    assert (tmp_path / "hb.db").exists()


def test_invalid_threshold_raises(tmp_db):
    with pytest.raises(ValueError):
        check_heartbeat(tmp_db, "pipe", 0)

    with pytest.raises(ValueError):
        check_heartbeat(tmp_db, "pipe", -10)


def test_never_seen_is_dead(tmp_db):
    result = check_heartbeat(tmp_db, "pipe_a", 60)
    assert result.pipeline == "pipe_a"
    assert result.last_seen is None
    assert result.is_alive is False


def test_recent_heartbeat_is_alive(tmp_db):
    record_heartbeat(tmp_db, "pipe_b", ts=time.time() - 10)
    result = check_heartbeat(tmp_db, "pipe_b", threshold_seconds=60)
    assert result.is_alive is True


def test_stale_heartbeat_is_dead(tmp_db):
    record_heartbeat(tmp_db, "pipe_c", ts=time.time() - 200)
    result = check_heartbeat(tmp_db, "pipe_c", threshold_seconds=60)
    assert result.is_alive is False


def test_multiple_records_uses_most_recent(tmp_db):
    now = time.time()
    record_heartbeat(tmp_db, "pipe_d", ts=now - 500)
    record_heartbeat(tmp_db, "pipe_d", ts=now - 5)
    result = check_heartbeat(tmp_db, "pipe_d", threshold_seconds=60)
    assert result.is_alive is True


def test_check_all_heartbeats_returns_list(tmp_db):
    now = time.time()
    record_heartbeat(tmp_db, "p1", ts=now - 10)
    record_heartbeat(tmp_db, "p2", ts=now - 9999)
    specs = [
        {"pipeline": "p1", "threshold_seconds": 60},
        {"pipeline": "p2", "threshold_seconds": 60},
    ]
    results = check_all_heartbeats(tmp_db, specs)
    assert len(results) == 2
    assert results[0].is_alive is True
    assert results[1].is_alive is False


def test_heartbeat_result_str_alive(tmp_db):
    record_heartbeat(tmp_db, "pipe_e", ts=time.time() - 5)
    r = check_heartbeat(tmp_db, "pipe_e", 60)
    s = str(r)
    assert "ALIVE" in s
    assert "pipe_e" in s


def test_heartbeat_result_str_never_seen(tmp_db):
    r = check_heartbeat(tmp_db, "ghost", 60)
    s = str(r)
    assert "DEAD" in s
    assert "never seen" in s
