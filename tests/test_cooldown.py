"""Tests for pipewatch.cooldown."""

from __future__ import annotations

import time
import pytest
from pathlib import Path

from pipewatch.cooldown import CooldownPolicy, init_cooldown_db


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "cooldown.db"
    init_cooldown_db(db)
    return db


def test_invalid_window_raises():
    with pytest.raises(ValueError):
        CooldownPolicy(pipeline="p", window_seconds=0)


def test_invalid_window_negative_raises():
    with pytest.raises(ValueError):
        CooldownPolicy(pipeline="p", window_seconds=-60)


def test_not_cooling_down_when_no_record(tmp_db):
    policy = CooldownPolicy(pipeline="pipe_a", window_seconds=300)
    assert policy.is_cooling_down(tmp_db) is False


def test_cooling_down_immediately_after_record(tmp_db):
    policy = CooldownPolicy(pipeline="pipe_a", window_seconds=300)
    policy.record_alert(tmp_db)
    assert policy.is_cooling_down(tmp_db) is True


def test_not_cooling_down_after_window_expires(tmp_db, monkeypatch):
    policy = CooldownPolicy(pipeline="pipe_a", window_seconds=1)
    policy.record_alert(tmp_db)
    # Advance time beyond the window
    monkeypatch.setattr(time, "time", lambda: time.time() + 5)
    assert policy.is_cooling_down(tmp_db) is False


def test_reset_clears_cooldown(tmp_db):
    policy = CooldownPolicy(pipeline="pipe_b", window_seconds=300)
    policy.record_alert(tmp_db)
    assert policy.is_cooling_down(tmp_db) is True
    policy.reset(tmp_db)
    assert policy.is_cooling_down(tmp_db) is False


def test_record_updates_existing_entry(tmp_db, monkeypatch):
    policy = CooldownPolicy(pipeline="pipe_c", window_seconds=300)
    # First record at a fake-old time
    old_time = time.time() - 400
    monkeypatch.setattr(time, "time", lambda: old_time)
    policy.record_alert(tmp_db)
    # Now record again at current time
    monkeypatch.undo()
    policy.record_alert(tmp_db)
    assert policy.is_cooling_down(tmp_db) is True


def test_str_representation():
    policy = CooldownPolicy(pipeline="my_pipe", window_seconds=120)
    assert "my_pipe" in str(policy)
    assert "120" in str(policy)


def test_independent_pipelines(tmp_db):
    p1 = CooldownPolicy(pipeline="alpha", window_seconds=300)
    p2 = CooldownPolicy(pipeline="beta", window_seconds=300)
    p1.record_alert(tmp_db)
    assert p1.is_cooling_down(tmp_db) is True
    assert p2.is_cooling_down(tmp_db) is False
