"""Tests for pipewatch.silencer."""

from __future__ import annotations

import time

import pytest

from pipewatch.silencer import (
    Silence,
    add_silence,
    clear_silences,
    init_silencer_db,
    is_silenced,
    list_silences,
)


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "silences.db")
    init_silencer_db(db)
    return db


def test_init_db_creates_table(tmp_db):
    # Re-initialising should be idempotent
    init_silencer_db(tmp_db)
    silences = list_silences(tmp_db)
    assert silences == []


def test_add_silence_returns_silence_object(tmp_db):
    s = add_silence(tmp_db, "pipe_a", duration_seconds=60, reason="maintenance")
    assert isinstance(s, Silence)
    assert s.pipeline == "pipe_a"
    assert s.reason == "maintenance"
    assert s.is_active()


def test_is_silenced_true_for_active(tmp_db):
    add_silence(tmp_db, "pipe_b", duration_seconds=120)
    assert is_silenced(tmp_db, "pipe_b") is True


def test_is_silenced_false_when_expired(tmp_db):
    add_silence(tmp_db, "pipe_c", duration_seconds=-1)  # already expired
    assert is_silenced(tmp_db, "pipe_c") is False


def test_is_silenced_false_for_unknown_pipeline(tmp_db):
    assert is_silenced(tmp_db, "nonexistent") is False


def test_list_silences_only_active_by_default(tmp_db):
    add_silence(tmp_db, "pipe_active", duration_seconds=300)
    add_silence(tmp_db, "pipe_expired", duration_seconds=-1)
    active = list_silences(tmp_db)
    names = [s.pipeline for s in active]
    assert "pipe_active" in names
    assert "pipe_expired" not in names


def test_list_silences_include_expired(tmp_db):
    add_silence(tmp_db, "pipe_active", duration_seconds=300)
    add_silence(tmp_db, "pipe_expired", duration_seconds=-1)
    all_s = list_silences(tmp_db, include_expired=True)
    names = [s.pipeline for s in all_s]
    assert "pipe_active" in names
    assert "pipe_expired" in names


def test_clear_silences_all(tmp_db):
    add_silence(tmp_db, "pipe_x", 60)
    add_silence(tmp_db, "pipe_y", 60)
    removed = clear_silences(tmp_db)
    assert removed == 2
    assert list_silences(tmp_db) == []


def test_clear_silences_specific_pipeline(tmp_db):
    add_silence(tmp_db, "pipe_x", 60)
    add_silence(tmp_db, "pipe_y", 60)
    removed = clear_silences(tmp_db, pipeline="pipe_x")
    assert removed == 1
    remaining = [s.pipeline for s in list_silences(tmp_db)]
    assert "pipe_y" in remaining
    assert "pipe_x" not in remaining


def test_silence_str_active(tmp_db):
    s = add_silence(tmp_db, "pipe_z", 3600, reason="deploy")
    text = str(s)
    assert "active" in text
    assert "pipe_z" in text
    assert "deploy" in text


def test_silence_str_expired():
    s = Silence(pipeline="old_pipe", until=time.time() - 10)
    assert "expired" in str(s)
