"""Tests for pipewatch.snapshot."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipewatch.checks import CheckResult
from pipewatch.snapshot import (
    Snapshot,
    SnapshotDiff,
    diff_snapshots,
    load_snapshot,
    save_snapshot,
)


def _r(pipeline: str, check: str, healthy: bool) -> CheckResult:
    return CheckResult(pipeline=pipeline, check=check, healthy=healthy, detail="")


@pytest.fixture
def snap_file(tmp_path: Path) -> Path:
    return tmp_path / "snap.json"


def test_save_snapshot_creates_file(snap_file):
    results = [_r("pipe1", "http", True)]
    snap = save_snapshot(results, snap_file)
    assert snap_file.exists()
    assert isinstance(snap.timestamp, float)
    assert len(snap.results) == 1


def test_load_snapshot_returns_none_when_missing(tmp_path):
    result = load_snapshot(tmp_path / "nonexistent.json")
    assert result is None


def test_save_and_load_roundtrip(snap_file):
    results = [_r("pipe1", "http", True), _r("pipe2", "freshness", False)]
    save_snapshot(results, snap_file)
    loaded = load_snapshot(snap_file)
    assert loaded is not None
    assert len(loaded.results) == 2
    assert loaded.results[0]["pipeline"] == "pipe1"
    assert loaded.results[1]["healthy"] is False


def test_diff_no_changes():
    prev = Snapshot(
        timestamp=time.time(),
        results=[{"pipeline": "p", "check": "http", "healthy": True, "detail": ""}],
    )
    current = [_r("p", "http", True)]
    diff = diff_snapshots(prev, current)
    assert not diff.has_changes
    assert diff.unchanged_healthy == 1


def test_diff_detects_new_failure():
    prev = Snapshot(
        timestamp=time.time(),
        results=[{"pipeline": "p", "check": "http", "healthy": True, "detail": ""}],
    )
    current = [_r("p", "http", False)]
    diff = diff_snapshots(prev, current)
    assert diff.has_changes
    assert "p::http" in diff.new_failures


def test_diff_detects_recovery():
    prev = Snapshot(
        timestamp=time.time(),
        results=[{"pipeline": "p", "check": "http", "healthy": False, "detail": ""}],
    )
    current = [_r("p", "http", True)]
    diff = diff_snapshots(prev, current)
    assert diff.has_changes
    assert "p::http" in diff.recovered


def test_snapshot_str():
    snap = Snapshot(timestamp=1_700_000_000.0, results=[])
    assert "Snapshot" in str(snap)


def test_snapshot_diff_str_includes_failures():
    diff = SnapshotDiff(new_failures=["p::http"], recovered=["q::freshness"])
    text = str(diff)
    assert "p::http" in text
    assert "q::freshness" in text
