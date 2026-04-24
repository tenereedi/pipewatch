"""Tests for pipewatch.heatmap."""
import datetime
import os
import sqlite3
import tempfile
import time

import pytest

from pipewatch.checks import CheckResult
from pipewatch.history import init_db, save_results
from pipewatch.heatmap import HeatmapRow, _heat_char, build_heatmap, print_heatmap


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test_history.db")
    init_db(db)
    return db


def _r(pipeline: str, healthy: bool, hour: int) -> CheckResult:
    dt = datetime.datetime(2024, 6, 15, hour, 0, 0)
    ts = dt.timestamp()
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        passed=healthy,
        message="ok" if healthy else "fail",
        timestamp=ts,
    )


def test_heat_char_empty():
    assert _heat_char(0) == "."


def test_heat_char_levels():
    assert _heat_char(1) == "\u2591"
    assert _heat_char(3) == "\u2592"
    assert _heat_char(7) == "\u2593"
    assert _heat_char(15) == "\u2588"


def test_build_heatmap_empty(tmp_db):
    rows = build_heatmap(tmp_db)
    assert rows == []


def test_build_heatmap_ignores_healthy(tmp_db):
    save_results(tmp_db, [_r("pipe-a", True, 10), _r("pipe-a", True, 11)])
    rows = build_heatmap(tmp_db)
    assert rows == []


def test_build_heatmap_counts_failures_by_hour(tmp_db):
    save_results(tmp_db, [
        _r("pipe-a", False, 3),
        _r("pipe-a", False, 3),
        _r("pipe-a", False, 14),
    ])
    rows = build_heatmap(tmp_db)
    assert len(rows) == 1
    row = rows[0]
    assert row.pipeline == "pipe-a"
    assert row.buckets[3] == 2
    assert row.buckets[14] == 1
    assert row.total_failures == 3
    assert row.peak_hour == 3


def test_build_heatmap_multiple_pipelines(tmp_db):
    save_results(tmp_db, [
        _r("pipe-a", False, 5),
        _r("pipe-b", False, 5),
        _r("pipe-b", False, 5),
    ])
    rows = build_heatmap(tmp_db)
    names = [r.pipeline for r in rows]
    assert "pipe-a" in names
    assert "pipe-b" in names
    # sorted by total failures descending
    assert rows[0].pipeline == "pipe-b"


def test_build_heatmap_pipeline_filter(tmp_db):
    save_results(tmp_db, [
        _r("pipe-a", False, 8),
        _r("pipe-b", False, 9),
    ])
    rows = build_heatmap(tmp_db, pipeline="pipe-a")
    assert len(rows) == 1
    assert rows[0].pipeline == "pipe-a"


def test_print_heatmap_empty(capsys):
    print_heatmap([])
    out = capsys.readouterr().out
    assert "No failure data" in out


def test_print_heatmap_shows_rows(capsys):
    row = HeatmapRow(pipeline="my-pipe")
    row.buckets[6] = 4
    print_heatmap([row])
    out = capsys.readouterr().out
    assert "my-pipe" in out
    assert "peak=06h" in out
