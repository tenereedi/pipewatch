"""Tests for pipewatch.digest module."""
import pytest
from datetime import datetime, timezone
from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.digest import build_digest, print_digest, DigestEntry


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def _r(pipeline: str, check: str, ok: bool) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check=check,
        ok=ok,
        message="ok" if ok else "fail",
        latency_ms=10.0,
    )


def test_build_digest_empty(tmp_db):
    entries = build_digest(tmp_db, hours=24)
    assert entries == []


def test_build_digest_all_healthy(tmp_db):
    save_results(tmp_db, [_r("pipe_a", "http", True), _r("pipe_a", "freshness", True)])
    entries = build_digest(tmp_db, hours=24)
    assert len(entries) == 1
    e = entries[0]
    assert e.pipeline == "pipe_a"
    assert e.healthy == 2
    assert e.unhealthy == 0
    assert e.failure_rate == 0.0


def test_build_digest_mixed_health(tmp_db):
    save_results(tmp_db, [
        _r("pipe_a", "http", True),
        _r("pipe_a", "http", False),
        _r("pipe_b", "freshness", True),
    ])
    entries = build_digest(tmp_db, hours=24)
    assert len(entries) == 2
    by_name = {e.pipeline: e for e in entries}
    assert by_name["pipe_a"].failure_rate == pytest.approx(0.5)
    assert by_name["pipe_b"].failure_rate == 0.0


def test_build_digest_filter_by_pipeline(tmp_db):
    save_results(tmp_db, [
        _r("pipe_a", "http", True),
        _r("pipe_b", "http", False),
    ])
    entries = build_digest(tmp_db, hours=24, pipeline="pipe_a")
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe_a"


def test_digest_entry_str():
    from pipewatch.trending import TrendSummary
    ts = TrendSummary(pipeline="p", total=4, healthy=4, failure_rate=0.0, trending_down=False)
    e = DigestEntry(pipeline="p", total_checks=4, healthy=4, unhealthy=0, failure_rate=0.0, trend=ts)
    assert "p" in str(e)
    assert "4/4" in str(e)


def test_print_digest_no_data(tmp_db, capsys):
    print_digest(tmp_db, hours=24)
    out = capsys.readouterr().out
    assert "No data" in out


def test_print_digest_with_data(tmp_db, capsys):
    save_results(tmp_db, [_r("pipe_a", "http", False)])
    print_digest(tmp_db, hours=24)
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "degraded" in out
