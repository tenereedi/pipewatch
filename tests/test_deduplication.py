"""Tests for pipewatch.deduplication."""

import time
from pathlib import Path

import pytest

from pipewatch.checks import CheckResult
from pipewatch.deduplication import (
    DedupEntry,
    clear_resolved,
    init_dedup_db,
    is_duplicate,
    record_failure,
)


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "dedup.db"
    init_dedup_db(db)
    return db


def _r(pipeline: str, check: str, healthy: bool = False) -> CheckResult:
    return CheckResult(
        pipeline_name=pipeline,
        check_name=check,
        passed=healthy,
        message="ok" if healthy else "fail",
    )


def test_init_db_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "new_dedup.db"
    assert not db.exists()
    init_dedup_db(db)
    assert db.exists()


def test_record_failure_first_time_returns_count_1(tmp_db: Path) -> None:
    result = _r("pipe-a", "http")
    entry = record_failure(result, db_path=tmp_db)
    assert isinstance(entry, DedupEntry)
    assert entry.pipeline == "pipe-a"
    assert entry.check_name == "http"
    assert entry.count == 1


def test_record_failure_increments_count(tmp_db: Path) -> None:
    result = _r("pipe-a", "http")
    record_failure(result, db_path=tmp_db)
    record_failure(result, db_path=tmp_db)
    entry = record_failure(result, db_path=tmp_db)
    assert entry.count == 3


def test_is_duplicate_false_before_threshold(tmp_db: Path) -> None:
    result = _r("pipe-b", "freshness")
    record_failure(result, db_path=tmp_db)
    assert is_duplicate(result, min_count=2, db_path=tmp_db) is False


def test_is_duplicate_true_at_threshold(tmp_db: Path) -> None:
    result = _r("pipe-b", "freshness")
    record_failure(result, db_path=tmp_db)
    record_failure(result, db_path=tmp_db)
    assert is_duplicate(result, min_count=2, db_path=tmp_db) is True


def test_is_duplicate_false_for_unknown_pipeline(tmp_db: Path) -> None:
    result = _r("unknown-pipe", "http")
    assert is_duplicate(result, db_path=tmp_db) is False


def test_clear_resolved_removes_healthy_entries(tmp_db: Path) -> None:
    failing = _r("pipe-c", "http", healthy=False)
    record_failure(failing, db_path=tmp_db)
    record_failure(failing, db_path=tmp_db)
    assert is_duplicate(failing, min_count=2, db_path=tmp_db) is True

    resolved = _r("pipe-c", "http", healthy=True)
    clear_resolved([resolved], db_path=tmp_db)
    assert is_duplicate(failing, min_count=2, db_path=tmp_db) is False


def test_clear_resolved_does_not_remove_still_failing(tmp_db: Path) -> None:
    result = _r("pipe-d", "http", healthy=False)
    record_failure(result, db_path=tmp_db)
    record_failure(result, db_path=tmp_db)

    other_healthy = _r("pipe-x", "http", healthy=True)
    clear_resolved([other_healthy], db_path=tmp_db)
    assert is_duplicate(result, min_count=2, db_path=tmp_db) is True


def test_dedup_entry_age_seconds(tmp_db: Path) -> None:
    result = _r("pipe-e", "row_count")
    entry = record_failure(result, db_path=tmp_db)
    assert entry.age_seconds() >= 0.0
    assert entry.age_seconds() < 5.0
