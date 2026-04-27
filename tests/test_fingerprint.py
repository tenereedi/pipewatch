"""Tests for pipewatch.fingerprint."""

from __future__ import annotations

import time
import pytest

from pipewatch.checks import CheckResult
from pipewatch.fingerprint import (
    FingerprintRecord,
    _make_fingerprint,
    init_fingerprint_db,
    load_fingerprints,
    record_fingerprint,
)


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "fp.db")
    init_fingerprint_db(db)
    return db


def _r(pipeline: str, healthy: bool = False, message: str = "") -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        healthy=healthy,
        message=message,
        timestamp=time.time(),
    )


def test_init_db_creates_file(tmp_path):
    db = str(tmp_path / "new.db")
    init_fingerprint_db(db)
    import os
    assert os.path.exists(db)


def test_make_fingerprint_stable():
    r = _r("pipe-a", message="timeout")
    assert _make_fingerprint(r) == _make_fingerprint(r)


def test_make_fingerprint_differs_by_pipeline():
    r1 = _r("pipe-a", message="err")
    r2 = _r("pipe-b", message="err")
    assert _make_fingerprint(r1) != _make_fingerprint(r2)


def test_make_fingerprint_differs_by_message():
    r1 = _r("pipe-a", message="timeout")
    r2 = _r("pipe-a", message="connection refused")
    assert _make_fingerprint(r1) != _make_fingerprint(r2)


def test_record_fingerprint_first_time(tmp_db):
    r = _r("pipe-a", message="timeout")
    rec = record_fingerprint(r, tmp_db)
    assert isinstance(rec, FingerprintRecord)
    assert rec.occurrences == 1
    assert rec.pipeline == "pipe-a"
    assert rec.message == "timeout"


def test_record_fingerprint_increments_count(tmp_db):
    r = _r("pipe-a", message="timeout")
    record_fingerprint(r, tmp_db)
    record_fingerprint(r, tmp_db)
    rec = record_fingerprint(r, tmp_db)
    assert rec.occurrences == 3


def test_different_messages_create_separate_fingerprints(tmp_db):
    r1 = _r("pipe-a", message="timeout")
    r2 = _r("pipe-a", message="connection refused")
    record_fingerprint(r1, tmp_db)
    record_fingerprint(r2, tmp_db)
    records = load_fingerprints(tmp_db)
    assert len(records) == 2


def test_load_fingerprints_filter_by_pipeline(tmp_db):
    record_fingerprint(_r("pipe-a", message="err"), tmp_db)
    record_fingerprint(_r("pipe-b", message="err"), tmp_db)
    results = load_fingerprints(tmp_db, pipeline="pipe-a")
    assert all(r.pipeline == "pipe-a" for r in results)
    assert len(results) == 1


def test_load_fingerprints_sorted_by_occurrences(tmp_db):
    r1 = _r("pipe-a", message="timeout")
    r2 = _r("pipe-b", message="500 error")
    record_fingerprint(r1, tmp_db)
    record_fingerprint(r1, tmp_db)
    record_fingerprint(r2, tmp_db)
    records = load_fingerprints(tmp_db)
    assert records[0].occurrences >= records[-1].occurrences


def test_fingerprint_str_contains_key_info(tmp_db):
    r = _r("pipe-x", message="bad gateway")
    rec = record_fingerprint(r, tmp_db)
    s = str(rec)
    assert "pipe-x" in s
    assert "bad gateway" in s
    assert "occurrences=1" in s
