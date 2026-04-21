"""Tests for pipewatch.ratelimit."""

from __future__ import annotations

import time
import pytest
from pathlib import Path

from pipewatch.ratelimit import (
    RateLimitPolicy,
    clear_ratelimit,
    init_ratelimit_db,
    is_rate_limited,
    record_alert_sent,
)


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "ratelimit.db"
    init_ratelimit_db(db)
    return db


def test_invalid_cooldown_raises():
    with pytest.raises(ValueError):
        RateLimitPolicy(cooldown_seconds=-1)


def test_not_rate_limited_when_no_record(tmp_db: Path):
    policy = RateLimitPolicy(cooldown_seconds=300)
    assert is_rate_limited("pipe_a", "http", policy, tmp_db) is False


def test_rate_limited_immediately_after_record(tmp_db: Path):
    now = time.time()
    policy = RateLimitPolicy(cooldown_seconds=300)
    record_alert_sent("pipe_a", "http", tmp_db, now=now)
    assert is_rate_limited("pipe_a", "http", policy, tmp_db, now=now + 10) is True


def test_not_rate_limited_after_cooldown_expires(tmp_db: Path):
    now = time.time()
    policy = RateLimitPolicy(cooldown_seconds=60)
    record_alert_sent("pipe_a", "http", tmp_db, now=now)
    assert is_rate_limited("pipe_a", "http", policy, tmp_db, now=now + 61) is False


def test_rate_limit_is_per_pipeline_and_check_type(tmp_db: Path):
    now = time.time()
    policy = RateLimitPolicy(cooldown_seconds=300)
    record_alert_sent("pipe_a", "http", tmp_db, now=now)
    # Different check type should not be limited
    assert is_rate_limited("pipe_a", "freshness", policy, tmp_db, now=now + 1) is False
    # Different pipeline should not be limited
    assert is_rate_limited("pipe_b", "http", policy, tmp_db, now=now + 1) is False


def test_record_updates_existing_entry(tmp_db: Path):
    now = time.time()
    policy = RateLimitPolicy(cooldown_seconds=60)
    record_alert_sent("pipe_a", "http", tmp_db, now=now - 120)  # old record
    assert is_rate_limited("pipe_a", "http", policy, tmp_db, now=now) is False
    # Re-record with current time
    record_alert_sent("pipe_a", "http", tmp_db, now=now)
    assert is_rate_limited("pipe_a", "http", policy, tmp_db, now=now + 10) is True


def test_clear_ratelimit_removes_all_records(tmp_db: Path):
    now = time.time()
    policy = RateLimitPolicy(cooldown_seconds=300)
    record_alert_sent("pipe_a", "http", tmp_db, now=now)
    record_alert_sent("pipe_b", "freshness", tmp_db, now=now)
    clear_ratelimit(tmp_db)
    assert is_rate_limited("pipe_a", "http", policy, tmp_db, now=now + 1) is False
    assert is_rate_limited("pipe_b", "freshness", policy, tmp_db, now=now + 1) is False


def test_zero_cooldown_never_rate_limits(tmp_db: Path):
    now = time.time()
    policy = RateLimitPolicy(cooldown_seconds=0)
    record_alert_sent("pipe_a", "http", tmp_db, now=now)
    assert is_rate_limited("pipe_a", "http", policy, tmp_db, now=now) is False
