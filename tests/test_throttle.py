"""Tests for pipewatch.throttle."""

import time
import pytest
from pathlib import Path

from pipewatch.throttle import (
    ThrottlePolicy,
    init_throttle_db,
    is_throttled,
    record_alert,
    clear_throttle,
)


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "throttle.db"
    init_throttle_db(db)
    return db


def test_invalid_cooldown_raises():
    with pytest.raises(ValueError, match="cooldown_seconds must be positive"):
        ThrottlePolicy(pipeline="p", cooldown_seconds=0)


def test_not_throttled_when_no_record(tmp_db: Path):
    policy = ThrottlePolicy(pipeline="pipe-a", cooldown_seconds=60)
    assert is_throttled(policy, db_path=tmp_db) is False


def test_throttled_immediately_after_record(tmp_db: Path):
    policy = ThrottlePolicy(pipeline="pipe-b", cooldown_seconds=3600)
    record_alert(policy, db_path=tmp_db)
    assert is_throttled(policy, db_path=tmp_db) is True


def test_not_throttled_after_cooldown_expires(tmp_db: Path):
    policy = ThrottlePolicy(pipeline="pipe-c", cooldown_seconds=10)
    past = time.time() - 20
    record_alert(policy, db_path=tmp_db, now=past)
    assert is_throttled(policy, db_path=tmp_db) is False


def test_clear_removes_throttle(tmp_db: Path):
    policy = ThrottlePolicy(pipeline="pipe-d", cooldown_seconds=3600)
    record_alert(policy, db_path=tmp_db)
    assert is_throttled(policy, db_path=tmp_db) is True
    clear_throttle("pipe-d", db_path=tmp_db)
    assert is_throttled(policy, db_path=tmp_db) is False


def test_alert_type_isolation(tmp_db: Path):
    p1 = ThrottlePolicy(pipeline="pipe-e", cooldown_seconds=3600, alert_type="email")
    p2 = ThrottlePolicy(pipeline="pipe-e", cooldown_seconds=3600, alert_type="webhook")
    record_alert(p1, db_path=tmp_db)
    assert is_throttled(p1, db_path=tmp_db) is True
    assert is_throttled(p2, db_path=tmp_db) is False


def test_record_updates_existing(tmp_db: Path):
    policy = ThrottlePolicy(pipeline="pipe-f", cooldown_seconds=3600)
    old_time = time.time() - 7200
    record_alert(policy, db_path=tmp_db, now=old_time)
    # overwrite with a fresh timestamp
    record_alert(policy, db_path=tmp_db)
    assert is_throttled(policy, db_path=tmp_db) is True
