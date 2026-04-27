"""Tests for pipewatch.cli_heartbeat."""

from __future__ import annotations

import argparse
import time

import pytest

from pipewatch.cli_heartbeat import add_heartbeat_subcommand, handle_heartbeat
from pipewatch.heartbeat import init_heartbeat_db, record_heartbeat


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "hb.db")
    init_heartbeat_db(db)
    return db


def _args(tmp_db, heartbeat_cmd, **kwargs):
    ns = argparse.Namespace(db=tmp_db, heartbeat_cmd=heartbeat_cmd, **kwargs)
    return ns


def test_record_returns_true(tmp_db):
    args = _args(tmp_db, "record", pipeline="my_pipe")
    assert handle_heartbeat(args) is True


def test_check_alive_returns_true(tmp_db):
    record_heartbeat(tmp_db, "live_pipe", ts=time.time() - 5)
    args = _args(tmp_db, "check", pipeline=["live_pipe:60"])
    assert handle_heartbeat(args) is True


def test_check_dead_returns_false(tmp_db):
    record_heartbeat(tmp_db, "stale_pipe", ts=time.time() - 9999)
    args = _args(tmp_db, "check", pipeline=["stale_pipe:60"])
    assert handle_heartbeat(args) is False


def test_check_never_seen_returns_false(tmp_db):
    args = _args(tmp_db, "check", pipeline=["ghost:120"])
    assert handle_heartbeat(args) is False


def test_check_invalid_spec_returns_false(tmp_db):
    args = _args(tmp_db, "check", pipeline=["no_colon_here"])
    assert handle_heartbeat(args) is False


def test_check_bad_threshold_returns_false(tmp_db):
    args = _args(tmp_db, "check", pipeline=["pipe:notanumber"])
    assert handle_heartbeat(args) is False


def test_no_subcommand_returns_false(tmp_db):
    args = _args(tmp_db, None)
    assert handle_heartbeat(args) is False


def test_add_heartbeat_subcommand_registers(tmp_db):
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    add_heartbeat_subcommand(sub)
    parsed = parser.parse_args(["heartbeat", "record", "my_pipe", "--db", tmp_db])
    assert parsed.pipeline == "my_pipe"
