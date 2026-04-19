"""Tests for cli_trending subcommand handler."""
import argparse
import pytest
from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.cli_trending import handle_trending


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "h.db")
    init_db(db)
    return db


def _args(db, pipeline=None, window=20, threshold=0.4):
    ns = argparse.Namespace(db=db, pipeline=pipeline,
                            window=window, threshold=threshold)
    return ns


def _r(pipeline, healthy):
    return CheckResult(pipeline=pipeline, check_type="http",
                       healthy=healthy, message="")


def test_handle_trending_no_data(tmp_db, capsys):
    code = handle_trending(_args(tmp_db))
    out = capsys.readouterr().out
    assert "No trend data" in out
    assert code == 0


def test_handle_trending_all_ok(tmp_db, capsys):
    save_results(tmp_db, [_r("pipe1", True)] * 10)
    code = handle_trending(_args(tmp_db, pipeline="pipe1"))
    out = capsys.readouterr().out
    assert "pipe1" in out
    assert code == 0


def test_handle_trending_returns_1_when_trending_down(tmp_db, capsys):
    save_results(tmp_db, [_r("bad_pipe", False)] * 9 + [_r("bad_pipe", True)])
    code = handle_trending(_args(tmp_db, pipeline="bad_pipe", threshold=0.4))
    assert code == 1
    out = capsys.readouterr().out
    assert "TRENDING DOWN" in out or "trending down" in out


def test_handle_trending_custom_threshold(tmp_db, capsys):
    # 3/10 failures — ok at 0.4 threshold, bad at 0.2
    save_results(tmp_db, [_r("p", False)] * 3 + [_r("p", True)] * 7)
    assert handle_trending(_args(tmp_db, pipeline="p", threshold=0.4)) == 0
    assert handle_trending(_args(tmp_db, pipeline="p", threshold=0.2)) == 1
