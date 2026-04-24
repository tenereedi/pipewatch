"""Tests for pipewatch.scoring."""

import time
import pytest

from pipewatch.history import init_db, save_results
from pipewatch.checks import CheckResult
from pipewatch.scoring import (
    PipelineScore,
    compute_score,
    compute_all_scores,
    _score_bar,
)


@pytest.fixture()
def tmp_db(tmp_path):
    path = str(tmp_path / "history.db")
    init_db(path)
    return path


def _r(pipeline: str, healthy: bool) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        timestamp=time.time(),
    )


def _populate(db, pipeline, healthy_count, unhealthy_count):
    results = [_r(pipeline, True)] * healthy_count + [_r(pipeline, False)] * unhealthy_count
    save_results(db, results)


# ---------------------------------------------------------------------------
# score bar
# ---------------------------------------------------------------------------

def test_score_bar_full():
    assert _score_bar(100.0) == "##########"


def test_score_bar_empty():
    assert _score_bar(0.0) == "----------"


def test_score_bar_half():
    assert _score_bar(50.0) == "#####-----"


# ---------------------------------------------------------------------------
# compute_score
# ---------------------------------------------------------------------------

def test_compute_score_no_data_returns_none(tmp_db):
    assert compute_score(tmp_db, pipeline="ghost") is None


def test_compute_score_all_healthy(tmp_db):
    _populate(tmp_db, "pipe-a", 10, 0)
    s = compute_score(tmp_db, "pipe-a")
    assert s is not None
    assert s.score == 100.0
    assert s.healthy == 10
    assert s.unhealthy == 0
    assert s.grade == "A"


def test_compute_score_all_unhealthy(tmp_db):
    _populate(tmp_db, "pipe-b", 0, 8)
    s = compute_score(tmp_db, "pipe-b")
    assert s is not None
    assert s.score == 0.0
    assert s.grade == "F"


def test_compute_score_mixed(tmp_db):
    _populate(tmp_db, "pipe-c", 3, 1)  # 75 %
    s = compute_score(tmp_db, "pipe-c")
    assert s is not None
    assert s.score == 75.0
    assert s.grade == "B"


def test_pipeline_score_str_contains_name(tmp_db):
    _populate(tmp_db, "my-pipeline", 5, 5)
    s = compute_score(tmp_db, "my-pipeline")
    assert "my-pipeline" in str(s)
    assert "50.0" in str(s)


# ---------------------------------------------------------------------------
# compute_all_scores
# ---------------------------------------------------------------------------

def test_compute_all_scores_empty(tmp_db):
    assert compute_all_scores(tmp_db) == []


def test_compute_all_scores_multiple_pipelines(tmp_db):
    _populate(tmp_db, "alpha", 10, 0)
    _populate(tmp_db, "beta", 5, 5)
    _populate(tmp_db, "gamma", 0, 10)
    scores = compute_all_scores(tmp_db)
    assert len(scores) == 3
    # sorted ascending by score
    assert scores[0].pipeline == "gamma"
    assert scores[-1].pipeline == "alpha"
