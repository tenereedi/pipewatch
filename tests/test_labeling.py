"""Tests for pipewatch.labeling."""
import pytest

from pipewatch.labeling import (
    LabelSet,
    filter_by_label,
    get_labels,
    init_labeling_db,
    remove_label,
    set_label,
)
from pipewatch.checks import CheckResult


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "labels.db")
    init_labeling_db(db)
    return db


def _r(name: str, healthy: bool = True) -> CheckResult:
    return CheckResult(
        pipeline_name=name,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
    )


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

def test_init_db_creates_file(tmp_path):
    db = str(tmp_path / "new_labels.db")
    init_labeling_db(db)
    import os
    assert os.path.exists(db)


# ---------------------------------------------------------------------------
# set / get
# ---------------------------------------------------------------------------

def test_set_and_get_label(tmp_db):
    set_label(tmp_db, "pipe-a", "env", "prod")
    ls = get_labels(tmp_db, "pipe-a")
    assert ls.get("env") == "prod"


def test_get_labels_empty(tmp_db):
    ls = get_labels(tmp_db, "unknown")
    assert ls.labels == {}


def test_set_label_overwrite(tmp_db):
    set_label(tmp_db, "pipe-a", "env", "staging")
    set_label(tmp_db, "pipe-a", "env", "prod")
    ls = get_labels(tmp_db, "pipe-a")
    assert ls.get("env") == "prod"


def test_set_multiple_labels(tmp_db):
    set_label(tmp_db, "pipe-a", "env", "prod")
    set_label(tmp_db, "pipe-a", "team", "data")
    ls = get_labels(tmp_db, "pipe-a")
    assert ls.get("team") == "data"
    assert len(ls.labels) == 2


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------

def test_remove_label_returns_true(tmp_db):
    set_label(tmp_db, "pipe-a", "env", "prod")
    assert remove_label(tmp_db, "pipe-a", "env") is True
    assert get_labels(tmp_db, "pipe-a").get("env") is None


def test_remove_nonexistent_label_returns_false(tmp_db):
    assert remove_label(tmp_db, "pipe-x", "env") is False


# ---------------------------------------------------------------------------
# LabelSet __str__
# ---------------------------------------------------------------------------

def test_label_set_str_no_labels():
    ls = LabelSet(pipeline="p")
    assert "no labels" in str(ls)


def test_label_set_str_with_labels():
    ls = LabelSet(pipeline="p", labels={"env": "prod", "team": "data"})
    s = str(ls)
    assert "env=prod" in s
    assert "team=data" in s


# ---------------------------------------------------------------------------
# filter_by_label
# ---------------------------------------------------------------------------

def test_filter_by_label_matches(tmp_db):
    set_label(tmp_db, "pipe-a", "env", "prod")
    set_label(tmp_db, "pipe-b", "env", "staging")
    results = [_r("pipe-a"), _r("pipe-b")]
    matched = filter_by_label(results, tmp_db, "env", "prod")
    assert len(matched) == 1
    assert matched[0].pipeline_name == "pipe-a"


def test_filter_by_label_no_match(tmp_db):
    results = [_r("pipe-a"), _r("pipe-b")]
    matched = filter_by_label(results, tmp_db, "env", "prod")
    assert matched == []
