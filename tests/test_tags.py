"""Tests for pipewatch.tags module."""
import pytest
from pipewatch.checks import CheckResult
from pipewatch.tags import (
    TagGroup,
    filter_by_tag,
    group_by_tag,
    print_tag_summary,
)


def _r(pipeline: str, healthy: bool, tag: str | None = None) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "fail",
        tag=tag,
    )


# ---------------------------------------------------------------------------
# group_by_tag
# ---------------------------------------------------------------------------

def test_group_by_tag_empty():
    assert group_by_tag([]) == []


def test_group_by_tag_single_tag():
    results = [_r("p1", True, "etl"), _r("p2", False, "etl")]
    groups = group_by_tag(results)
    assert len(groups) == 1
    assert groups[0].tag == "etl"
    assert groups[0].total == 2
    assert groups[0].healthy == 1
    assert groups[0].unhealthy == 1


def test_group_by_tag_multiple_tags():
    results = [
        _r("p1", True, "etl"),
        _r("p2", True, "api"),
        _r("p3", False, "api"),
    ]
    groups = group_by_tag(results)
    tags = {g.tag for g in groups}
    assert tags == {"etl", "api"}


def test_group_by_tag_untagged_falls_back():
    results = [_r("p1", True, None)]
    groups = group_by_tag(results)
    assert groups[0].tag == "untagged"


# ---------------------------------------------------------------------------
# TagGroup properties
# ---------------------------------------------------------------------------

def test_tag_group_health_rate_empty():
    tg = TagGroup(tag="x")
    assert tg.health_rate == 1.0


def test_tag_group_str():
    tg = TagGroup(tag="etl", results=[_r("p", True, "etl"), _r("p2", False, "etl")])
    assert "etl" in str(tg)
    assert "1/2" in str(tg)
    assert "50%" in str(tg)


# ---------------------------------------------------------------------------
# filter_by_tag
# ---------------------------------------------------------------------------

def test_filter_by_tag_none_returns_all():
    results = [_r("p1", True, "etl"), _r("p2", False, "api")]
    assert filter_by_tag(results, None) == results


def test_filter_by_tag_empty_string_returns_all():
    results = [_r("p1", True, "etl")]
    assert filter_by_tag(results, "") == results


def test_filter_by_tag_case_insensitive():
    results = [_r("p1", True, "ETL"), _r("p2", False, "api")]
    filtered = filter_by_tag(results, "etl")
    assert len(filtered) == 1
    assert filtered[0].pipeline == "p1"


def test_filter_by_tag_no_match_returns_empty():
    results = [_r("p1", True, "etl")]
    assert filter_by_tag(results, "missing") == []


# ---------------------------------------------------------------------------
# print_tag_summary
# ---------------------------------------------------------------------------

def test_print_tag_summary_empty(capsys):
    print_tag_summary([])
    out = capsys.readouterr().out
    assert "No tag data" in out


def test_print_tag_summary_shows_tags(capsys):
    groups = [TagGroup(tag="etl", results=[_r("p", True, "etl")])]
    print_tag_summary(groups)
    out = capsys.readouterr().out
    assert "etl" in out
    assert "Tag Summary" in out
