"""Tests for pipewatch.grouping."""

from __future__ import annotations

import pytest

from pipewatch.checks import CheckResult
from pipewatch.grouping import (
    ResultGroup,
    group_by_check_type,
    group_by_source,
    print_groups,
)


def _r(
    name: str,
    check_type: str = "http",
    healthy: bool = True,
    message: str = "ok",
) -> CheckResult:
    return CheckResult(
        pipeline_name=name,
        check_type=check_type,
        is_healthy=healthy,
        message=message,
    )


# --- ResultGroup unit tests ---

def test_result_group_empty():
    g = ResultGroup(name="empty")
    assert g.total == 0
    assert g.healthy == 0
    assert g.unhealthy == 0
    assert g.health_rate == 1.0


def test_result_group_mixed():
    g = ResultGroup(name="test", results=[_r("a"), _r("b", healthy=False), _r("c")])
    assert g.total == 3
    assert g.healthy == 2
    assert g.unhealthy == 1
    assert abs(g.health_rate - 2 / 3) < 1e-9


def test_result_group_str_contains_name():
    g = ResultGroup(name="mygroup", results=[_r("x")])
    assert "mygroup" in str(g)
    assert "total=1" in str(g)


# --- group_by_source ---

def test_group_by_source_empty():
    assert group_by_source([]) == {}


def test_group_by_source_no_slash():
    results = [_r("alpha"), _r("alpha"), _r("beta")]
    groups = group_by_source(results)
    assert set(groups.keys()) == {"alpha", "beta"}
    assert groups["alpha"].total == 2
    assert groups["beta"].total == 1


def test_group_by_source_with_slash():
    results = [_r("warehouse/orders"), _r("warehouse/users"), _r("api/health")]
    groups = group_by_source(results)
    assert set(groups.keys()) == {"warehouse", "api"}
    assert groups["warehouse"].total == 2


# --- group_by_check_type ---

def test_group_by_check_type_empty():
    assert group_by_check_type([]) == {}


def test_group_by_check_type_mixed():
    results = [
        _r("p1", check_type="http"),
        _r("p2", check_type="freshness"),
        _r("p3", check_type="http"),
        _r("p4", check_type="row_count"),
    ]
    groups = group_by_check_type(results)
    assert groups["http"].total == 2
    assert groups["freshness"].total == 1
    assert groups["row_count"].total == 1


def test_group_by_check_type_unknown_fallback():
    r = CheckResult(pipeline_name="p", check_type=None, is_healthy=True, message="")
    groups = group_by_check_type([r])
    assert "unknown" in groups


# --- print_groups ---

def test_print_groups_empty(capsys):
    print_groups({})
    captured = capsys.readouterr()
    assert "No results" in captured.out


def test_print_groups_shows_names(capsys):
    groups = group_by_check_type([_r("p1", check_type="http"), _r("p2", check_type="freshness")])
    print_groups(groups)
    captured = capsys.readouterr()
    assert "http" in captured.out
    assert "freshness" in captured.out
