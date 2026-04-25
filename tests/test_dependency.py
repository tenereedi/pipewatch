"""Tests for pipewatch.dependency."""
from __future__ import annotations

import pytest

from pipewatch.dependency import (
    DependencyNode,
    DependencyViolation,
    build_graph,
    check_dependencies,
    validate_graph,
    _detect_cycle,
)


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------

def test_build_graph_empty():
    assert build_graph([]) == {}


def test_build_graph_single_node():
    nodes = [DependencyNode("a", depends_on=["b", "c"])]
    assert build_graph(nodes) == {"a": ["b", "c"]}


def test_build_graph_multiple_nodes():
    nodes = [
        DependencyNode("a", depends_on=["b"]),
        DependencyNode("b", depends_on=[]),
    ]
    graph = build_graph(nodes)
    assert graph["a"] == ["b"]
    assert graph["b"] == []


# ---------------------------------------------------------------------------
# cycle detection
# ---------------------------------------------------------------------------

def test_no_cycle_linear():
    nodes = [
        DependencyNode("a", depends_on=["b"]),
        DependencyNode("b", depends_on=["c"]),
        DependencyNode("c"),
    ]
    assert validate_graph(nodes) is None


def test_cycle_direct():
    nodes = [
        DependencyNode("a", depends_on=["b"]),
        DependencyNode("b", depends_on=["a"]),
    ]
    result = validate_graph(nodes)
    assert result is not None
    assert "Cycle detected" in result


def test_cycle_indirect():
    nodes = [
        DependencyNode("a", depends_on=["b"]),
        DependencyNode("b", depends_on=["c"]),
        DependencyNode("c", depends_on=["a"]),
    ]
    result = validate_graph(nodes)
    assert result is not None


def test_no_cycle_empty():
    assert validate_graph([]) is None


# ---------------------------------------------------------------------------
# check_dependencies
# ---------------------------------------------------------------------------

def test_no_violations_when_all_healthy():
    nodes = [
        DependencyNode("a", depends_on=["b"]),
        DependencyNode("b"),
    ]
    violations = check_dependencies(nodes, unhealthy=set())
    assert violations == []


def test_violation_when_dependency_unhealthy():
    nodes = [
        DependencyNode("a", depends_on=["b"]),
        DependencyNode("b"),
    ]
    violations = check_dependencies(nodes, unhealthy={"b"})
    assert len(violations) == 1
    assert violations[0].pipeline == "a"
    assert violations[0].blocked_by == "b"


def test_multiple_violations():
    nodes = [
        DependencyNode("a", depends_on=["b", "c"]),
    ]
    violations = check_dependencies(nodes, unhealthy={"b", "c"})
    assert len(violations) == 2
    blocked_by = {v.blocked_by for v in violations}
    assert blocked_by == {"b", "c"}


def test_violation_str():
    v = DependencyViolation("a", "b", "dependency 'b' is unhealthy")
    assert "BLOCKED" in str(v)
    assert "a" in str(v)
    assert "b" in str(v)


def test_dependency_node_str_with_deps():
    n = DependencyNode("a", depends_on=["b"])
    assert "a" in str(n)
    assert "b" in str(n)


def test_dependency_node_str_no_deps():
    n = DependencyNode("standalone")
    assert "no dependencies" in str(n)
