"""Tests for pipewatch.cli_dependency."""
from __future__ import annotations

import argparse
import pytest

from pipewatch.checks import CheckResult
from pipewatch.dependency import DependencyNode
from pipewatch.cli_dependency import handle_dependency


def _args(**kwargs) -> argparse.Namespace:
    defaults = {"validate": False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _r(name: str, healthy: bool) -> CheckResult:
    return CheckResult(
        pipeline_name=name,
        check_type="http",
        passed=healthy,
        message="ok" if healthy else "fail",
    )


# ---------------------------------------------------------------------------

def test_no_nodes_returns_true(capsys):
    ok = handle_dependency(_args(), nodes=[], results=[])
    assert ok is True
    out = capsys.readouterr().out
    assert "No dependency" in out


def test_all_healthy_no_violations(capsys):
    nodes = [DependencyNode("a", depends_on=["b"]), DependencyNode("b")]
    results = [_r("a", True), _r("b", True)]
    ok = handle_dependency(_args(), nodes=nodes, results=results)
    assert ok is True
    assert "satisfied" in capsys.readouterr().out


def test_unhealthy_dependency_returns_false(capsys):
    nodes = [DependencyNode("a", depends_on=["b"]), DependencyNode("b")]
    results = [_r("a", True), _r("b", False)]
    ok = handle_dependency(_args(), nodes=nodes, results=results)
    assert ok is False
    assert "BLOCKED" in capsys.readouterr().out


def test_validate_acyclic_prints_ok(capsys):
    nodes = [DependencyNode("a", depends_on=["b"]), DependencyNode("b")]
    ok = handle_dependency(_args(validate=True), nodes=nodes, results=[])
    assert ok is True
    assert "acyclic" in capsys.readouterr().out


def test_validate_cycle_returns_false(capsys):
    nodes = [
        DependencyNode("a", depends_on=["b"]),
        DependencyNode("b", depends_on=["a"]),
    ]
    ok = handle_dependency(_args(validate=True), nodes=nodes, results=[])
    assert ok is False
    assert "ERROR" in capsys.readouterr().out
