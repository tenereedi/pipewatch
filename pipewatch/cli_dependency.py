"""CLI subcommand for pipeline dependency inspection."""
from __future__ import annotations

import argparse
from typing import List

from pipewatch.checks import CheckResult
from pipewatch.dependency import (
    DependencyNode,
    check_dependencies,
    validate_graph,
)


def add_dependency_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("dependency", help="Inspect pipeline dependency graph")
    p.add_argument("--validate", action="store_true", help="Check for cycles")
    p.set_defaults(func=handle_dependency)


def handle_dependency(
    args: argparse.Namespace,
    nodes: List[DependencyNode],
    results: List[CheckResult],
) -> bool:
    """Print dependency status; return False if any violations exist."""
    if not nodes:
        print("No dependency nodes configured.")
        return True

    if args.validate:
        error = validate_graph(nodes)
        if error:
            print(f"[ERROR] {error}")
            return False
        print("[OK] Dependency graph is acyclic.")

    unhealthy = {r.pipeline_name for r in results if not r.is_healthy}
    violations = check_dependencies(nodes, unhealthy)

    if not violations:
        print("All pipeline dependencies are satisfied.")
        return True

    for v in violations:
        print(str(v))
    return False
