"""Pipeline dependency graph — track and validate inter-pipeline dependencies."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class DependencyNode:
    name: str
    depends_on: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        if self.depends_on:
            return f"{self.name} -> [{', '.join(self.depends_on)}]"
        return f"{self.name} (no dependencies)"


@dataclass
class DependencyViolation:
    pipeline: str
    blocked_by: str
    reason: str

    def __str__(self) -> str:
        return f"[BLOCKED] {self.pipeline} blocked by {self.blocked_by}: {self.reason}"


def build_graph(nodes: List[DependencyNode]) -> Dict[str, List[str]]:
    """Return adjacency map: pipeline -> list of dependencies."""
    return {n.name: list(n.depends_on) for n in nodes}


def _detect_cycle(graph: Dict[str, List[str]]) -> Optional[List[str]]:
    """Return a cycle path if one exists, otherwise None."""
    visited: Set[str] = set()
    stack: Set[str] = set()

    def dfs(node: str, path: List[str]) -> Optional[List[str]]:
        visited.add(node)
        stack.add(node)
        for neighbour in graph.get(node, []):
            if neighbour not in visited:
                result = dfs(neighbour, path + [neighbour])
                if result:
                    return result
            elif neighbour in stack:
                return path + [neighbour]
        stack.discard(node)
        return None

    for node in list(graph):
        if node not in visited:
            cycle = dfs(node, [node])
            if cycle:
                return cycle
    return None


def check_dependencies(
    nodes: List[DependencyNode],
    unhealthy: Set[str],
) -> List[DependencyViolation]:
    """Return violations for pipelines whose dependencies are unhealthy."""
    violations: List[DependencyViolation] = []
    for node in nodes:
        for dep in node.depends_on:
            if dep in unhealthy:
                violations.append(
                    DependencyViolation(
                        pipeline=node.name,
                        blocked_by=dep,
                        reason=f"dependency '{dep}' is unhealthy",
                    )
                )
    return violations


def validate_graph(nodes: List[DependencyNode]) -> Optional[str]:
    """Return an error string if the graph has a cycle, else None."""
    graph = build_graph(nodes)
    cycle = _detect_cycle(graph)
    if cycle:
        return "Cycle detected: " + " -> ".join(cycle)
    return None
