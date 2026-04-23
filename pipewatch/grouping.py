"""Group pipeline check results by a named field for aggregated reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.checks import CheckResult


@dataclass
class ResultGroup:
    """A named group of CheckResults with aggregated stats."""

    name: str
    results: List[CheckResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def healthy(self) -> int:
        return sum(1 for r in self.results if r.is_healthy)

    @property
    def unhealthy(self) -> int:
        return self.total - self.healthy

    @property
    def health_rate(self) -> float:
        if self.total == 0:
            return 1.0
        return self.healthy / self.total

    def __str__(self) -> str:
        pct = f"{self.health_rate * 100:.1f}%"
        return (
            f"[{self.name}] total={self.total} "
            f"healthy={self.healthy} unhealthy={self.unhealthy} ({pct})"
        )


def group_by_source(results: List[CheckResult]) -> Dict[str, ResultGroup]:
    """Group results by their pipeline name prefix (before the first '/')."""
    groups: Dict[str, ResultGroup] = {}
    for result in results:
        key = result.pipeline_name.split("/")[0] if "/" in result.pipeline_name else result.pipeline_name
        if key not in groups:
            groups[key] = ResultGroup(name=key)
        groups[key].results.append(result)
    return groups


def group_by_check_type(results: List[CheckResult]) -> Dict[str, ResultGroup]:
    """Group results by their check type (e.g. 'http', 'freshness', 'row_count')."""
    groups: Dict[str, ResultGroup] = {}
    for result in results:
        key = result.check_type or "unknown"
        if key not in groups:
            groups[key] = ResultGroup(name=key)
        groups[key].results.append(result)
    return groups


def print_groups(groups: Dict[str, ResultGroup]) -> None:
    """Print a summary table of grouped results."""
    if not groups:
        print("No results to group.")
        return
    print(f"{'Group':<30} {'Total':>6} {'Healthy':>8} {'Unhealthy':>10} {'Rate':>7}")
    print("-" * 65)
    for name, group in sorted(groups.items()):
        pct = f"{group.health_rate * 100:.1f}%"
        print(f"{name:<30} {group.total:>6} {group.healthy:>8} {group.unhealthy:>10} {pct:>7}")
