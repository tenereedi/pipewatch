"""Tag-based filtering and grouping for pipeline checks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checks import CheckResult


@dataclass
class TagGroup:
    tag: str
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
        pct = self.health_rate * 100
        return (
            f"[{self.tag}] {self.healthy}/{self.total} healthy ({pct:.0f}%)"
        )


def group_by_tag(results: List[CheckResult]) -> List[TagGroup]:
    """Group a flat list of CheckResults by their pipeline tag."""
    groups: dict[str, TagGroup] = {}
    for result in results:
        tag = result.tag or "untagged"
        if tag not in groups:
            groups[tag] = TagGroup(tag=tag)
        groups[tag].results.append(result)
    return list(groups.values())


def filter_by_tag(
    results: List[CheckResult], tag: Optional[str]
) -> List[CheckResult]:
    """Return only results whose tag matches *tag* (case-insensitive).

    If *tag* is None or empty, the original list is returned unchanged.
    """
    if not tag:
        return results
    tag_lower = tag.lower()
    return [r for r in results if (r.tag or "").lower() == tag_lower]


def print_tag_summary(groups: List[TagGroup]) -> None:
    """Print a summary table of health per tag."""
    if not groups:
        print("No tag data available.")
        return
    print("\n--- Tag Summary ---")
    for group in sorted(groups, key=lambda g: g.tag):
        print(f"  {group}")
    print()
