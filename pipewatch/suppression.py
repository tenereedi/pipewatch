"""Suppression rules: skip alerting for pipelines matching name patterns or tags."""
from __future__ import annotations

import fnmatch
import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checks import CheckResult


@dataclass
class SuppressionRule:
    """A single suppression rule."""
    pipeline_pattern: str = "*"          # glob pattern matched against pipeline name
    tags: List[str] = field(default_factory=list)  # any-match on result tags
    reason: str = ""

    def matches(self, result: CheckResult) -> bool:
        """Return True if this rule suppresses *result*."""
        name_match = fnmatch.fnmatch(result.pipeline, self.pipeline_pattern)
        if not name_match:
            return False
        if self.tags:
            result_tags = getattr(result, "tags", []) or []
            return any(t in result_tags for t in self.tags)
        return True


@dataclass
class SuppressionConfig:
    rules: List[SuppressionRule] = field(default_factory=list)

    @staticmethod
    def from_file(path: str) -> "SuppressionConfig":
        """Load suppression rules from a JSON file."""
        if not os.path.exists(path):
            return SuppressionConfig()
        with open(path, "r") as fh:
            data = json.load(fh)
        rules = [
            SuppressionRule(
                pipeline_pattern=r.get("pipeline_pattern", "*"),
                tags=r.get("tags", []),
                reason=r.get("reason", ""),
            )
            for r in data.get("rules", [])
        ]
        return SuppressionConfig(rules=rules)

    def is_suppressed(self, result: CheckResult) -> bool:
        """Return True if any rule matches *result*."""
        return any(rule.matches(result) for rule in self.rules)

    def filter(self, results: List[CheckResult]) -> List[CheckResult]:
        """Return only results that are NOT suppressed."""
        return [r for r in results if not self.is_suppressed(r)]

    def suppressed_reason(self, result: CheckResult) -> Optional[str]:
        """Return the reason string of the first matching rule, or None."""
        for rule in self.rules:
            if rule.matches(result):
                return rule.reason or "(no reason given)"
        return None
