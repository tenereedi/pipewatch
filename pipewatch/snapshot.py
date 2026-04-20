"""Snapshot module: capture and compare pipeline check states over time."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.checks import CheckResult

DEFAULT_SNAPSHOT_PATH = Path(".pipewatch_snapshots.json")


@dataclass
class Snapshot:
    timestamp: float
    results: List[Dict]

    def __str__(self) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        return f"Snapshot(ts={ts}, pipelines={len(self.results)})"


@dataclass
class SnapshotDiff:
    new_failures: List[str] = field(default_factory=list)
    recovered: List[str] = field(default_factory=list)
    unchanged_healthy: int = 0
    unchanged_unhealthy: int = 0

    @property
    def has_changes(self) -> bool:
        return bool(self.new_failures or self.recovered)

    def __str__(self) -> str:
        lines = []
        if self.new_failures:
            lines.append(f"  New failures: {', '.join(self.new_failures)}")
        if self.recovered:
            lines.append(f"  Recovered:    {', '.join(self.recovered)}")
        lines.append(f"  Stable healthy: {self.unchanged_healthy}")
        lines.append(f"  Stable unhealthy: {self.unchanged_unhealthy}")
        return "\n".join(lines)


def _results_to_dicts(results: List[CheckResult]) -> List[Dict]:
    return [
        {"pipeline": r.pipeline, "check": r.check, "healthy": r.healthy, "detail": r.detail}
        for r in results
    ]


def save_snapshot(results: List[CheckResult], path: Path = DEFAULT_SNAPSHOT_PATH) -> Snapshot:
    snap = Snapshot(timestamp=time.time(), results=_results_to_dicts(results))
    with open(path, "w") as f:
        json.dump({"timestamp": snap.timestamp, "results": snap.results}, f, indent=2)
    return snap


def load_snapshot(path: Path = DEFAULT_SNAPSHOT_PATH) -> Optional[Snapshot]:
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    return Snapshot(timestamp=data["timestamp"], results=data["results"])


def diff_snapshots(previous: Snapshot, current: List[CheckResult]) -> SnapshotDiff:
    prev_map: Dict[str, bool] = {
        f"{r['pipeline']}::{r['check']}": r["healthy"] for r in previous.results
    }
    curr_map: Dict[str, bool] = {
        f"{r.pipeline}::{r.check}": r.healthy for r in current
    }

    diff = SnapshotDiff()
    for key, healthy in curr_map.items():
        was_healthy = prev_map.get(key, True)
        if was_healthy and not healthy:
            diff.new_failures.append(key)
        elif not was_healthy and healthy:
            diff.recovered.append(key)
        elif healthy:
            diff.unchanged_healthy += 1
        else:
            diff.unchanged_unhealthy += 1
    return diff
