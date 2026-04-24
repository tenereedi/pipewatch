"""Pipeline health scoring: compute a numeric health score (0–100) for each pipeline
based on recent check results stored in history."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_recent
from pipewatch.checks import CheckResult


@dataclass
class PipelineScore:
    pipeline: str
    score: float          # 0.0 – 100.0
    total: int
    healthy: int
    unhealthy: int

    def __str__(self) -> str:
        bar = _score_bar(self.score)
        return (
            f"{self.pipeline:<30} score={self.score:5.1f}  "
            f"[{bar}]  ({self.healthy}/{self.total} healthy)"
        )

    @property
    def grade(self) -> str:
        if self.score >= 90:
            return "A"
        if self.score >= 75:
            return "B"
        if self.score >= 50:
            return "C"
        if self.score >= 25:
            return "D"
        return "F"


def _score_bar(score: float, width: int = 10) -> str:
    filled = round(score / 100 * width)
    return "#" * filled + "-" * (width - filled)


def compute_score(
    db_path: str,
    pipeline: str,
    limit: int = 50,
) -> Optional[PipelineScore]:
    """Return a PipelineScore for *pipeline* using the most recent *limit* results."""
    results: List[CheckResult] = load_recent(db_path, pipeline=pipeline, limit=limit)
    if not results:
        return None
    total = len(results)
    healthy = sum(1 for r in results if r.healthy)
    unhealthy = total - healthy
    score = (healthy / total) * 100.0
    return PipelineScore(
        pipeline=pipeline,
        score=round(score, 2),
        total=total,
        healthy=healthy,
        unhealthy=unhealthy,
    )


def compute_all_scores(
    db_path: str,
    limit: int = 50,
) -> List[PipelineScore]:
    """Compute scores for every pipeline found in recent history."""
    all_results: List[CheckResult] = load_recent(db_path, limit=limit * 20)
    pipelines = sorted({r.pipeline for r in all_results})
    scores = []
    for name in pipelines:
        s = compute_score(db_path, pipeline=name, limit=limit)
        if s is not None:
            scores.append(s)
    scores.sort(key=lambda s: s.score)
    return scores
