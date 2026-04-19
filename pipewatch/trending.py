"""Analyze historical check results to detect trending failures."""
from dataclasses import dataclass
from typing import Optional
from pipewatch.history import load_recent, _connect


@dataclass
class TrendSummary:
    pipeline: str
    check_type: str
    total: int
    failures: int
    failure_rate: float
    trending_down: bool

    def __str__(self) -> str:
        status = "⚠ TRENDING DOWN" if self.trending_down else "OK"
        return (
            f"{self.pipeline}/{self.check_type}: "
            f"{self.failures}/{self.total} failures "
            f"({self.failure_rate:.0%}) [{status}]"
        )


def compute_trend(db_path: str, pipeline: Optional[str] = None,
                  window: int = 20, threshold: float = 0.4) -> list[TrendSummary]:
    """Return trend summaries for each pipeline/check_type pair."""
    rows = load_recent(db_path, pipeline=pipeline, limit=window)
    if not rows:
        return []

    grouped: dict[tuple[str, str], list[bool]] = {}
    for row in rows:
        key = (row["pipeline"], row["check_type"])
        grouped.setdefault(key, []).append(row["healthy"] == 1)

    summaries = []
    for (pipe, ctype), results in grouped.items():
        total = len(results)
        failures = sum(1 for r in results if not r)
        rate = failures / total if total else 0.0
        summaries.append(TrendSummary(
            pipeline=pipe,
            check_type=ctype,
            total=total,
            failures=failures,
            failure_rate=rate,
            trending_down=rate >= threshold,
        ))
    return summaries


def has_any_trending_down(summaries: list[TrendSummary]) -> bool:
    return any(s.trending_down for s in summaries)
