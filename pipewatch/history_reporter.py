"""Format and print pipeline check history from the database."""

from typing import List, Optional
from pipewatch.history import load_recent
from pipewatch.reporter import _colorize
from pathlib import Path


def _status_label(healthy: int) -> str:
    return _colorize("PASS", "green") if healthy else _colorize("FAIL", "red")


def print_history(
    pipeline: Optional[str] = None,
    limit: int = 20,
    db_path: Optional[Path] = None,
) -> None:
    """Print recent check results in a human-readable table."""
    kwargs = {"limit": limit}
    if pipeline:
        kwargs["pipeline"] = pipeline
    if db_path:
        kwargs["db_path"] = db_path

    rows = load_recent(**kwargs)

    if not rows:
        print("No history found.")
        return

    header = f"{'#':<4} {'Pipeline':<20} {'Type':<12} {'Status':<10} {'Message':<30} {'Timestamp'}"
    print(header)
    print("-" * len(header))
    for i, row in enumerate(rows, 1):
        status = _status_label(row["healthy"])
        print(
            f"{i:<4} {row['pipeline']:<20} {row['check_type']:<12} {status:<10} "
            f"{(row['message'] or '')[:30]:<30} {row['timestamp']}"
        )


def history_summary(pipeline: Optional[str] = None, limit: int = 100, db_path: Optional[Path] = None) -> dict:
    """Return pass/fail counts for recent history."""
    kwargs = {"limit": limit}
    if pipeline:
        kwargs["pipeline"] = pipeline
    if db_path:
        kwargs["db_path"] = db_path

    rows = load_recent(**kwargs)
    total = len(rows)
    passed = sum(1 for r in rows if r["healthy"])
    failed = total - passed
    return {"total": total, "passed": passed, "failed": failed}
