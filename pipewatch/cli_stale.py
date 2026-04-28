"""CLI sub-command: pipewatch stale — detect pipelines that have stopped reporting."""
from __future__ import annotations

import argparse
from typing import List

from pipewatch.history import load_recent
from pipewatch.stale import check_all_staleness


def _discover_pipelines(db_path: str) -> List[str]:
    """Return a sorted, unique list of pipeline names found in the history DB."""
    rows = load_recent(db_path, limit=500)
    seen: dict = {}
    for r in rows:
        seen[r.pipeline] = True
    return sorted(seen.keys())


def add_stale_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "stale",
        help="Detect pipelines that have not reported results recently",
    )
    parser.add_argument(
        "--db",
        default="pipewatch_history.db",
        help="Path to the history SQLite database",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=300,
        metavar="SECONDS",
        help="Seconds without a result before a pipeline is considered stale (default: 300)",
    )
    parser.add_argument(
        "--pipelines",
        nargs="*",
        metavar="NAME",
        help="Pipeline names to check (default: all pipelines in history)",
    )


def handle_stale(args: argparse.Namespace) -> bool:
    pipelines = args.pipelines or _discover_pipelines(args.db)
    if not pipelines:
        print("No pipelines found in history.")
        return False

    results = check_all_staleness(
        db_path=args.db,
        pipelines=pipelines,
        threshold_seconds=args.threshold,
    )

    any_stale = False
    for r in results:
        print(r)
        if r.is_stale:
            any_stale = True

    if any_stale:
        print("\n⚠ One or more pipelines are stale.")
    else:
        print("\n✓ All pipelines are reporting within the threshold.")

    return not any_stale
