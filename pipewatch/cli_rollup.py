"""CLI subcommand: pipewatch rollup — show time-bucketed health summaries."""

from __future__ import annotations

import argparse
import sys

from pipewatch.rollup import compute_rollup, print_rollup


def add_rollup_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "rollup",
        help="Show aggregated health summaries over a time window.",
    )
    p.add_argument(
        "--db",
        default="pipewatch_history.db",
        help="Path to history database (default: pipewatch_history.db).",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Filter to a single pipeline name.",
    )
    p.add_argument(
        "--window",
        choices=["1h", "6h", "24h"],
        default="1h",
        help="Time window to aggregate over (default: 1h).",
    )


def handle_rollup(args: argparse.Namespace) -> bool:
    try:
        buckets = compute_rollup(
            db_path=args.db,
            pipeline=getattr(args, "pipeline", None),
            window=getattr(args, "window", "1h"),
        )
    except ValueError as exc:
        print(f"[rollup] Error: {exc}", file=sys.stderr)
        return False

    print_rollup(buckets)

    if not buckets:
        return True

    any_unhealthy = any(b.unhealthy > 0 for b in buckets)
    return not any_unhealthy
