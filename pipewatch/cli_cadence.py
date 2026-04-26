"""CLI subcommand for cadence checks."""

from __future__ import annotations

import argparse
import sys
from typing import List

from pipewatch.cadence import CadencePolicy, check_all_cadences


def add_cadence_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("cadence", help="Check pipeline run cadence")
    parser.add_argument(
        "--db", default="pipewatch_history.db", help="Path to history database"
    )
    parser.add_argument(
        "--pipeline",
        action="append",
        dest="pipelines",
        metavar="NAME:INTERVAL",
        help="Pipeline name and expected interval in seconds (e.g. my_pipe:300). Repeatable.",
    )
    parser.add_argument(
        "--grace",
        type=int,
        default=60,
        help="Grace period in seconds before a pipeline is considered overdue (default: 60)",
    )


def handle_cadence(args: argparse.Namespace) -> bool:
    if not args.pipelines:
        print("No pipelines specified. Use --pipeline NAME:INTERVAL.", file=sys.stderr)
        return False

    policies: List[CadencePolicy] = []
    for spec in args.pipelines:
        try:
            name, interval_str = spec.rsplit(":", 1)
            interval = int(interval_str)
        except (ValueError, AttributeError):
            print(
                f"Invalid pipeline spec '{spec}'. Expected NAME:INTERVAL_SECONDS.",
                file=sys.stderr,
            )
            return False
        policies.append(
            CadencePolicy(
                pipeline=name,
                expected_interval_seconds=interval,
                grace_seconds=args.grace,
            )
        )

    results = check_all_cadences(policies, db_path=args.db)
    all_ok = True
    for result in results:
        print(str(result))
        if result.is_overdue:
            all_ok = False

    if not all_ok:
        print("\n[cadence] One or more pipelines are overdue.", file=sys.stderr)

    return all_ok
