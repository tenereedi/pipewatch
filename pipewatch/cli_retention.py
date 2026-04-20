"""CLI sub-command for managing history retention."""

from __future__ import annotations

import argparse

from pipewatch.retention import RetentionPolicy, prune_history


def add_retention_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "retention",
        help="Prune pipeline history records older than a given age",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        required=True,
        metavar="DAYS",
        help="Delete records older than this many days",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        metavar="NAME",
        help="Limit pruning to a specific pipeline (default: all pipelines)",
    )
    parser.add_argument(
        "--db",
        default="pipewatch_history.db",
        metavar="PATH",
        help="Path to the history database (default: pipewatch_history.db)",
    )


def handle_retention(args: argparse.Namespace) -> None:
    try:
        policy = RetentionPolicy(
            max_age_days=args.max_age_days,
            pipeline=args.pipeline,
        )
    except ValueError as exc:
        print(f"[retention] Invalid policy: {exc}")
        return

    deleted = prune_history(args.db, policy)
    scope = f"pipeline '{args.pipeline}'" if args.pipeline else "all pipelines"
    print(f"[retention] Pruned {deleted} record(s) older than {args.max_age_days} day(s) from {scope}.")
