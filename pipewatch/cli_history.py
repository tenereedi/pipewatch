"""CLI subcommand handlers for history inspection."""

import argparse
from pathlib import Path
from pipewatch.history import clear_history
from pipewatch.history_reporter import print_history, history_summary


def add_history_subcommand(subparsers: argparse._SubParsersAction) -> None:
    """Register the 'history' subcommand and its options."""
    parser = subparsers.add_parser("history", help="View or manage check history")
    parser.add_argument("--pipeline", "-p", default=None, help="Filter by pipeline name")
    parser.add_argument("--limit", "-n", type=int, default=20, help="Max rows to show (default: 20)")
    parser.add_argument("--summary", action="store_true", help="Show pass/fail summary instead of full table")
    parser.add_argument("--clear", action="store_true", help="Delete all stored history")
    parser.add_argument("--db", default=None, help="Path to history database file")
    parser.set_defaults(func=handle_history)


def handle_history(args: argparse.Namespace) -> int:
    """Execute the history subcommand."""
    db_path = Path(args.db) if args.db else None
    kwargs = {}
    if db_path:
        kwargs["db_path"] = db_path

    if args.clear:
        removed = clear_history(**kwargs)
        print(f"Cleared {removed} record(s) from history.")
        return 0

    if args.summary:
        s = history_summary(pipeline=args.pipeline, limit=args.limit, **kwargs)
        print(f"Total: {s['total']}  Passed: {s['passed']}  Failed: {s['failed']}")
        return 0 if s["failed"] == 0 else 1

    print_history(pipeline=args.pipeline, limit=args.limit, **kwargs)
    return 0
