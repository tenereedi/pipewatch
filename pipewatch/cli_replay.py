"""CLI subcommand for replaying historical pipeline check results."""

from __future__ import annotations

import argparse
from typing import List

from pipewatch.replay import load_replay_window, replay_all, replay_summary


def add_replay_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "replay",
        help="Replay historical check results for one or more pipelines.",
    )
    parser.add_argument(
        "pipelines",
        nargs="*",
        metavar="PIPELINE",
        help="Pipeline names to replay (omit for all stored pipelines).",
    )
    parser.add_argument(
        "--db",
        default="pipewatch_history.db",
        help="Path to the history database (default: pipewatch_history.db).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of recent results to replay per pipeline (default: 50).",
    )
    parser.set_defaults(func=handle_replay)


def handle_replay(args: argparse.Namespace) -> None:
    """Handle the 'replay' subcommand.

    Loads historical check results for the specified pipelines from the
    database and prints a summary followed by individual result details.
    """
    pipelines: List[str] = args.pipelines

    if not pipelines:
        print("No pipelines specified. Pass pipeline names as arguments.")
        return

    try:
        windows = replay_all(args.db, pipelines, limit=args.limit)
    except FileNotFoundError:
        print(f"Error: Database file not found: {args.db}")
        return
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading replay data: {exc}")
        return

    for window in windows:
        print(replay_summary(window))
        for result in window.results:
            status = "OK" if result.is_healthy else "FAIL"
            ts = getattr(result, "timestamp", "n/a")
            print(f"  [{status}] {result.name}: {result.message} (ts={ts})")
