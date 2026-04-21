"""CLI sub-command: pipewatch notify — run checks and dispatch notifications."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load
from pipewatch.notifier import NotifierConfig, dispatch_notifications
from pipewatch.runner import run_all_checks


def add_notifier_subcommand(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser(
        "notify",
        help="Run pipeline checks and dispatch notifications to configured channels.",
    )
    p.add_argument("config", help="Path to pipewatch YAML config file.")
    p.add_argument(
        "--webhook",
        metavar="URL",
        default=None,
        help="Override webhook URL from config.",
    )
    p.add_argument(
        "--all",
        dest="all_results",
        action="store_true",
        help="Notify for all results, not just failures.",
    )
    p.add_argument(
        "--no-stdout",
        dest="no_stdout",
        action="store_true",
        help="Suppress stdout output.",
    )


def handle_notify(args: argparse.Namespace) -> int:
    """Execute the notify sub-command. Returns exit code."""
    try:
        watch_cfg = load(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}", file=sys.stderr)
        return 2

    results = run_all_checks(watch_cfg)

    notifier_cfg = NotifierConfig(
        webhook_url=args.webhook,
        stdout=not args.no_stdout,
        only_failures=not args.all_results,
    )

    outcomes = dispatch_notifications(results, notifier_cfg)

    failures = [r for r in results if not r.is_healthy()]
    if failures:
        print(f"\n{len(failures)} failure(s) detected.", file=sys.stderr)
        return 1
    return 0
