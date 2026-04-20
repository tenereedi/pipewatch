"""Entry point for the pipewatch CLI."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load
from pipewatch.runner import run_and_report
from pipewatch.cli_history import add_history_subcommand, handle_history
from pipewatch.cli_trending import add_trending_subcommand, handle_trending
from pipewatch.cli_baseline import add_baseline_subcommand, handle_baseline
from pipewatch.cli_retention import add_retention_subcommand, handle_retention
from pipewatch.cli_silencer import add_silencer_subcommand, handle_silence
from pipewatch.cli_snapshot import add_snapshot_subcommand, handle_snapshot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and alert on data pipeline health.",
    )
    parser.add_argument(
        "--config",
        default="pipewatch/example_config.yaml",
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--db",
        default="pipewatch_history.db",
        help="Path to SQLite history database",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Skip saving results to history",
    )

    subparsers = parser.add_subparsers(dest="command")
    add_history_subcommand(subparsers)
    add_trending_subcommand(subparsers)
    add_baseline_subcommand(subparsers)
    add_retention_subcommand(subparsers)
    add_silencer_subcommand(subparsers)
    add_snapshot_subcommand(subparsers)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "history":
        handle_history(args)
        return
    if args.command == "trending":
        handle_trending(args)
        return
    if args.command == "baseline":
        handle_baseline(args)
        return
    if args.command == "retention":
        handle_retention(args)
        return
    if args.command == "silence":
        handle_silence(args)
        return
    if args.command == "snapshot":
        handle_snapshot(args)
        return

    cfg = load(args.config)
    ok = run_and_report(cfg, db_path=args.db, save=(not args.no_save))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
