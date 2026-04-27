"""CLI sub-commands for quota management."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.quota import (
    QuotaPolicy,
    evaluate_quota,
    init_quota_db,
    record_failure,
)

_DEFAULT_DB = Path("pipewatch_quota.db")


def add_quota_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser("quota", help="Manage per-pipeline failure quotas")
    sub = parser.add_subparsers(dest="quota_cmd")

    rec = sub.add_parser("record", help="Record a failure event for a pipeline")
    rec.add_argument("pipeline", help="Pipeline name")
    rec.add_argument("--db", default=str(_DEFAULT_DB), help="Path to quota DB")

    chk = sub.add_parser("check", help="Check whether a pipeline has exceeded its quota")
    chk.add_argument("pipeline", help="Pipeline name")
    chk.add_argument("--max-failures", type=int, default=5, help="Allowed failures")
    chk.add_argument("--window", type=int, default=3600, help="Window in seconds")
    chk.add_argument("--db", default=str(_DEFAULT_DB), help="Path to quota DB")


def handle_quota(args: argparse.Namespace) -> bool:
    db = Path(getattr(args, "db", str(_DEFAULT_DB)))
    init_quota_db(db)

    cmd = getattr(args, "quota_cmd", None)
    if cmd == "record":
        record_failure(args.pipeline, db_path=db)
        print(f"Recorded failure for '{args.pipeline}'.")
        return True

    if cmd == "check":
        try:
            policy = QuotaPolicy(
                pipeline=args.pipeline,
                max_failures=args.max_failures,
                window_seconds=args.window,
            )
        except ValueError as exc:
            print(f"Invalid quota policy: {exc}")
            return False
        result = evaluate_quota(policy, db_path=db)
        print(result)
        return not result.exceeded

    print("No quota sub-command specified. Use 'record' or 'check'.")
    return False
