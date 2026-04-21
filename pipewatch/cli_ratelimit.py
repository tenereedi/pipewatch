"""CLI subcommand for managing alert rate limits."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.ratelimit import (
    DEFAULT_DB,
    RateLimitPolicy,
    clear_ratelimit,
    init_ratelimit_db,
    is_rate_limited,
    record_alert_sent,
)


def add_ratelimit_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("ratelimit", help="Manage alert rate limits")
    sub = parser.add_subparsers(dest="ratelimit_cmd")

    status_p = sub.add_parser("status", help="Check if a pipeline/check is rate limited")
    status_p.add_argument("pipeline", help="Pipeline name")
    status_p.add_argument("check_type", help="Check type (e.g. http, freshness)")
    status_p.add_argument("--cooldown", type=int, default=300, help="Cooldown in seconds")

    record_p = sub.add_parser("record", help="Record that an alert was sent")
    record_p.add_argument("pipeline", help="Pipeline name")
    record_p.add_argument("check_type", help="Check type")

    sub.add_parser("clear", help="Clear all rate limit records")

    parser.set_defaults(func=handle_ratelimit)


def handle_ratelimit(args: argparse.Namespace, db_path: Path = DEFAULT_DB) -> None:
    init_ratelimit_db(db_path)

    cmd = getattr(args, "ratelimit_cmd", None)
    if cmd == "status":
        policy = RateLimitPolicy(cooldown_seconds=args.cooldown)
        limited = is_rate_limited(args.pipeline, args.check_type, policy, db_path)
        if limited:
            print(
                f"[rate-limited] {args.pipeline}/{args.check_type} "
                f"is in cooldown (cooldown={args.cooldown}s)"
            )
        else:
            print(
                f"[ok] {args.pipeline}/{args.check_type} is NOT rate limited"
            )
    elif cmd == "record":
        record_alert_sent(args.pipeline, args.check_type, db_path)
        print(f"Recorded alert sent for {args.pipeline}/{args.check_type}")
    elif cmd == "clear":
        clear_ratelimit(db_path)
        print("All rate limit records cleared.")
    else:
        print("Usage: pipewatch ratelimit {status,record,clear} ...")
