"""CLI subcommand for managing pipeline alert cooldowns."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.cooldown import CooldownPolicy, init_cooldown_db

_DEFAULT_DB = Path(".pipewatch_cooldown.db")


def add_cooldown_subcommand(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("cooldown", help="Manage pipeline alert cooldowns")
    sub = p.add_subparsers(dest="cooldown_cmd")

    chk = sub.add_parser("check", help="Check if a pipeline is in cooldown")
    chk.add_argument("pipeline", help="Pipeline name")
    chk.add_argument("--window", type=int, default=300, help="Cooldown window in seconds")
    chk.add_argument("--db", default=str(_DEFAULT_DB))

    rec = sub.add_parser("record", help="Record that an alert was sent for a pipeline")
    rec.add_argument("pipeline", help="Pipeline name")
    rec.add_argument("--window", type=int, default=300)
    rec.add_argument("--db", default=str(_DEFAULT_DB))

    rst = sub.add_parser("reset", help="Reset cooldown for a pipeline")
    rst.add_argument("pipeline", help="Pipeline name")
    rst.add_argument("--window", type=int, default=300)
    rst.add_argument("--db", default=str(_DEFAULT_DB))


def handle_cooldown(args: argparse.Namespace) -> bool:
    if not hasattr(args, "cooldown_cmd") or args.cooldown_cmd is None:
        print("[cooldown] No subcommand provided. Use check, record, or reset.")
        return False

    db = Path(args.db)
    init_cooldown_db(db)

    try:
        policy = CooldownPolicy(pipeline=args.pipeline, window_seconds=args.window)
    except ValueError as exc:
        print(f"[cooldown] Invalid policy: {exc}")
        return False

    if args.cooldown_cmd == "check":
        cooling = policy.is_cooling_down(db)
        status = "COOLING DOWN" if cooling else "ready"
        print(f"[cooldown] {args.pipeline}: {status} (window={args.window}s)")
        return not cooling

    if args.cooldown_cmd == "record":
        policy.record_alert(db)
        print(f"[cooldown] Recorded alert for {args.pipeline!r}.")
        return True

    if args.cooldown_cmd == "reset":
        policy.reset(db)
        print(f"[cooldown] Reset cooldown for {args.pipeline!r}.")
        return True

    return False
