"""CLI subcommand for managing alert throttle records."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.throttle import (
    ThrottlePolicy,
    init_throttle_db,
    is_throttled,
    record_alert,
    clear_throttle,
    _DEFAULT_DB,
)


def add_throttle_subcommand(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("throttle", help="Manage alert throttle records")
    sub = p.add_subparsers(dest="throttle_cmd")

    chk = sub.add_parser("check", help="Check whether a pipeline alert is throttled")
    chk.add_argument("pipeline", help="Pipeline name")
    chk.add_argument("--alert-type", default="default")
    chk.add_argument("--cooldown", type=int, default=3600)
    chk.add_argument("--db", default=str(_DEFAULT_DB))

    rec = sub.add_parser("record", help="Record that an alert was just fired")
    rec.add_argument("pipeline", help="Pipeline name")
    rec.add_argument("--alert-type", default="default")
    rec.add_argument("--cooldown", type=int, default=3600)
    rec.add_argument("--db", default=str(_DEFAULT_DB))

    clr = sub.add_parser("clear", help="Clear throttle record for a pipeline")
    clr.add_argument("pipeline", help="Pipeline name")
    clr.add_argument("--alert-type", default="default")
    clr.add_argument("--db", default=str(_DEFAULT_DB))


def handle_throttle(args: argparse.Namespace) -> bool:
    cmd = getattr(args, "throttle_cmd", None)
    if cmd is None:
        print("Usage: pipewatch throttle {check,record,clear}")
        return False

    db = Path(args.db)
    init_throttle_db(db)

    if cmd == "check":
        policy = ThrottlePolicy(
            pipeline=args.pipeline,
            cooldown_seconds=args.cooldown,
            alert_type=args.alert_type,
        )
        throttled = is_throttled(policy, db_path=db)
        status = "THROTTLED" if throttled else "OK (not throttled)"
        print(f"{args.pipeline} [{args.alert_type}]: {status}")
        return not throttled

    if cmd == "record":
        policy = ThrottlePolicy(
            pipeline=args.pipeline,
            cooldown_seconds=args.cooldown,
            alert_type=args.alert_type,
        )
        record_alert(policy, db_path=db)
        print(f"Recorded alert for '{args.pipeline}' [{args.alert_type}]")
        return True

    if cmd == "clear":
        clear_throttle(args.pipeline, alert_type=args.alert_type, db_path=db)
        print(f"Cleared throttle for '{args.pipeline}' [{args.alert_type}]")
        return True

    return False
