"""CLI sub-commands for heartbeat tracking."""

from __future__ import annotations

import argparse
import time
from typing import List

from pipewatch.heartbeat import (
    check_all_heartbeats,
    init_heartbeat_db,
    record_heartbeat,
)

_DEFAULT_DB = "pipewatch_heartbeat.db"


def add_heartbeat_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("heartbeat", help="Manage pipeline heartbeats")
    sub = p.add_subparsers(dest="heartbeat_cmd")

    # record
    rec = sub.add_parser("record", help="Record a heartbeat for a pipeline")
    rec.add_argument("pipeline", help="Pipeline name")
    rec.add_argument("--db", default=_DEFAULT_DB)

    # check
    chk = sub.add_parser("check", help="Check whether pipelines are alive")
    chk.add_argument("pipeline", nargs="+", help="pipeline:threshold_seconds pairs, e.g. my_pipe:300")
    chk.add_argument("--db", default=_DEFAULT_DB)

    p.set_defaults(func=handle_heartbeat)


def handle_heartbeat(args: argparse.Namespace) -> bool:
    init_heartbeat_db(args.db)

    if args.heartbeat_cmd == "record":
        record_heartbeat(args.db, args.pipeline)
        print(f"Heartbeat recorded for '{args.pipeline}' at {time.time():.0f}")
        return True

    if args.heartbeat_cmd == "check":
        specs: List[dict] = []
        for token in args.pipeline:
            if ":" not in token:
                print(f"Invalid spec '{token}', expected pipeline:threshold_seconds")
                return False
            name, raw_threshold = token.rsplit(":", 1)
            try:
                threshold = float(raw_threshold)
            except ValueError:
                print(f"Invalid threshold '{raw_threshold}' for pipeline '{name}'")
                return False
            specs.append({"pipeline": name, "threshold_seconds": threshold})

        results = check_all_heartbeats(args.db, specs)
        all_alive = True
        for r in results:
            print(r)
            if not r.is_alive:
                all_alive = False
        return all_alive

    print("No heartbeat sub-command given. Use 'record' or 'check'.")
    return False
