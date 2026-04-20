"""CLI subcommand for managing pipeline silences."""

from __future__ import annotations

import argparse

from pipewatch.silencer import (
    add_silence,
    clear_silences,
    init_silencer_db,
    is_silenced,
    list_silences,
)


def add_silencer_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("silence", help="Manage pipeline alert silences")
    sub = parser.add_subparsers(dest="silence_cmd", required=True)

    add_p = sub.add_parser("add", help="Silence a pipeline for N seconds")
    add_p.add_argument("pipeline", help="Pipeline name to silence")
    add_p.add_argument("duration", type=int, help="Duration in seconds")
    add_p.add_argument("--reason", default=None, help="Optional reason")

    sub.add_parser("list", help="List active silences").add_argument(
        "--all", dest="include_expired", action="store_true", help="Include expired silences"
    )

    clear_p = sub.add_parser("clear", help="Remove silences")
    clear_p.add_argument("--pipeline", default=None, help="Clear only this pipeline")

    check_p = sub.add_parser("check", help="Check if a pipeline is currently silenced")
    check_p.add_argument("pipeline", help="Pipeline name to check")

    parser.set_defaults(func=handle_silence)


def handle_silence(args: argparse.Namespace, db_path: str = "pipewatch_history.db") -> None:
    init_silencer_db(db_path)

    if args.silence_cmd == "add":
        silence = add_silence(db_path, args.pipeline, args.duration, reason=getattr(args, "reason", None))
        print(f"Silenced: {silence}")

    elif args.silence_cmd == "list":
        include_expired = getattr(args, "include_expired", False)
        silences = list_silences(db_path, include_expired=include_expired)
        if not silences:
            print("No active silences.")
        else:
            for s in silences:
                print(f"  {s}")

    elif args.silence_cmd == "clear":
        pipeline = getattr(args, "pipeline", None)
        removed = clear_silences(db_path, pipeline=pipeline)
        target = pipeline if pipeline else "all pipelines"
        print(f"Cleared {removed} silence(s) for {target}.")

    elif args.silence_cmd == "check":
        if is_silenced(db_path, args.pipeline):
            print(f"  {args.pipeline} is currently SILENCED.")
        else:
            print(f"  {args.pipeline} is NOT silenced.")
