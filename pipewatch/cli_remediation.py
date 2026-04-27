"""CLI subcommand for managing remediation hints."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.remediation import (
    init_remediation_db,
    set_hint,
    get_hint,
    list_hints,
)

_DB_DEFAULT = Path(".pipewatch_remediation.db")


def add_remediation_subcommand(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("remediation", help="Manage remediation hints for pipelines")
    sub = p.add_subparsers(dest="remediation_cmd")

    # set
    s = sub.add_parser("set", help="Set a remediation hint")
    s.add_argument("pipeline", help="Pipeline name")
    s.add_argument("check_type", help="Check type (e.g. http, freshness)")
    s.add_argument("hint", help="Remediation hint text")
    s.add_argument("--db", default=str(_DB_DEFAULT))

    # get
    g = sub.add_parser("get", help="Get a remediation hint")
    g.add_argument("pipeline")
    g.add_argument("check_type")
    g.add_argument("--db", default=str(_DB_DEFAULT))

    # list
    ls = sub.add_parser("list", help="List remediation hints")
    ls.add_argument("--pipeline", default=None, help="Filter by pipeline")
    ls.add_argument("--db", default=str(_DB_DEFAULT))


def handle_remediation(args: argparse.Namespace) -> bool:
    db = Path(args.db)
    init_remediation_db(db)

    cmd = getattr(args, "remediation_cmd", None)

    if cmd == "set":
        hint = set_hint(args.pipeline, args.check_type, args.hint, db_path=db)
        print(f"Hint saved: {hint}")
        return True

    if cmd == "get":
        hint = get_hint(args.pipeline, args.check_type, db_path=db)
        if hint is None:
            print(f"No hint found for {args.pipeline}/{args.check_type}")
            return False
        print(hint)
        return True

    if cmd == "list":
        hints = list_hints(pipeline=args.pipeline, db_path=db)
        if not hints:
            print("No remediation hints stored.")
            return True
        for h in hints:
            print(h)
        return True

    print("No subcommand given. Use set / get / list.")
    return False
