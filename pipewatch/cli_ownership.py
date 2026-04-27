"""CLI subcommand for managing pipeline ownership."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.ownership import (
    get_owner,
    init_ownership_db,
    list_owners,
    remove_owner,
    set_owner,
)

_DEFAULT_DB = Path("pipewatch_ownership.db")


def add_ownership_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("ownership", help="Manage pipeline ownership records")
    sub = p.add_subparsers(dest="ownership_cmd")

    # set
    s = sub.add_parser("set", help="Assign an owner to a pipeline")
    s.add_argument("pipeline", help="Pipeline name")
    s.add_argument("owner", help="Owner name or team")
    s.add_argument("--contact", default=None, help="Contact email or Slack handle")
    s.add_argument("--db", default=str(_DEFAULT_DB), help="Path to ownership DB")

    # get
    g = sub.add_parser("get", help="Show owner for a pipeline")
    g.add_argument("pipeline", help="Pipeline name")
    g.add_argument("--db", default=str(_DEFAULT_DB))

    # list
    ls = sub.add_parser("list", help="List all ownership records")
    ls.add_argument("--db", default=str(_DEFAULT_DB))

    # remove
    r = sub.add_parser("remove", help="Remove ownership record")
    r.add_argument("pipeline", help="Pipeline name")
    r.add_argument("--db", default=str(_DEFAULT_DB))


def handle_ownership(args: argparse.Namespace) -> bool:
    db = Path(getattr(args, "db", str(_DEFAULT_DB)))
    init_ownership_db(db)

    cmd = getattr(args, "ownership_cmd", None)

    if cmd == "set":
        rec = set_owner(args.pipeline, args.owner, contact=args.contact, db_path=db)
        print(f"Owner set: {rec}")
        return True

    if cmd == "get":
        rec = get_owner(args.pipeline, db_path=db)
        if rec is None:
            print(f"No owner registered for '{args.pipeline}'.")
            return False
        print(rec)
        return True

    if cmd == "list":
        records = list_owners(db_path=db)
        if not records:
            print("No ownership records found.")
            return True
        for rec in records:
            print(rec)
        return True

    if cmd == "remove":
        removed = remove_owner(args.pipeline, db_path=db)
        if removed:
            print(f"Removed ownership record for '{args.pipeline}'.")
        else:
            print(f"No record found for '{args.pipeline}'.")
        return removed

    print("No ownership subcommand specified. Use set/get/list/remove.")
    return False
