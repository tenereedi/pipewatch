"""CLI subcommand for managing pipeline runbooks."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.runbook import (
    delete_runbook,
    get_runbook,
    init_runbook_db,
    list_runbooks,
    set_runbook,
)

_DEFAULT_DB = Path("pipewatch_runbook.db")


def add_runbook_subcommand(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser("runbook", help="Manage remediation runbooks for pipelines")
    p.add_argument("--db", type=Path, default=_DEFAULT_DB, metavar="FILE")
    sub = p.add_subparsers(dest="runbook_cmd")

    # set
    ps = sub.add_parser("set", help="Attach a runbook URL to a pipeline")
    ps.add_argument("pipeline")
    ps.add_argument("title")
    ps.add_argument("url")
    ps.add_argument("--notes", default="")

    # get
    pg = sub.add_parser("get", help="Show the runbook for a pipeline")
    pg.add_argument("pipeline")

    # list
    sub.add_parser("list", help="List all registered runbooks")

    # delete
    pd = sub.add_parser("delete", help="Remove a runbook entry")
    pd.add_argument("pipeline")


def handle_runbook(args: argparse.Namespace) -> bool:
    db: Path = args.db
    init_runbook_db(db)

    cmd = getattr(args, "runbook_cmd", None)

    if cmd == "set":
        entry = set_runbook(
            pipeline=args.pipeline,
            title=args.title,
            url=args.url,
            notes=args.notes,
            db_path=db,
        )
        print(f"Saved: {entry}")
        return True

    if cmd == "get":
        entry = get_runbook(args.pipeline, db_path=db)
        if entry is None:
            print(f"No runbook found for '{args.pipeline}'.")
            return False
        print(entry)
        return True

    if cmd == "list":
        entries = list_runbooks(db_path=db)
        if not entries:
            print("No runbooks registered.")
            return True
        for e in entries:
            print(e)
        return True

    if cmd == "delete":
        removed = delete_runbook(args.pipeline, db_path=db)
        if removed:
            print(f"Deleted runbook for '{args.pipeline}'.")
            return True
        print(f"No runbook found for '{args.pipeline}'.")
        return False

    print("No runbook subcommand given. Use set / get / list / delete.")
    return False
