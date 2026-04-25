"""CLI sub-commands for the audit log feature."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.audit import clear_audit_log, load_audit_log, record_action

_DEFAULT_DB = Path(".pipewatch_audit.db")


def add_audit_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    audit_p = subparsers.add_parser("audit", help="View or manage the audit log")
    sub = audit_p.add_subparsers(dest="audit_cmd")

    show_p = sub.add_parser("show", help="Print recent audit entries")
    show_p.add_argument("--limit", type=int, default=20, help="Max entries to show")
    show_p.add_argument("--command", default=None, help="Filter by command name")
    show_p.add_argument("--db", default=str(_DEFAULT_DB), help="Audit DB path")

    clear_p = sub.add_parser("clear", help="Delete all audit log entries")
    clear_p.add_argument("--db", default=str(_DEFAULT_DB), help="Audit DB path")

    record_p = sub.add_parser("record", help="Manually record an audit entry")
    record_p.add_argument("command", help="Command name to record")
    record_p.add_argument("--detail", default=None, help="Optional detail string")
    record_p.add_argument("--db", default=str(_DEFAULT_DB), help="Audit DB path")


def handle_audit(args: argparse.Namespace) -> bool:
    db = Path(getattr(args, "db", str(_DEFAULT_DB)))

    if args.audit_cmd == "show":
        entries = load_audit_log(
            db_path=db,
            limit=args.limit,
            command_filter=args.command,
        )
        if not entries:
            print("No audit entries found.")
            return True
        for entry in entries:
            print(entry)
        return True

    if args.audit_cmd == "clear":
        removed = clear_audit_log(db_path=db)
        print(f"Cleared {removed} audit log entries.")
        return True

    if args.audit_cmd == "record":
        entry = record_action(command=args.command, detail=args.detail, db_path=db)
        print(f"Recorded: {entry}")
        return True

    print("No audit sub-command provided. Use: show | clear | record")
    return False
