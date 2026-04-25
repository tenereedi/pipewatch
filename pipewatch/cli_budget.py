"""CLI subcommand for check-budget enforcement."""
from __future__ import annotations

import argparse
import sys
from typing import List

from pipewatch.budget import (
    BudgetPolicy,
    BudgetResult,
    check_all_budgets,
    init_budget_db,
    record_check,
)

_DEFAULT_DB = "pipewatch_budget.db"


def add_budget_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser("budget", help="Check-budget enforcement")
    sub = parser.add_subparsers(dest="budget_cmd")

    rec = sub.add_parser("record", help="Record that a check fired")
    rec.add_argument("pipeline", help="Pipeline name")
    rec.add_argument("--db", default=_DEFAULT_DB)

    chk = sub.add_parser("check", help="Evaluate budget for pipelines")
    chk.add_argument("pipelines", nargs="+", help="Pipeline names")
    chk.add_argument("--max-checks", type=int, default=100)
    chk.add_argument("--window", type=int, default=3600, help="Window in seconds")
    chk.add_argument("--db", default=_DEFAULT_DB)

    parser.set_defaults(func=handle_budget)


def handle_budget(args: argparse.Namespace) -> bool:
    init_budget_db(args.db)

    if args.budget_cmd == "record":
        record_check(args.db, args.pipeline)
        print(f"Recorded check for '{args.pipeline}'.")
        return True

    if args.budget_cmd == "check":
        try:
            policy = BudgetPolicy(
                max_checks=args.max_checks,
                window_seconds=args.window,
            )
        except ValueError as exc:
            print(f"Invalid policy: {exc}", file=sys.stderr)
            return False

        results: List[BudgetResult] = check_all_budgets(args.db, args.pipelines, policy)
        any_exceeded = False
        for r in results:
            print(r)
            if r.budget_exceeded:
                any_exceeded = True
        return not any_exceeded

    print("No budget subcommand given. Use 'record' or 'check'.", file=sys.stderr)
    return False
