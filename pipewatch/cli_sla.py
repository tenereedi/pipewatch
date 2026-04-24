"""CLI sub-command for SLA reporting."""
from __future__ import annotations

import argparse
from typing import List

from pipewatch.sla import SLAPolicy, check_all_slas, any_sla_breached
from pipewatch.history import load_recent


def add_sla_subcommand(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("sla", help="Check SLA compliance for pipelines")
    p.add_argument(
        "--db", default="pipewatch_history.db", help="Path to history database"
    )
    p.add_argument(
        "--target", type=float, default=0.95,
        help="Default success-rate target (0–1), default 0.95",
    )
    p.add_argument(
        "--window", type=int, default=60,
        help="Lookback window in minutes, default 60",
    )
    p.add_argument(
        "--pipeline", nargs="*", default=None,
        help="Limit to specific pipeline names (default: all discovered)",
    )
    p.set_defaults(func=handle_sla)


def _discover_pipelines(db_path: str) -> List[str]:
    """Return distinct pipeline names found in recent history."""
    rows = load_recent(db_path, limit=1000)
    seen: List[str] = []
    for r in rows:
        if r.pipeline not in seen:
            seen.append(r.pipeline)
    return seen


def handle_sla(args: argparse.Namespace) -> bool:
    pipelines: List[str] = args.pipeline or _discover_pipelines(args.db)

    if not pipelines:
        print("No pipeline history found.")
        return True

    policies = [
        SLAPolicy(
            pipeline=name,
            target_rate=args.target,
            window_minutes=args.window,
        )
        for name in pipelines
    ]

    results = check_all_slas(policies, args.db)

    for r in results:
        print(r)

    breached = any_sla_breached(results)
    if breached:
        print("\n⚠️  One or more SLA targets were NOT met.")
    else:
        print("\n✅  All SLA targets met.")

    return not breached
