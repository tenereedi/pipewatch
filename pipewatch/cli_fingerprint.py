"""CLI subcommand for failure fingerprint management."""

from __future__ import annotations

import argparse

from pipewatch.fingerprint import init_fingerprint_db, load_fingerprints

_DEFAULT_DB = "pipewatch_fingerprints.db"


def add_fingerprint_subcommand(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "fingerprint", help="View recurring failure fingerprints"
    )
    p.add_argument(
        "--db", default=_DEFAULT_DB, help="Path to fingerprint database"
    )
    p.add_argument(
        "--pipeline", default=None, help="Filter by pipeline name"
    )
    p.add_argument(
        "--min-occurrences",
        type=int,
        default=1,
        metavar="N",
        help="Only show fingerprints seen at least N times",
    )
    p.set_defaults(fingerprint_func=handle_fingerprint)


def handle_fingerprint(args: argparse.Namespace) -> bool:
    init_fingerprint_db(args.db)
    records = load_fingerprints(args.db, pipeline=getattr(args, "pipeline", None))
    filtered = [r for r in records if r.occurrences >= args.min_occurrences]

    if not filtered:
        print("No fingerprints found.")
        return True

    print(f"{'FINGERPRINT':<18} {'PIPELINE':<24} {'OCCURRENCES':>11}  MESSAGE")
    print("-" * 80)
    for rec in filtered:
        msg = rec.message[:40] + "..." if len(rec.message) > 40 else rec.message
        print(f"{rec.fingerprint:<18} {rec.pipeline:<24} {rec.occurrences:>11}  {msg}")

    print(f"\nTotal fingerprints shown: {len(filtered)}")
    return True
