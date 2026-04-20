"""CLI subcommand for snapshot capture and diff."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.snapshot import (
    DEFAULT_SNAPSHOT_PATH,
    diff_snapshots,
    load_snapshot,
    save_snapshot,
)
from pipewatch.runner import run_all_checks
from pipewatch.config import load


def add_snapshot_subcommand(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("snapshot", help="Capture or diff pipeline snapshots")
    p.add_argument(
        "action",
        choices=["capture", "diff"],
        help="'capture' saves current state; 'diff' compares with last snapshot",
    )
    p.add_argument("--config", default="pipewatch/example_config.yaml", help="Config file path")
    p.add_argument("--snapshot-file", default=str(DEFAULT_SNAPSHOT_PATH), help="Snapshot file path")


def handle_snapshot(args: argparse.Namespace) -> None:
    snap_path = Path(args.snapshot_file)
    cfg = load(args.config)
    results = run_all_checks(cfg)

    if args.action == "capture":
        snap = save_snapshot(results, snap_path)
        print(f"[snapshot] Saved: {snap}")
        return

    # diff
    previous = load_snapshot(snap_path)
    if previous is None:
        print("[snapshot] No previous snapshot found. Run 'capture' first.")
        return

    diff = diff_snapshots(previous, results)
    if diff.has_changes:
        print("[snapshot] Changes detected since last snapshot:")
        print(str(diff))
    else:
        print("[snapshot] No changes since last snapshot.")
        print(str(diff))
