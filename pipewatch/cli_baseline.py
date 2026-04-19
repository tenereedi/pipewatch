"""CLI subcommand for baseline deviation reporting."""

import argparse
from pipewatch.baseline import check_all_baselines
from pipewatch.history import load_recent


def add_baseline_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "baseline", help="Compare recent pipeline health against historical baseline"
    )
    parser.add_argument("--db", default="pipewatch_history.db", help="Path to history DB")
    parser.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    parser.add_argument(
        "--baseline-window", type=int, default=100, help="Rows used for baseline"
    )
    parser.add_argument(
        "--current-window", type=int, default=20, help="Rows used for current period"
    )
    parser.add_argument(
        "--threshold", type=float, default=0.10, help="Degradation threshold (0-1)"
    )


def handle_baseline(args: argparse.Namespace) -> bool:
    """Print baseline reports; return False if any pipeline is degraded."""
    # Discover pipeline names from DB if not filtered
    if args.pipeline:
        pipelines = [args.pipeline]
    else:
        rows = load_recent(args.db, limit=500)
        seen = dict.fromkeys(r["pipeline"] for r in rows)
        pipelines = list(seen)

    if not pipelines:
        print("No pipeline history found.")
        return True

    reports = check_all_baselines(
        pipelines,
        db_path=args.db,
        baseline_window=args.baseline_window,
        current_window=args.current_window,
    )

    if not reports:
        print("Not enough history to compute baselines.")
        return True

    any_degraded = False
    for report in reports:
        print(report)
        if report.is_degraded(threshold=args.threshold):
            any_degraded = True

    return not any_degraded
