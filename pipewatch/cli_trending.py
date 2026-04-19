"""CLI subcommand for trend analysis."""
import argparse
from pipewatch.trending import compute_trend, has_any_trending_down
from pipewatch.reporter import _colorize


def add_trending_subcommand(subparsers) -> None:
    parser = subparsers.add_parser(
        "trend", help="Analyze historical failure trends"
    )
    parser.add_argument("--db", default="pipewatch_history.db",
                        help="Path to history database")
    parser.add_argument("--pipeline", default=None,
                        help="Filter by pipeline name")
    parser.add_argument("--window", type=int, default=20,
                        help="Number of recent records to analyze per pipeline")
    parser.add_argument("--threshold", type=float, default=0.4,
                        help="Failure rate threshold to flag as trending down")


def handle_trending(args: argparse.Namespace) -> int:
    summaries = compute_trend(
        db_path=args.db,
        pipeline=getattr(args, "pipeline", None),
        window=args.window,
        threshold=args.threshold,
    )

    if not summaries:
        print("No trend data available.")
        return 0

    print(f"{'Pipeline':<20} {'Check':<12} {'Failures':>8} {'Rate':>6}  Status")
    print("-" * 60)
    for s in summaries:
        status = _colorize("TRENDING DOWN", "red") if s.trending_down else _colorize("OK", "green")
        print(f"{s.pipeline:<20} {s.check_type:<12} {s.failures:>4}/{s.total:<3} {s.failure_rate:>5.0%}  {status}")

    if has_any_trending_down(summaries):
        print("\n⚠  One or more pipelines are trending down.")
        return 1
    return 0
