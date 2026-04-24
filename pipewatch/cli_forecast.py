"""CLI subcommand: pipewatch forecast"""

from __future__ import annotations

import argparse

from pipewatch.forecast import forecast_all, ForecastResult
from pipewatch.history import load_recent


def add_forecast_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "forecast",
        help="Forecast future failure rates based on recent history trends.",
    )
    parser.add_argument("--db", default="pipewatch_history.db", help="Path to history DB")
    parser.add_argument("--window", type=int, default=40, help="Number of recent records to analyse")
    parser.add_argument("--min-records", type=int, default=10, help="Minimum records required")
    parser.add_argument("--at-risk-only", action="store_true", help="Only show at-risk pipelines")
    parser.set_defaults(func=handle_forecast)


def _discover_pipelines(db_path: str, window: int) -> list[str]:
    """Return distinct pipeline names present in recent history."""
    records = load_recent(db_path, limit=window * 20)
    seen: list[str] = []
    for r in records:
        if r.pipeline not in seen:
            seen.append(r.pipeline)
    return seen


def handle_forecast(args: argparse.Namespace) -> None:
    pipelines = _discover_pipelines(args.db, args.window)

    if not pipelines:
        print("No pipeline history found.")
        return

    forecasts = forecast_all(
        db_path=args.db,
        pipelines=pipelines,
        window=args.window,
        min_records=args.min_records,
    )

    if not forecasts:
        print("Not enough history data to produce forecasts.")
        return

    if args.at_risk_only:
        forecasts = [f for f in forecasts if f.is_at_risk]
        if not forecasts:
            print("No at-risk pipelines detected.")
            return

    print(f"{'Pipeline':<30} {'Predicted':>10} {'Delta':>8}  Trend")
    print("-" * 62)
    for f in sorted(forecasts, key=lambda x: -x.predicted_failure_rate):
        risk_flag = " ⚠" if f.is_at_risk else ""
        print(
            f"{f.pipeline:<30} {f.predicted_failure_rate:>9.1%} "
            f"{f.trend_delta:>+8.1%}  "
            f"{'worsening' if f.trend_delta > 0.05 else ('improving' if f.trend_delta < -0.05 else 'stable')}"
            f"{risk_flag}"
        )
