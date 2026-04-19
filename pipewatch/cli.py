"""CLI entry point for pipewatch."""

import sys
import logging
import argparse

from pipewatch.config import load
from pipewatch.runner import run_and_report
from pipewatch.scheduler import PipelineScheduler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and alert on data pipeline health.",
    )
    parser.add_argument(
        "-c", "--config",
        default="pipewatch.yaml",
        help="Path to YAML config file (default: pipewatch.yaml)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        metavar="SECONDS",
        help="Run checks repeatedly on this interval. Omit for a single run.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        config = load(args.config)
    except FileNotFoundError:
        print(f"[pipewatch] Config file not found: {args.config}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"[pipewatch] Failed to load config: {exc}", file=sys.stderr)
        return 2

    if args.interval:
        scheduler = PipelineScheduler(
            interval_seconds=args.interval,
            check_fn=lambda: run_and_report(config),
        )
        scheduler.start()
        return 0
    else:
        all_healthy = run_and_report(config)
        return 0 if all_healthy else 1


if __name__ == "__main__":
    sys.exit(main())
