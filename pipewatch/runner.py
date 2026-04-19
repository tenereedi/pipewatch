"""Orchestrates loading config, running checks, reporting, and alerting."""

import logging
from typing import List

from pipewatch.checks import CheckResult, check_http, check_freshness, check_row_count
from pipewatch.config import WatchConfig, PipelineConfig
from pipewatch.reporter import print_results, print_summary, summarize
from pipewatch.alerts import dispatch_alerts

logger = logging.getLogger(__name__)


def _run_pipeline_checks(pipeline: PipelineConfig) -> List[CheckResult]:
    """Run all configured checks for a single pipeline."""
    results: List[CheckResult] = []

    if pipeline.url:
        results.append(check_http(pipeline.name, pipeline.url, pipeline.timeout))

    if pipeline.freshness_path and pipeline.max_age_seconds is not None:
        results.append(
            check_freshness(pipeline.name, pipeline.freshness_path, pipeline.max_age_seconds)
        )

    if pipeline.row_count_query and pipeline.min_rows is not None:
        results.append(
            check_row_count(pipeline.name, pipeline.row_count_query, pipeline.min_rows)
        )

    return results


def run_all_checks(config: WatchConfig) -> List[CheckResult]:
    """Run checks for every pipeline defined in the config."""
    all_results: List[CheckResult] = []
    for pipeline in config.pipelines:
        logger.debug("Checking pipeline: %s", pipeline.name)
        results = _run_pipeline_checks(pipeline)
        if not results:
            logger.warning("No checks configured for pipeline '%s'", pipeline.name)
        all_results.extend(results)
    return all_results


def run_and_report(config: WatchConfig) -> bool:
    """Run checks, print results, dispatch alerts. Returns True if all healthy."""
    results = run_all_checks(config)
    print_results(results)
    summary = summarize(results)
    print_summary(summary)
    dispatch_alerts(results, config.alerts)
    return summary["healthy"] == summary["total"]
