"""Tests for pipewatch.runner."""

from unittest.mock import MagicMock, patch
from pipewatch.runner import run_all_checks, run_and_report
from pipewatch.checks import CheckResult
from pipewatch.config import WatchConfig, PipelineConfig


def _make_config(pipelines=None, alerts=None):
    return WatchConfig(
        pipelines=pipelines or [],
        alerts=alerts or {},
    )


def _make_pipeline(**kwargs):
    defaults = dict(
        name="test",
        url=None,
        freshness_path=None,
        max_age_seconds=None,
        row_count_query=None,
        min_rows=None,
        timeout=10,
    )
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


def test_run_all_checks_no_pipelines():
    config = _make_config()
    results = run_all_checks(config)
    assert results == []


def test_run_all_checks_http_only():
    pipeline = _make_pipeline(name="api", url="http://example.com")
    config = _make_config(pipelines=[pipeline])
    fake_result = CheckResult(pipeline="api", check="http", healthy=True, message="OK")
    with patch("pipewatch.runner.check_http", return_value=fake_result) as mock_http:
        results = run_all_checks(config)
    mock_http.assert_called_once_with("api", "http://example.com", 10)
    assert len(results) == 1
    assert results[0].healthy


def test_run_and_report_all_healthy_returns_true():
    config = _make_config()
    healthy_result = CheckResult(pipeline="p", check="http", healthy=True, message="OK")
    with patch("pipewatch.runner.run_all_checks", return_value=[healthy_result]), \
         patch("pipewatch.runner.print_results"), \
         patch("pipewatch.runner.print_summary"), \
         patch("pipewatch.runner.dispatch_alerts"):
        ok = run_and_report(config)
    assert ok is True


def test_run_and_report_unhealthy_returns_false():
    config = _make_config()
    bad_result = CheckResult(pipeline="p", check="http", healthy=False, message="fail")
    with patch("pipewatch.runner.run_all_checks", return_value=[bad_result]), \
         patch("pipewatch.runner.print_results"), \
         patch("pipewatch.runner.print_summary"), \
         patch("pipewatch.runner.dispatch_alerts"):
        ok = run_and_report(config)
    assert ok is False
