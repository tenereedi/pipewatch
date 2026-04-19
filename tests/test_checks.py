"""Tests for pipeline health checks."""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from pipewatch.checks import check_http, check_freshness, CheckResult


# --- check_http ---

def _mock_response(status_code):
    resp = MagicMock()
    resp.status_code = status_code
    return resp


def test_check_http_ok():
    with patch("pipewatch.checks.requests.get", return_value=_mock_response(200)):
        result = check_http("my_pipeline", "http://example.com/health")
    assert result.status == "ok"
    assert result.pipeline_name == "my_pipeline"


def test_check_http_non_200():
    with patch("pipewatch.checks.requests.get", return_value=_mock_response(503)):
        result = check_http("my_pipeline", "http://example.com/health")
    assert result.status == "critical"
    assert "503" in result.message


def test_check_http_timeout():
    import requests as req
    with patch("pipewatch.checks.requests.get", side_effect=req.exceptions.Timeout):
        result = check_http("my_pipeline", "http://example.com/health")
    assert result.status == "critical"
    assert "timed out" in result.message


def test_check_http_connection_error():
    import requests as req
    with patch("pipewatch.checks.requests.get", side_effect=req.exceptions.ConnectionError("refused")):
        result = check_http("my_pipeline", "http://example.com/health")
    assert result.status == "critical"


# --- check_freshness ---

def test_check_freshness_ok():
    recent = datetime.now(timezone.utc) - timedelta(seconds=30)
    result = check_freshness("etl", recent, max_age_seconds=60)
    assert result.status == "ok"


def test_check_freshness_stale():
    old = datetime.now(timezone.utc) - timedelta(seconds=120)
    result = check_freshness("etl", old, max_age_seconds=60)
    assert result.status == "critical"
    assert "120" in result.message or "121" in result.message


def test_check_freshness_no_last_run():
    result = check_freshness("etl", None, max_age_seconds=60)
    assert result.status == "critical"


def test_check_result_is_healthy():
    r = CheckResult("p", "ok", "all good")
    assert r.is_healthy is True
    r2 = CheckResult("p", "critical", "broken")
    assert r2.is_healthy is False
