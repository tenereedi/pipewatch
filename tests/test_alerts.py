"""Tests for pipewatch.alerts module."""
from unittest.mock import MagicMock, patch
import pytest

from pipewatch.alerts import AlertConfig, dispatch_alerts, send_email_alert, _build_email_body
from pipewatch.checks import CheckResult


def _make_result(pipeline: str, healthy: bool) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        check_type="http",
        healthy=healthy,
        message="ok" if healthy else "failed",
    )


def test_build_email_body():
    failures = [_make_result("pipe-a", False), _make_result("pipe-b", False)]
    body = _build_email_body(failures)
    assert "pipe-a" in body
    assert "pipe-b" in body
    assert "failed" in body


def test_send_email_alert_missing_config_returns_false():
    failures = [_make_result("pipe-a", False)]
    config = AlertConfig()  # no email_to / email_from
    result = send_email_alert(failures, config)
    assert result is False


@patch("pipewatch.alerts.smtplib.SMTP")
def test_send_email_alert_success(mock_smtp_cls):
    mock_server = MagicMock()
    mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
    mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

    failures = [_make_result("pipe-a", False)]
    config = AlertConfig(email_to=["ops@example.com"], email_from="pipewatch@example.com")
    result = send_email_alert(failures, config)
    assert result is True
    mock_server.sendmail.assert_called_once()


@patch("pipewatch.alerts.smtplib.SMTP")
def test_send_email_alert_smtp_error_returns_false(mock_smtp_cls):
    mock_smtp_cls.side_effect = OSError("connection refused")
    failures = [_make_result("pipe-a", False)]
    config = AlertConfig(email_to=["ops@example.com"], email_from="pipewatch@example.com")
    result = send_email_alert(failures, config)
    assert result is False


@patch("pipewatch.alerts.send_email_alert") 
def test_dispatch_alerts_no_failures(mock_send):
    results = [_make_result("pipe-a", True), _make_result("pipe-b", True)]
    dispatch_alerts(results, AlertConfig())
    mock_send.assert_not_called()


@patch("pipewatch.alerts.send_email_alert")
def test_dispatch_alerts_with_failures(mock_send):
    results = [_make_result("pipe-a", True), _make_result("pipe-b", False)]
    config = AlertConfig(email_to=["ops@example.com"], email_from="pw@example.com")
    dispatch_alerts(results, config)
    mock_send.assert_called_once()
    failures_arg = mock_send.call_args[0][0]
    assert len(failures_arg) == 1
    assert failures_arg[0].pipeline == "pipe-b"
