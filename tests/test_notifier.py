"""Tests for pipewatch.notifier."""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.checks import CheckResult
from pipewatch.notifier import NotifierConfig, _filter_results, dispatch_notifications, send_webhook


def _r(pipeline: str, check: str, healthy: bool, msg: str = "") -> CheckResult:
    return CheckResult(pipeline=pipeline, check=check, passed=healthy, message=msg)


# ---------------------------------------------------------------------------
# _filter_results
# ---------------------------------------------------------------------------

def test_filter_results_only_failures():
    results = [_r("p", "http", True), _r("p", "fresh", False)]
    filtered = _filter_results(results, only_failures=True)
    assert len(filtered) == 1
    assert not filtered[0].is_healthy()


def test_filter_results_all():
    results = [_r("p", "http", True), _r("p", "fresh", False)]
    filtered = _filter_results(results, only_failures=False)
    assert len(filtered) == 2


# ---------------------------------------------------------------------------
# send_webhook
# ---------------------------------------------------------------------------

class _OKHandler(BaseHTTPRequestHandler):
    received: list = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        _OKHandler.received.append(json.loads(body))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_):  # silence test output
        pass


def test_send_webhook_success():
    server = HTTPServer(("127.0.0.1", 0), _OKHandler)
    port = server.server_address[1]
    t = Thread(target=server.handle_request, daemon=True)
    t.start()

    results = [_r("pipe", "http", False, "timeout")]
    ok = send_webhook(f"http://127.0.0.1:{port}", results)
    t.join(timeout=2)
    server.server_close()

    assert ok is True
    assert _OKHandler.received[0][0]["pipeline"] == "pipe"


def test_send_webhook_bad_url_returns_false():
    ok = send_webhook("http://127.0.0.1:1", [_r("p", "c", False)])
    assert ok is False


# ---------------------------------------------------------------------------
# dispatch_notifications
# ---------------------------------------------------------------------------

def test_dispatch_stdout_only(capsys):
    cfg = NotifierConfig(stdout=True, only_failures=True)
    results = [_r("pipe", "http", False, "err"), _r("pipe", "fresh", True)]
    outcomes = dispatch_notifications(results, cfg)
    captured = capsys.readouterr().out
    assert "FAIL" in captured
    assert "OK" not in captured   # healthy filtered out
    assert outcomes["stdout"] is True


def test_dispatch_no_stdout_suppresses_output(capsys):
    cfg = NotifierConfig(stdout=False, only_failures=False)
    dispatch_notifications([_r("p", "c", True)], cfg)
    assert capsys.readouterr().out == ""


def test_dispatch_webhook_called_on_failure():
    cfg = NotifierConfig(stdout=False, webhook_url="http://example.com/hook", only_failures=True)
    results = [_r("p", "c", False, "bad")]
    with patch("pipewatch.notifier.send_webhook", return_value=True) as mock_wh:
        outcomes = dispatch_notifications(results, cfg)
    mock_wh.assert_called_once()
    assert outcomes["webhook"] is True


def test_dispatch_webhook_skipped_when_no_failures():
    cfg = NotifierConfig(stdout=False, webhook_url="http://example.com/hook", only_failures=True)
    results = [_r("p", "c", True)]
    with patch("pipewatch.notifier.send_webhook") as mock_wh:
        dispatch_notifications(results, cfg)
    mock_wh.assert_not_called()
