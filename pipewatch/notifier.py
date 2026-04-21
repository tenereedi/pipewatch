"""Notification routing: send alerts to configured channels (email, webhook, stdout)."""
from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checks import CheckResult

logger = logging.getLogger(__name__)


@dataclass
class NotifierConfig:
    email: Optional[dict] = None          # passed straight to alerts.send_email_alert
    webhook_url: Optional[str] = None     # HTTP POST endpoint
    stdout: bool = True                   # always-on fallback
    only_failures: bool = True            # skip healthy results


def _filter_results(results: List[CheckResult], only_failures: bool) -> List[CheckResult]:
    if only_failures:
        return [r for r in results if not r.is_healthy()]
    return list(results)


def send_webhook(url: str, results: List[CheckResult]) -> bool:
    """POST a JSON payload to *url*. Returns True on success."""
    payload = [
        {
            "pipeline": r.pipeline,
            "check": r.check,
            "healthy": r.is_healthy(),
            "message": r.message,
        }
        for r in results
    ]
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status < 400
    except Exception as exc:  # noqa: BLE001
        logger.warning("Webhook delivery failed: %s", exc)
        return False


def dispatch_notifications(results: List[CheckResult], cfg: NotifierConfig) -> dict:
    """Route *results* to all configured channels.

    Returns a dict mapping channel name -> bool success.
    """
    targets = _filter_results(results, cfg.only_failures)
    outcomes: dict = {}

    if cfg.stdout:
        for r in targets:
            status = "OK" if r.is_healthy() else "FAIL"
            print(f"[{status}] {r.pipeline}/{r.check}: {r.message}")
        outcomes["stdout"] = True

    if cfg.webhook_url and targets:
        outcomes["webhook"] = send_webhook(cfg.webhook_url, targets)

    if cfg.email and targets:
        from pipewatch.alerts import dispatch_alerts  # avoid circular at module level
        dispatch_alerts(targets, cfg.email)
        outcomes["email"] = True

    return outcomes
