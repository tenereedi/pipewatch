"""Pipeline health check implementations."""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
import requests


@dataclass
class CheckResult:
    pipeline_name: str
    status: str  # "ok", "warning", "critical"
    message: str
    checked_at: datetime = None

    def __post_init__(self):
        if self.checked_at is None:
            self.checked_at = datetime.now(timezone.utc)

    @property
    def is_healthy(self) -> bool:
        return self.status == "ok"


def check_http(pipeline_name: str, url: str, timeout: int = 10) -> CheckResult:
    """Check pipeline health via HTTP endpoint."""
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return CheckResult(pipeline_name, "ok", f"HTTP {resp.status_code}")
        return CheckResult(pipeline_name, "critical", f"HTTP {resp.status_code}")
    except requests.exceptions.Timeout:
        return CheckResult(pipeline_name, "critical", "Request timed out")
    except requests.exceptions.ConnectionError as e:
        return CheckResult(pipeline_name, "critical", f"Connection error: {e}")


def check_freshness(
    pipeline_name: str,
    last_run_at: Optional[datetime],
    max_age_seconds: int,
) -> CheckResult:
    """Check that pipeline ran recently enough."""
    if last_run_at is None:
        return CheckResult(pipeline_name, "critical", "No last run time available")

    now = datetime.now(timezone.utc)
    if last_run_at.tzinfo is None:
        last_run_at = last_run_at.replace(tzinfo=timezone.utc)

    age = (now - last_run_at).total_seconds()
    if age <= max_age_seconds:
        return CheckResult(pipeline_name, "ok", f"Last run {int(age)}s ago")
    return CheckResult(
        pipeline_name,
        "critical",
        f"Last run {int(age)}s ago, max allowed {max_age_seconds}s",
    )


def run_checks(pipeline) -> list[CheckResult]:
    """Run all configured checks for a pipeline."""
    results = []
    if pipeline.health_url:
        results.append(check_http(pipeline.name, pipeline.health_url))
    if pipeline.max_age_seconds is not None and pipeline.last_run_at is not None:
        results.append(
            check_freshness(pipeline.name, pipeline.last_run_at, pipeline.max_age_seconds)
        )
    return results
