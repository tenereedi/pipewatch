"""Alert dispatching for failed pipeline checks."""
from dataclasses import dataclass
from typing import List, Optional
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from pipewatch.checks import CheckResult

logger = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    email_to: Optional[List[str]] = None
    email_from: Optional[str] = None
    smtp_host: str = "localhost"
    smtp_port: int = 25
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None


def _build_email_body(failures: List[CheckResult]) -> str:
    lines = ["The following pipeline checks failed:\n"]
    for result in failures:
        lines.append(f"  [{result.pipeline}] {result.check_type}: {result.message}")
    return "\n".join(lines)


def send_email_alert(
    failures: List[CheckResult],
    config: AlertConfig,
) -> bool:
    """Send an email alert for failed checks. Returns True on success."""
    if not config.email_to or not config.email_from:
        logger.warning("Email alert skipped: missing email_to or email_from.")
        return False

    body = _build_email_body(failures)
    msg = MIMEMultipart()
    msg["From"] = config.email_from
    msg["To"] = ", ".join(config.email_to)
    msg["Subject"] = f"[pipewatch] {len(failures)} pipeline check(s) failed"
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
            if config.smtp_user and config.smtp_password:
                server.login(config.smtp_user, config.smtp_password)
            server.sendmail(config.email_from, config.email_to, msg.as_string())
        logger.info("Alert email sent to %s", config.email_to)
        return True
    except Exception as exc:
        logger.error("Failed to send alert email: %s", exc)
        return False


def dispatch_alerts(results: List[CheckResult], config: AlertConfig) -> None:
    """Dispatch alerts for any unhealthy check results."""
    failures = [r for r in results if not r.is_healthy()]
    if not failures:
        logger.debug("No failures to alert on.")
        return
    send_email_alert(failures, config)
