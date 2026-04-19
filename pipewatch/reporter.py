"""Format and print check results to the terminal."""
from pipewatch.checks import CheckResult

STATUS_ICONS = {
    "ok": "\u2705",
    "warning": "\u26a0\ufe0f",
    "critical": "\u274c",
}

STATUS_COLORS = {
    "ok": "\033[92m",
    "warning": "\033[93m",
    "critical": "\033[91m",
    "reset": "\033[0m",
}


def _colorize(status: str, text: str, use_color: bool = True) -> str:
    if not use_color:
        return text
    color = STATUS_COLORS.get(status, "")
    reset = STATUS_COLORS["reset"]
    return f"{color}{text}{reset}"


def format_result(result: CheckResult, use_color: bool = True) -> str:
    icon = STATUS_ICONS.get(result.status, "?")
    status_str = _colorize(result.status, result.status.upper(), use_color)
    ts = result.checked_at.strftime("%H:%M:%S")
    return f"[{ts}] {icon} {result.pipeline_name}: {status_str} — {result.message}"


def print_results(results: list[CheckResult], use_color: bool = True) -> None:
    if not results:
        print("No checks were run.")
        return
    for result in results:
        print(format_result(result, use_color=use_color))


def summarize(results: list[CheckResult]) -> dict:
    summary = {"ok": 0, "warning": 0, "critical": 0, "total": len(results)}
    for r in results:
        if r.status in summary:
            summary[r.status] += 1
    return summary


def print_summary(results: list[CheckResult], use_color: bool = True) -> None:
    s = summarize(results)
    total = s["total"]
    ok = s["ok"]
    warn = s["warning"]
    crit = s["critical"]
    line = f"\nSummary: {total} checks — "
    line += _colorize("ok", f"{ok} ok", use_color) + ", "
    line += _colorize("warning", f"{warn} warning", use_color) + ", "
    line += _colorize("critical", f"{crit} critical", use_color)
    print(line)
