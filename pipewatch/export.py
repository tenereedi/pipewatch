"""Export pipeline check results to JSON or CSV formats."""

import csv
import json
import io
from typing import List, Literal
from pipewatch.checks import CheckResult


ExportFormat = Literal["json", "csv"]


def _result_to_dict(result: CheckResult) -> dict:
    return {
        "pipeline": result.pipeline,
        "check": result.check,
        "healthy": result.is_healthy(),
        "message": result.message,
        "latency_ms": result.latency_ms,
    }


def export_json(results: List[CheckResult], indent: int = 2) -> str:
    """Serialize results to a JSON string."""
    return json.dumps([_result_to_dict(r) for r in results], indent=indent)


def export_csv(results: List[CheckResult]) -> str:
    """Serialize results to a CSV string."""
    fieldnames = ["pipeline", "check", "healthy", "message", "latency_ms"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(_result_to_dict(result))
    return buf.getvalue()


def export_results(results: List[CheckResult], fmt: ExportFormat) -> str:
    """Export results in the specified format."""
    if fmt == "json":
        return export_json(results)
    elif fmt == "csv":
        return export_csv(results)
    else:
        raise ValueError(f"Unsupported export format: {fmt}")


def save_export(results: List[CheckResult], fmt: ExportFormat, path: str) -> None:
    """Write exported results to a file."""
    content = export_results(results, fmt)
    with open(path, "w", newline="") as f:
        f.write(content)
