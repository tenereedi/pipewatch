"""Tests for pipewatch/export.py."""

import json
import csv
import io
import pytest
from pipewatch.checks import CheckResult
from pipewatch.export import export_json, export_csv, export_results, save_export


def _make_result(pipeline="pipe1", check="http", status=200, message="OK", latency_ms=42.0):
    return CheckResult(pipeline=pipeline, check=check, status=status, message=message, latency_ms=latency_ms)


def test_export_json_empty():
    out = export_json([])
    assert json.loads(out) == []


def test_export_json_single_result():
    r = _make_result()
    data = json.loads(export_json([r]))
    assert len(data) == 1
    assert data[0]["pipeline"] == "pipe1"
    assert data[0]["check"] == "http"
    assert data[0]["healthy"] is True
    assert data[0]["message"] == "OK"
    assert data[0]["latency_ms"] == 42.0


def test_export_json_unhealthy():
    r = _make_result(status=500, message="Server Error")
    data = json.loads(export_json([r]))
    assert data[0]["healthy"] is False


def test_export_csv_headers():
    out = export_csv([])
    reader = csv.DictReader(io.StringIO(out))
    assert set(reader.fieldnames) == {"pipeline", "check", "healthy", "message", "latency_ms"}


def test_export_csv_single_result():
    r = _make_result(pipeline="p2", check="freshness", status=200, message="fresh")
    out = export_csv([r])
    reader = csv.DictReader(io.StringIO(out))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "p2"
    assert rows[0]["check"] == "freshness"
    assert rows[0]["healthy"] == "True"


def test_export_results_json():
    r = _make_result()
    out = export_results([r], "json")
    data = json.loads(out)
    assert isinstance(data, list)


def test_export_results_csv():
    r = _make_result()
    out = export_results([r], "csv")
    assert "pipeline" in out


def test_export_results_invalid_format():
    with pytest.raises(ValueError, match="Unsupported"):
        export_results([], "xml")


def test_save_export_json(tmp_path):
    r = _make_result()
    path = str(tmp_path / "out.json")
    save_export([r], "json", path)
    with open(path) as f:
        data = json.load(f)
    assert len(data) == 1
    assert data[0]["pipeline"] == "pipe1"


def test_save_export_csv(tmp_path):
    r = _make_result()
    path = str(tmp_path / "out.csv")
    save_export([r], "csv", path)
    with open(path) as f:
        content = f.read()
    assert "pipe1" in content
