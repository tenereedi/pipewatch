"""Tests for pipewatch.suppression."""
import json
import os
import pytest

from pipewatch.checks import CheckResult
from pipewatch.suppression import SuppressionRule, SuppressionConfig


def _r(pipeline: str, healthy: bool = True, tags=None) -> CheckResult:
    r = CheckResult(pipeline=pipeline, check="http", healthy=healthy, message="ok")
    r.tags = tags or []
    return r


# ---------------------------------------------------------------------------
# SuppressionRule.matches
# ---------------------------------------------------------------------------

def test_rule_matches_exact_name():
    rule = SuppressionRule(pipeline_pattern="payments")
    assert rule.matches(_r("payments")) is True
    assert rule.matches(_r("orders")) is False


def test_rule_matches_glob():
    rule = SuppressionRule(pipeline_pattern="data-*")
    assert rule.matches(_r("data-ingestion")) is True
    assert rule.matches(_r("data-export")) is True
    assert rule.matches(_r("reporting")) is False


def test_rule_with_tags_requires_tag_match():
    rule = SuppressionRule(pipeline_pattern="*", tags=["nightly"])
    assert rule.matches(_r("any", tags=["nightly"])) is True
    assert rule.matches(_r("any", tags=["realtime"])) is False
    assert rule.matches(_r("any", tags=[])) is False


def test_rule_name_must_match_even_with_tags():
    rule = SuppressionRule(pipeline_pattern="payments", tags=["nightly"])
    assert rule.matches(_r("orders", tags=["nightly"])) is False


# ---------------------------------------------------------------------------
# SuppressionConfig.is_suppressed / filter
# ---------------------------------------------------------------------------

def test_empty_config_suppresses_nothing():
    cfg = SuppressionConfig()
    results = [_r("a"), _r("b", healthy=False)]
    assert cfg.filter(results) == results


def test_filter_removes_matching_results():
    cfg = SuppressionConfig(rules=[SuppressionRule(pipeline_pattern="payments")])
    results = [_r("payments"), _r("orders")]
    filtered = cfg.filter(results)
    assert len(filtered) == 1
    assert filtered[0].pipeline == "orders"


def test_suppressed_reason_returns_none_when_no_match():
    cfg = SuppressionConfig(rules=[SuppressionRule(pipeline_pattern="payments", reason="planned maintenance")])
    assert cfg.suppressed_reason(_r("orders")) is None


def test_suppressed_reason_returns_reason_string():
    cfg = SuppressionConfig(rules=[SuppressionRule(pipeline_pattern="payments", reason="planned maintenance")])
    assert cfg.suppressed_reason(_r("payments")) == "planned maintenance"


def test_suppressed_reason_fallback_when_empty_reason():
    cfg = SuppressionConfig(rules=[SuppressionRule(pipeline_pattern="*", reason="")])
    assert cfg.suppressed_reason(_r("anything")) == "(no reason given)"


# ---------------------------------------------------------------------------
# SuppressionConfig.from_file
# ---------------------------------------------------------------------------

def test_from_file_missing_path_returns_empty(tmp_path):
    cfg = SuppressionConfig.from_file(str(tmp_path / "nonexistent.json"))
    assert cfg.rules == []


def test_from_file_loads_rules(tmp_path):
    data = {
        "rules": [
            {"pipeline_pattern": "etl-*", "tags": [], "reason": "ETL window"},
            {"pipeline_pattern": "*", "tags": ["debug"]},
        ]
    }
    p = tmp_path / "suppress.json"
    p.write_text(json.dumps(data))
    cfg = SuppressionConfig.from_file(str(p))
    assert len(cfg.rules) == 2
    assert cfg.rules[0].pipeline_pattern == "etl-*"
    assert cfg.rules[0].reason == "ETL window"
    assert cfg.rules[1].tags == ["debug"]
