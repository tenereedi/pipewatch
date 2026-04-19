"""Tests for pipewatch config loader."""

import pytest
import tempfile
import os
import yaml

from pipewatch.config import WatchConfig, PipelineConfig


SAMPLE_CONFIG = {
    "pipelines": [
        {
            "name": "test_pipeline",
            "source": "postgres",
            "interval_seconds": 90,
            "thresholds": {"max_lag_seconds": 200},
            "alerts": ["slack"],
        },
        {
            "name": "minimal_pipeline",
            "source": "bigquery",
        },
    ]
}


@pytest.fixture
def config_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(SAMPLE_CONFIG, f)
        path = f.name
    yield path
    os.unlink(path)


def test_load_valid_config(config_file):
    config = WatchConfig.load(config_file)
    assert len(config.pipelines) == 2


def test_pipeline_fields(config_file):
    config = WatchConfig.load(config_file)
    p = config.pipelines[0]
    assert p.name == "test_pipeline"
    assert p.source == "postgres"
    assert p.interval_seconds == 90
    assert p.thresholds == {"max_lag_seconds": 200}
    assert p.alerts == ["slack"]


def test_pipeline_defaults(config_file):
    config = WatchConfig.load(config_file)
    p = config.pipelines[1]
    assert p.interval_seconds == 60
    assert p.thresholds == {}
    assert p.alerts == []


def test_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        WatchConfig.load("/nonexistent/path/config.yaml")


def test_invalid_config_raises():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump({"not_pipelines": []}, f)
        path = f.name
    try:
        with pytest.raises(ValueError, match="pipelines"):
            WatchConfig.load(path)
    finally:
        os.unlink(path)
