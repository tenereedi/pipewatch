"""Configuration loader for pipewatch pipelines."""

import os
from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class PipelineConfig:
    name: str
    source: str
    interval_seconds: int = 60
    thresholds: dict[str, Any] = field(default_factory=dict)
    alerts: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineConfig":
        return cls(
            name=data["name"],
            source=data["source"],
            interval_seconds=data.get("interval_seconds", 60),
            thresholds=data.get("thresholds", {}),
            alerts=data.get("alerts", []),
        )


@dataclass
class WatchConfig:
    pipelines: list[PipelineConfig] = field(default_factory=list)

    @classmethod
    def load(cls, path: str) -> "WatchConfig":
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, "r") as f:
            raw = yaml.safe_load(f)
        if not isinstance(raw, dict) or "pipelines" not in raw:
            raise ValueError("Config must contain a top-level 'pipelines' key")
        pipelines = [PipelineConfig.from_dict(p) for p in raw["pipelines"]]
        return cls(pipelines=pipelines)
