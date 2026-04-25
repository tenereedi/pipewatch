"""Labeling module: attach and query custom key-value labels on pipeline results."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.checks import CheckResult


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_labeling_db(db_path: str) -> None:
    """Create the labels table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS labels (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline TEXT    NOT NULL,
                key      TEXT    NOT NULL,
                value    TEXT    NOT NULL,
                UNIQUE(pipeline, key)
            )
            """
        )


@dataclass
class LabelSet:
    pipeline: str
    labels: Dict[str, str] = field(default_factory=dict)

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self.labels.get(key, default)

    def __str__(self) -> str:
        if not self.labels:
            return f"{self.pipeline}: (no labels)"
        pairs = ", ".join(f"{k}={v}" for k, v in sorted(self.labels.items()))
        return f"{self.pipeline}: {pairs}"


def set_label(db_path: str, pipeline: str, key: str, value: str) -> None:
    """Insert or replace a label for a pipeline."""
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO labels (pipeline, key, value) VALUES (?, ?, ?)"
            " ON CONFLICT(pipeline, key) DO UPDATE SET value=excluded.value",
            (pipeline, key, value),
        )


def remove_label(db_path: str, pipeline: str, key: str) -> bool:
    """Remove a label; returns True if a row was deleted."""
    with _connect(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM labels WHERE pipeline=? AND key=?", (pipeline, key)
        )
        return cur.rowcount > 0


def get_labels(db_path: str, pipeline: str) -> LabelSet:
    """Return all labels for a pipeline."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT key, value FROM labels WHERE pipeline=?", (pipeline,)
        ).fetchall()
    return LabelSet(pipeline=pipeline, labels={r["key"]: r["value"] for r in rows})


def filter_by_label(
    results: List[CheckResult], db_path: str, key: str, value: str
) -> List[CheckResult]:
    """Return only results whose pipeline carries a matching label."""
    matched: List[CheckResult] = []
    for result in results:
        ls = get_labels(db_path, result.pipeline_name)
        if ls.get(key) == value:
            matched.append(result)
    return matched
