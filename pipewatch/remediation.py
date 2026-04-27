"""Remediation hints: map pipeline check failures to suggested fix actions."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

_DB_DEFAULT = Path(".pipewatch_remediation.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_remediation_db(db_path: Path = _DB_DEFAULT) -> None:
    """Create the remediation hints table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS remediation_hints (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline    TEXT NOT NULL,
                check_type  TEXT NOT NULL,
                hint        TEXT NOT NULL,
                added_at    REAL NOT NULL
            )
            """
        )
        conn.commit()


@dataclass
class RemediationHint:
    pipeline: str
    check_type: str
    hint: str
    added_at: float

    def __str__(self) -> str:
        return f"[{self.pipeline}/{self.check_type}] {self.hint}"


def set_hint(
    pipeline: str,
    check_type: str,
    hint: str,
    db_path: Path = _DB_DEFAULT,
) -> RemediationHint:
    """Insert or replace a remediation hint for a pipeline+check_type pair."""
    import time

    now = time.time()
    with _connect(db_path) as conn:
        conn.execute(
            "DELETE FROM remediation_hints WHERE pipeline=? AND check_type=?",
            (pipeline, check_type),
        )
        conn.execute(
            "INSERT INTO remediation_hints (pipeline, check_type, hint, added_at) VALUES (?,?,?,?)",
            (pipeline, check_type, hint, now),
        )
        conn.commit()
    return RemediationHint(pipeline=pipeline, check_type=check_type, hint=hint, added_at=now)


def get_hint(
    pipeline: str,
    check_type: str,
    db_path: Path = _DB_DEFAULT,
) -> Optional[RemediationHint]:
    """Return the remediation hint for a pipeline+check_type, or None."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM remediation_hints WHERE pipeline=? AND check_type=?",
            (pipeline, check_type),
        ).fetchone()
    if row is None:
        return None
    return RemediationHint(
        pipeline=row["pipeline"],
        check_type=row["check_type"],
        hint=row["hint"],
        added_at=row["added_at"],
    )


def list_hints(
    pipeline: Optional[str] = None,
    db_path: Path = _DB_DEFAULT,
) -> List[RemediationHint]:
    """Return all remediation hints, optionally filtered by pipeline."""
    with _connect(db_path) as conn:
        if pipeline:
            rows = conn.execute(
                "SELECT * FROM remediation_hints WHERE pipeline=? ORDER BY added_at DESC",
                (pipeline,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM remediation_hints ORDER BY pipeline, check_type"
            ).fetchall()
    return [
        RemediationHint(
            pipeline=r["pipeline"],
            check_type=r["check_type"],
            hint=r["hint"],
            added_at=r["added_at"],
        )
        for r in rows
    ]
