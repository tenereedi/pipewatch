"""Runbook link registry — attach remediation URLs to pipeline check results."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_DEFAULT_DB = Path("pipewatch_runbook.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_runbook_db(db_path: Path = _DEFAULT_DB) -> None:
    """Create the runbook table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runbooks (
                pipeline TEXT NOT NULL,
                title    TEXT NOT NULL,
                url      TEXT NOT NULL,
                notes    TEXT,
                PRIMARY KEY (pipeline)
            )
            """
        )


@dataclass
class RunbookEntry:
    pipeline: str
    title: str
    url: str
    notes: str = ""

    def __str__(self) -> str:
        base = f"[{self.pipeline}] {self.title} -> {self.url}"
        return f"{base}  ({self.notes})" if self.notes else base


def set_runbook(
    pipeline: str,
    title: str,
    url: str,
    notes: str = "",
    db_path: Path = _DEFAULT_DB,
) -> RunbookEntry:
    """Insert or replace a runbook entry for *pipeline*."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO runbooks (pipeline, title, url, notes)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(pipeline) DO UPDATE SET
                title  = excluded.title,
                url    = excluded.url,
                notes  = excluded.notes
            """,
            (pipeline, title, url, notes),
        )
    return RunbookEntry(pipeline=pipeline, title=title, url=url, notes=notes)


def get_runbook(pipeline: str, db_path: Path = _DEFAULT_DB) -> Optional[RunbookEntry]:
    """Return the runbook entry for *pipeline*, or None if absent."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT pipeline, title, url, notes FROM runbooks WHERE pipeline = ?",
            (pipeline,),
        ).fetchone()
    if row is None:
        return None
    return RunbookEntry(**dict(row))


def list_runbooks(db_path: Path = _DEFAULT_DB) -> list[RunbookEntry]:
    """Return all registered runbook entries."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT pipeline, title, url, notes FROM runbooks ORDER BY pipeline"
        ).fetchall()
    return [RunbookEntry(**dict(r)) for r in rows]


def delete_runbook(pipeline: str, db_path: Path = _DEFAULT_DB) -> bool:
    """Remove the runbook for *pipeline*. Returns True if a row was deleted."""
    with _connect(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM runbooks WHERE pipeline = ?", (pipeline,)
        )
    return cur.rowcount > 0
