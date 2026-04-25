"""Audit log: record and retrieve CLI actions performed by the user."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

_DEFAULT_DB = Path(".pipewatch_audit.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_audit_db(db_path: Path = _DEFAULT_DB) -> None:
    """Create the audit_log table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL    NOT NULL,
                command   TEXT    NOT NULL,
                detail    TEXT
            )
            """
        )


@dataclass
class AuditEntry:
    command: str
    detail: Optional[str]
    timestamp: float

    def __str__(self) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        suffix = f" — {self.detail}" if self.detail else ""
        return f"[{ts}] {self.command}{suffix}"


def record_action(
    command: str,
    detail: Optional[str] = None,
    db_path: Path = _DEFAULT_DB,
) -> AuditEntry:
    """Persist an audit entry and return it."""
    init_audit_db(db_path)
    ts = time.time()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO audit_log (timestamp, command, detail) VALUES (?, ?, ?)",
            (ts, command, detail),
        )
    return AuditEntry(command=command, detail=detail, timestamp=ts)


def load_audit_log(
    db_path: Path = _DEFAULT_DB,
    limit: int = 50,
    command_filter: Optional[str] = None,
) -> List[AuditEntry]:
    """Return recent audit entries, newest first."""
    init_audit_db(db_path)
    query = "SELECT timestamp, command, detail FROM audit_log"
    params: list = []
    if command_filter:
        query += " WHERE command = ?"
        params.append(command_filter)
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    with _connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [AuditEntry(command=r["command"], detail=r["detail"], timestamp=r["timestamp"]) for r in rows]


def clear_audit_log(db_path: Path = _DEFAULT_DB) -> int:
    """Delete all audit entries and return the count removed."""
    init_audit_db(db_path)
    with _connect(db_path) as conn:
        cur = conn.execute("DELETE FROM audit_log")
        return cur.rowcount
