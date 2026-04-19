"""Persist and retrieve pipeline check history using a local SQLite database."""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pipewatch.checks import CheckResult

DEFAULT_DB_PATH = Path.home() / ".pipewatch" / "history.db"


def _connect(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Create the results table if it doesn't exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS check_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline TEXT NOT NULL,
                check_type TEXT NOT NULL,
                healthy INTEGER NOT NULL,
                message TEXT,
                details TEXT,
                timestamp TEXT NOT NULL
            )
            """
        )


def save_results(results: List[CheckResult], db_path: Path = DEFAULT_DB_PATH) -> None:
    """Persist a list of CheckResult objects to the database."""
    init_db(db_path)
    rows = [
        (
            r.pipeline,
            r.check_type,
            int(r.healthy),
            r.message,
            json.dumps(r.details) if r.details else None,
            datetime.utcnow().isoformat(),
        )
        for r in results
    ]
    with _connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO check_results (pipeline, check_type, healthy, message, details, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )


def load_recent(pipeline: Optional[str] = None, limit: int = 50, db_path: Path = DEFAULT_DB_PATH) -> List[dict]:
    """Return recent check results, optionally filtered by pipeline name."""
    init_db(db_path)
    with _connect(db_path) as conn:
        if pipeline:
            rows = conn.execute(
                "SELECT * FROM check_results WHERE pipeline = ? ORDER BY id DESC LIMIT ?",
                (pipeline, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM check_results ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
    return [dict(r) for r in rows]


def clear_history(db_path: Path = DEFAULT_DB_PATH) -> int:
    """Delete all records and return the number of rows removed."""
    init_db(db_path)
    with _connect(db_path) as conn:
        cursor = conn.execute("DELETE FROM check_results")
        return cursor.rowcount
