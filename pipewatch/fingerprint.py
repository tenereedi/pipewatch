"""Failure fingerprinting: hash failure signatures to group recurring issues."""

from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

from pipewatch.checks import CheckResult


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_fingerprint_db(db_path: str) -> None:
    """Create the fingerprints table if it doesn't exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fingerprints (
                fingerprint TEXT NOT NULL,
                pipeline    TEXT NOT NULL,
                message     TEXT NOT NULL,
                first_seen  REAL NOT NULL,
                last_seen   REAL NOT NULL,
                occurrences INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (fingerprint)
            )
            """
        )


def _make_fingerprint(result: CheckResult) -> str:
    """Produce a stable hash from pipeline name + failure message."""
    raw = f"{result.pipeline}:{result.message or ''}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


@dataclass
class FingerprintRecord:
    fingerprint: str
    pipeline: str
    message: str
    first_seen: float
    last_seen: float
    occurrences: int

    def __str__(self) -> str:
        return (
            f"[{self.fingerprint}] {self.pipeline} | "
            f"occurrences={self.occurrences} | "
            f"msg={self.message!r}"
        )


def record_fingerprint(result: CheckResult, db_path: str) -> FingerprintRecord:
    """Upsert a fingerprint record for a failing CheckResult."""
    fp = _make_fingerprint(result)
    now = time.time()
    with _connect(db_path) as conn:
        existing = conn.execute(
            "SELECT * FROM fingerprints WHERE fingerprint = ?", (fp,)
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE fingerprints
                SET last_seen = ?, occurrences = occurrences + 1
                WHERE fingerprint = ?
                """,
                (now, fp),
            )
            row = conn.execute(
                "SELECT * FROM fingerprints WHERE fingerprint = ?", (fp,)
            ).fetchone()
        else:
            conn.execute(
                """
                INSERT INTO fingerprints (fingerprint, pipeline, message, first_seen, last_seen, occurrences)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (fp, result.pipeline, result.message or "", now, now),
            )
            row = conn.execute(
                "SELECT * FROM fingerprints WHERE fingerprint = ?", (fp,)
            ).fetchone()
    return FingerprintRecord(**dict(row))


def load_fingerprints(
    db_path: str, pipeline: Optional[str] = None
) -> list[FingerprintRecord]:
    """Return fingerprint records, optionally filtered by pipeline."""
    with _connect(db_path) as conn:
        if pipeline:
            rows = conn.execute(
                "SELECT * FROM fingerprints WHERE pipeline = ? ORDER BY occurrences DESC",
                (pipeline,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM fingerprints ORDER BY occurrences DESC"
            ).fetchall()
    return [FingerprintRecord(**dict(r)) for r in rows]
