"""Pipeline ownership registry — track which team or person owns each pipeline."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

_DEFAULT_DB = Path("pipewatch_ownership.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_ownership_db(db_path: Path = _DEFAULT_DB) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ownership (
                pipeline    TEXT NOT NULL,
                owner       TEXT NOT NULL,
                contact     TEXT,
                updated_at  REAL NOT NULL,
                PRIMARY KEY (pipeline)
            )
            """
        )
        conn.commit()


@dataclass
class OwnershipRecord:
    pipeline: str
    owner: str
    contact: Optional[str]
    updated_at: float

    def __str__(self) -> str:
        contact_str = f" <{self.contact}>" if self.contact else ""
        return f"{self.pipeline}: {self.owner}{contact_str}"


def set_owner(
    pipeline: str,
    owner: str,
    contact: Optional[str] = None,
    db_path: Path = _DEFAULT_DB,
) -> OwnershipRecord:
    now = time.time()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO ownership (pipeline, owner, contact, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(pipeline) DO UPDATE SET
                owner=excluded.owner,
                contact=excluded.contact,
                updated_at=excluded.updated_at
            """,
            (pipeline, owner, contact, now),
        )
        conn.commit()
    return OwnershipRecord(pipeline=pipeline, owner=owner, contact=contact, updated_at=now)


def get_owner(pipeline: str, db_path: Path = _DEFAULT_DB) -> Optional[OwnershipRecord]:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT pipeline, owner, contact, updated_at FROM ownership WHERE pipeline = ?",
            (pipeline,),
        ).fetchone()
    if row is None:
        return None
    return OwnershipRecord(**dict(row))


def list_owners(db_path: Path = _DEFAULT_DB) -> List[OwnershipRecord]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT pipeline, owner, contact, updated_at FROM ownership ORDER BY pipeline"
        ).fetchall()
    return [OwnershipRecord(**dict(r)) for r in rows]


def remove_owner(pipeline: str, db_path: Path = _DEFAULT_DB) -> bool:
    with _connect(db_path) as conn:
        cur = conn.execute("DELETE FROM ownership WHERE pipeline = ?", (pipeline,))
        conn.commit()
    return cur.rowcount > 0
