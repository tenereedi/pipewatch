"""Circuit breaker pattern for pipeline checks.

Tracks consecutive failures per pipeline and opens the circuit (skips checks)
once a threshold is exceeded, preventing alert storms and unnecessary load.
The circuit resets after a configurable recovery window.
"""

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DEFAULT_DB = Path(".pipewatch_circuit.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_circuit_db(db_path: Path = DEFAULT_DB) -> None:
    """Create the circuit breaker state table if it doesn't exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS circuit_state (
                pipeline TEXT PRIMARY KEY,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                opened_at REAL,
                state TEXT NOT NULL DEFAULT 'closed'
            )
            """
        )
        conn.commit()


@dataclass
class CircuitBreakerPolicy:
    """Policy controlling when a circuit opens and recovers."""

    pipeline: str
    failure_threshold: int = 3   # consecutive failures before opening
    recovery_window: float = 300.0  # seconds before attempting half-open

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if self.recovery_window <= 0:
            raise ValueError("recovery_window must be positive")


@dataclass
class CircuitState:
    """Current state of a circuit breaker for one pipeline."""

    pipeline: str
    state: str          # 'closed', 'open', or 'half-open'
    consecutive_failures: int
    opened_at: Optional[float]

    def __str__(self) -> str:
        if self.state == "closed":
            return f"[CLOSED] {self.pipeline} — {self.consecutive_failures} consecutive failure(s)"
        if self.state == "open":
            age = time.time() - (self.opened_at or 0)
            return f"[OPEN]   {self.pipeline} — opened {age:.0f}s ago"
        return f"[HALF-OPEN] {self.pipeline} — testing recovery"


def get_circuit_state(
    pipeline: str, db_path: Path = DEFAULT_DB
) -> CircuitState:
    """Return the current circuit state for a pipeline (defaults to closed)."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM circuit_state WHERE pipeline = ?", (pipeline,)
        ).fetchone()
    if row is None:
        return CircuitState(pipeline=pipeline, state="closed", consecutive_failures=0, opened_at=None)
    return CircuitState(
        pipeline=row["pipeline"],
        state=row["state"],
        consecutive_failures=row["consecutive_failures"],
        opened_at=row["opened_at"],
    )


def is_circuit_open(
    policy: CircuitBreakerPolicy, db_path: Path = DEFAULT_DB
) -> bool:
    """Return True if the circuit is open (checks should be skipped).

    Transitions an open circuit to half-open once the recovery window elapses.
    """
    state = get_circuit_state(policy.pipeline, db_path)
    if state.state == "closed":
        return False
    if state.state == "open":
        elapsed = time.time() - (state.opened_at or 0)
        if elapsed >= policy.recovery_window:
            # Transition to half-open so next check is allowed through
            _set_state(policy.pipeline, "half-open", state.consecutive_failures, state.opened_at, db_path)
            return False
        return True
    # half-open: allow one probe through
    return False


def record_result(
    policy: CircuitBreakerPolicy,
    success: bool,
    db_path: Path = DEFAULT_DB,
) -> CircuitState:
    """Update circuit state based on the latest check result.

    - Success resets the counter and closes the circuit.
    - Failure increments the counter; opens the circuit at threshold.
    """
    state = get_circuit_state(policy.pipeline, db_path)

    if success:
        new_state = CircuitState(
            pipeline=policy.pipeline,
            state="closed",
            consecutive_failures=0,
            opened_at=None,
        )
    else:
        new_failures = state.consecutive_failures + 1
        if new_failures >= policy.failure_threshold and state.state != "open":
            new_state = CircuitState(
                pipeline=policy.pipeline,
                state="open",
                consecutive_failures=new_failures,
                opened_at=time.time(),
            )
        else:
            new_state = CircuitState(
                pipeline=policy.pipeline,
                state="open" if state.state == "open" else "closed",
                consecutive_failures=new_failures,
                opened_at=state.opened_at,
            )

    _set_state(
        policy.pipeline,
        new_state.state,
        new_state.consecutive_failures,
        new_state.opened_at,
        db_path,
    )
    return new_state


def reset_circuit(pipeline: str, db_path: Path = DEFAULT_DB) -> None:
    """Manually reset a pipeline's circuit to closed with zero failures."""
    _set_state(pipeline, "closed", 0, None, db_path)


def _set_state(
    pipeline: str,
    state: str,
    consecutive_failures: int,
    opened_at: Optional[float],
    db_path: Path,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO circuit_state (pipeline, consecutive_failures, opened_at, state)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(pipeline) DO UPDATE SET
                consecutive_failures = excluded.consecutive_failures,
                opened_at = excluded.opened_at,
                state = excluded.state
            """,
            (pipeline, consecutive_failures, opened_at, state),
        )
        conn.commit()
