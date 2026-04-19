"""Simple interval-based scheduler for running pipeline checks periodically."""

import time
import logging
from datetime import datetime, timezone
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class SchedulerStop(Exception):
    """Raised to signal the scheduler should stop."""


class PipelineScheduler:
    """Runs a check function on a fixed interval."""

    def __init__(
        self,
        interval_seconds: int,
        check_fn: Callable,
        max_runs: Optional[int] = None,
    ):
        if interval_seconds < 1:
            raise ValueError("interval_seconds must be >= 1")
        self.interval_seconds = interval_seconds
        self.check_fn = check_fn
        self.max_runs = max_runs
        self._run_count = 0
        self._running = False

    @property
    def run_count(self) -> int:
        return self._run_count

    def start(self) -> None:
        """Start the scheduler loop."""
        self._running = True
        logger.info(
            "Scheduler started (interval=%ds, max_runs=%s)",
            self.interval_seconds,
            self.max_runs,
        )
        try:
            while self._running:
                tick = datetime.now(timezone.utc)
                logger.debug("Running checks at %s", tick.isoformat())
                try:
                    self.check_fn()
                except Exception as exc:  # noqa: BLE001
                    logger.error("check_fn raised an error: %s", exc)
                self._run_count += 1
                if self.max_runs is not None and self._run_count >= self.max_runs:
                    logger.info("Reached max_runs=%d, stopping.", self.max_runs)
                    break
                time.sleep(self.interval_seconds)
        except SchedulerStop:
            logger.info("Scheduler stopped via SchedulerStop.")
        finally:
            self._running = False

    def stop(self) -> None:
        """Signal the scheduler to stop after the current run."""
        self._running = False
