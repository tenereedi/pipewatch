"""Tests for pipewatch.scheduler."""

import pytest
from unittest.mock import MagicMock, patch
from pipewatch.scheduler import PipelineScheduler


def test_invalid_interval_raises():
    with pytest.raises(ValueError, match="interval_seconds must be >= 1"):
        PipelineScheduler(interval_seconds=0, check_fn=lambda: None)


def test_runs_exactly_max_runs():
    mock_fn = MagicMock()
    scheduler = PipelineScheduler(interval_seconds=1, check_fn=mock_fn, max_runs=3)
    with patch("time.sleep"):
        scheduler.start()
    assert mock_fn.call_count == 3
    assert scheduler.run_count == 3


def test_check_fn_exception_does_not_crash_scheduler():
    def bad_fn():
        raise RuntimeError("boom")

    scheduler = PipelineScheduler(interval_seconds=1, check_fn=bad_fn, max_runs=2)
    with patch("time.sleep"):
        scheduler.start()  # should not raise
    assert scheduler.run_count == 2


def test_stop_prevents_further_runs():
    call_count = 0

    def fn():
        nonlocal call_count
        call_count += 1
        scheduler.stop()

    scheduler = PipelineScheduler(interval_seconds=1, check_fn=fn, max_runs=10)
    with patch("time.sleep"):
        scheduler.start()
    assert call_count == 1


def test_run_count_starts_at_zero():
    scheduler = PipelineScheduler(interval_seconds=5, check_fn=lambda: None)
    assert scheduler.run_count == 0


def test_running_flag_false_after_completion():
    scheduler = PipelineScheduler(interval_seconds=1, check_fn=lambda: None, max_runs=1)
    with patch("time.sleep"):
        scheduler.start()
    assert not scheduler._running
