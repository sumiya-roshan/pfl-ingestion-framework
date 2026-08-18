"""
Tests for the retry loop used by run_one() in src/main/main.py.

main.py is a Databricks notebook (needs dbutils + a live cluster), so it can't
be imported directly here. This test reproduces the same retry loop logic
against a fake orchestrator.run() call — if you change the retry logic in
main.py, update the copy of run_one() below to match.

Run with (no pytest needed):
    python -m unittest databricks-ingestion-framework/tests/test_retry.py -v
"""
import time
import unittest
from unittest.mock import patch

AUDIT_STATUS_SUCCESS = "SUCCESS"
AUDIT_STATUS_FAILED = "FAILED"


class FakeTask:
    def __init__(self):
        self.source_object_name = "test_table"
        self.config_id = 1


class FakeSourceSystem:
    def __init__(self, retry_count=3, retry_interval=1):
        self.retry_count = retry_count
        self.retry_interval = retry_interval


def run_one(task, source_sys, fake_run):
    """Mirrors the retry loop in src/main/main.py's run_one()."""
    max_retries = int(source_sys.retry_count or 0)
    retry_interval = int(source_sys.retry_interval or 0)

    attempt = 0
    while True:
        result = fake_run()
        if result["status"] == AUDIT_STATUS_SUCCESS or attempt >= max_retries:
            return result
        attempt += 1
        print(f"Task {task.source_object_name} failed (attempt {attempt}/{max_retries}), "
              f"retrying in {retry_interval}s: {result.get('error')}")
        if retry_interval > 0:
            time.sleep(retry_interval)


def make_fake_run(fail_times):
    """Returns a callable that fails `fail_times` times, then succeeds."""
    calls = {"n": 0}
    def fake_run():
        calls["n"] += 1
        if calls["n"] <= fail_times:
            return {"status": AUDIT_STATUS_FAILED, "error": f"boom #{calls['n']}"}
        return {"status": AUDIT_STATUS_SUCCESS}
    return fake_run, calls


@patch("time.sleep", return_value=None)  # skip real sleeping during tests
class TestRetryLoop(unittest.TestCase):

    def test_succeeds_on_first_try_no_retries_needed(self, mock_sleep):
        task = FakeTask()
        source_sys = FakeSourceSystem(retry_count=3, retry_interval=5)
        fake_run, calls = make_fake_run(fail_times=0)

        result = run_one(task, source_sys, fake_run)

        self.assertEqual(result["status"], AUDIT_STATUS_SUCCESS)
        self.assertEqual(calls["n"], 1)
        mock_sleep.assert_not_called()

    def test_fails_then_succeeds_within_retry_budget(self, mock_sleep):
        task = FakeTask()
        source_sys = FakeSourceSystem(retry_count=3, retry_interval=5)
        fake_run, calls = make_fake_run(fail_times=2)

        result = run_one(task, source_sys, fake_run)

        self.assertEqual(result["status"], AUDIT_STATUS_SUCCESS)
        self.assertEqual(calls["n"], 3)          # 2 failures + 1 success
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_called_with(5)

    def test_exhausts_retries_and_returns_last_failure(self, mock_sleep):
        task = FakeTask()
        source_sys = FakeSourceSystem(retry_count=1, retry_interval=30)
        fake_run, calls = make_fake_run(fail_times=999)  # always fails

        result = run_one(task, source_sys, fake_run)

        self.assertEqual(result["status"], AUDIT_STATUS_FAILED)
        self.assertEqual(calls["n"], 2)          # 1 initial + 1 retry
        self.assertEqual(mock_sleep.call_count, 1)
        mock_sleep.assert_called_with(30)

    def test_retry_count_zero_means_no_retries(self, mock_sleep):
        task = FakeTask()
        source_sys = FakeSourceSystem(retry_count=0, retry_interval=5)
        fake_run, calls = make_fake_run(fail_times=999)

        result = run_one(task, source_sys, fake_run)

        self.assertEqual(result["status"], AUDIT_STATUS_FAILED)
        self.assertEqual(calls["n"], 1)
        mock_sleep.assert_not_called()

    def test_retry_interval_zero_does_not_sleep(self, mock_sleep):
        task = FakeTask()
        source_sys = FakeSourceSystem(retry_count=2, retry_interval=0)
        fake_run, calls = make_fake_run(fail_times=1)

        result = run_one(task, source_sys, fake_run)

        self.assertEqual(result["status"], AUDIT_STATUS_SUCCESS)
        mock_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
