"""
Standalone test for the batch-level orchestration logic in
src/main/main.py (the ThreadPoolExecutor fan-out).

Re-implements ONLY the orchestration slice against dummy in-memory tasks so
it runs without Spark / Databricks:
    - group tasks by batch_id
    - tables inside a batch ordered by priority ascending
    - max_workers = distinct batch_id count
    - one thread per batch, tables run sequentially inside it

Run:
    python tests/test_batch_orchestration.py
    python -m pytest tests/test_batch_orchestration.py -v
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

AUDIT_STATUS_SUCCESS = "SUCCESS"
AUDIT_STATUS_FAILED = "FAILED"


@dataclass
class DummyTask:
    config_id: int
    source_object_name: str
    batch_id: Optional[int]
    priority: int
    fail: bool = False
    sleep: float = 0.05


# ───────────────── orchestration slice copied from main.py ────────────────
def orchestrate(tasks, run_one):
    """Mirror of the execution block in src/main/main.py."""
    results = []

    batches = {}
    for task in sorted(tasks, key=lambda t: t.priority):
        batches.setdefault(task.batch_id, []).append(task)

    max_workers = len(batches)   # distinct batch_id count

    def run_batch(batch_id, batch_tasks):
        batch_results = []
        for task in batch_tasks:
            try:
                batch_results.append(run_one(task))
            except Exception as exc:
                batch_results.append({
                    "config_id": task.config_id,
                    "run_id": None,
                    "status": AUDIT_STATUS_FAILED,
                    "rows_read": 0,
                    "error": str(exc),
                })
        return batch_results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(run_batch, bid, bt): bid
            for bid, bt in batches.items()
        }
        for future in as_completed(future_to_batch):
            bid = future_to_batch[future]
            try:
                results.extend(future.result())
            except Exception as exc:
                for task in batches[bid]:
                    results.append({
                        "config_id": task.config_id,
                        "run_id": None,
                        "status": AUDIT_STATUS_FAILED,
                        "rows_read": 0,
                        "error": str(exc),
                    })

    return results, max_workers, batches


# ─────────────────────────────── dummy data ──────────────────────────────
def sample_tasks():
    #  Batch  Priority  Table
    spec = [
        (1, 1, "A"), (1, 2, "B"), (1, 3, "C"),
        (2, 1, "D"), (2, 2, "E"),
        (3, 1, "F"), (3, 2, "G"),
        (4, 1, "H"), (4, 2, "I"),
    ]
    return [
        DummyTask(config_id=i, source_object_name=name, batch_id=b, priority=p)
        for i, (b, p, name) in enumerate(spec, start=1)
    ]


class Recorder:
    """Thread-safe recorder of start/end events per table."""

    def __init__(self):
        self._lock = threading.Lock()
        self.events = []          # (table, "start"/"end", thread_name, ts)
        self.order_by_batch = {}  # batch_id -> [table, ...] in completion order

    def run_one(self, task):
        tname = threading.current_thread().name
        with self._lock:
            self.events.append((task.source_object_name, "start", tname, time.monotonic()))
        time.sleep(task.sleep)
        with self._lock:
            self.events.append((task.source_object_name, "end", tname, time.monotonic()))
            self.order_by_batch.setdefault(task.batch_id, []).append(task.source_object_name)
        if task.fail:
            raise RuntimeError(f"boom on {task.source_object_name}")
        return {
            "config_id": task.config_id,
            "run_id": f"run-{task.config_id}",
            "status": AUDIT_STATUS_SUCCESS,
            "rows_read": 100,
        }


# ──────────────────────────────── tests ──────────────────────────────────
def test_max_workers_equals_distinct_batch_count():
    _, max_workers, batches = orchestrate(sample_tasks(), Recorder().run_one)
    assert max_workers == 4
    assert sorted(batches.keys()) == [1, 2, 3, 4]


def test_tables_within_batch_run_sequentially_in_priority_order():
    rec = Recorder()
    orchestrate(sample_tasks(), rec.run_one)

    assert rec.order_by_batch[1] == ["A", "B", "C"]
    assert rec.order_by_batch[2] == ["D", "E"]
    assert rec.order_by_batch[3] == ["F", "G"]
    assert rec.order_by_batch[4] == ["H", "I"]

    # no time overlap between two tables of the SAME batch
    span = {}
    for name, kind, _t, ts in rec.events:
        span.setdefault(name, {})[kind] = ts
    for batch_id in (1, 2, 3, 4):
        ordered = rec.order_by_batch[batch_id]
        for earlier, later in zip(ordered, ordered[1:]):
            assert span[earlier]["end"] <= span[later]["start"] + 1e-6


def test_batches_run_in_parallel():
    rec = Recorder()
    tasks = sample_tasks()
    for t in tasks:
        t.sleep = 0.2

    start = time.monotonic()
    results, _, _ = orchestrate(tasks, rec.run_one)
    elapsed = time.monotonic() - start

    # serial would be 9 * 0.2 = 1.8s; batch 1 (3 tables) dominates at ~0.6s
    assert elapsed < 1.2, f"batches did not run in parallel (elapsed={elapsed:.2f}s)"
    assert len(results) == 9

    # each batch ran on exactly one thread; 4 threads total
    threads_by_batch = {}
    for name, _kind, tname, _ in rec.events:
        batch = next(t.batch_id for t in tasks if t.source_object_name == name)
        threads_by_batch.setdefault(batch, set()).add(tname)
    for batch, threads in threads_by_batch.items():
        assert len(threads) == 1, f"batch {batch} used multiple threads: {threads}"
    assert len({tn for s in threads_by_batch.values() for tn in s}) == 4


def test_failure_in_one_batch_does_not_stop_others():
    rec = Recorder()
    tasks = sample_tasks()
    for t in tasks:
        if t.source_object_name == "D":
            t.fail = True

    results, _, _ = orchestrate(tasks, rec.run_one)
    by_cfg = {r["config_id"]: r for r in results}
    assert len(results) == 9

    d_cfg = next(t.config_id for t in tasks if t.source_object_name == "D")
    e_cfg = next(t.config_id for t in tasks if t.source_object_name == "E")
    assert by_cfg[d_cfg]["status"] == AUDIT_STATUS_FAILED
    assert by_cfg[e_cfg]["status"] == AUDIT_STATUS_SUCCESS   # E still runs
    assert rec.order_by_batch[1] == ["A", "B", "C"]          # other batches fine


def test_null_batch_id_gets_its_own_thread():
    tasks = sample_tasks()
    tasks.append(DummyTask(config_id=99, source_object_name="Z", batch_id=None, priority=1))
    _, max_workers, batches = orchestrate(tasks, Recorder().run_one)
    assert max_workers == 5
    assert None in batches


# ─────────────────────────── manual runner ───────────────────────────────
if __name__ == "__main__":
    rec = Recorder()
    results, max_workers, batches = orchestrate(sample_tasks(), rec.run_one)

    print(f"\nmax_workers (distinct batch_id count) = {max_workers}\n")
    for bid, bt in batches.items():
        print(f"Batch {bid}: {[t.source_object_name for t in bt]}")

    print("\nExecution timeline (table | event | thread):")
    t0 = rec.events[0][3]
    for name, kind, tname, ts in rec.events:
        print(f"  {ts - t0:6.3f}s  {name:>2}  {kind:<5}  {tname}")

    print("\nCompletion order per batch:")
    for bid, names in rec.order_by_batch.items():
        print(f"  Batch {bid}: {' -> '.join(names)}")

    for fn in [
        test_max_workers_equals_distinct_batch_count,
        test_tables_within_batch_run_sequentially_in_priority_order,
        test_batches_run_in_parallel,
        test_failure_in_one_batch_does_not_stop_others,
        test_null_batch_id_gets_its_own_thread,
    ]:
        fn()
        print(f"PASS  {fn.__name__}")
    print("\nAll checks passed.")
