"""
Thread-pool fan-out strategies for the ingestion entry-point notebook
(``src/main/main.py``).

Two strategies. Both are fault-tolerant — a failing task is caught, turned into
a FAILED ``TaskResult`` and collected, so one bad table never stops the others —
and both return a flat ``list`` of ``TaskResult`` dicts in any order (the caller
sorts for display).

``execute_batches`` — the connector path (RDBMS / NoSQL / S3).
    Tasks are grouped by ``batch_id``. One pool thread per distinct batch runs
    that batch's tables one-by-one in ascending ``priority`` order; individual
    tables never get their own thread. ``max_workers`` = distinct ``batch_id``
    count — DERIVED here, never hardcoded, never taken from a widget or config.

``execute_parallel`` — the API-export path (LSQ Mavis).
    Each export is fully independent (start → poll → download → unzip); there is
    no ``batch_id`` or ``priority`` to honour, so every task gets its own pool
    thread. ``max_workers`` = task count.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from .config_manager import AUDIT_STATUS_FAILED
from .pipeline_results import make_task_result


def execute_parallel(tasks, run_one) -> list:
    """
    Run every task on its own pool thread — no batch/priority grouping.

    ``max_workers`` = one per task. Fault-tolerant: a failing task becomes a
    FAILED result and never stops the others.
    """
    max_workers = len(tasks)
    results: list = []
    print(
        f"\nStarting {len(tasks)} task(s) with ThreadPoolExecutor "
        f"(max_workers={max_workers})..."
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {executor.submit(run_one, task): task for task in tasks}
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                results.append(future.result())
            except Exception as exc:
                print(f"Task {task.source_object_name} (Config ID: {task.config_id}) failed with exception: {exc}")
                results.append(make_task_result(
                    config_id = task.config_id,
                    status    = AUDIT_STATUS_FAILED,
                    error     = str(exc),
                ))
    return results


def execute_batches(tasks, run_one) -> list:
    """
    Group ``tasks`` by ``batch_id`` and run them through ``run_one`` on a thread
    pool. One pool thread per distinct batch runs that batch's tables one-by-one
    in ascending ``priority`` order; tables never get their own thread.
    ``max_workers`` = distinct ``batch_id`` count — DERIVED here, never hardcoded
    or taken from a widget/config.

    Fault-tolerant: a failure in ``run_one`` (or a whole batch future) is caught,
    turned into a FAILED result and collected — one bad table never stops the
    others.
    """
    batches = {}
    for task in sorted(tasks, key=lambda t: t.priority):
        batches.setdefault(task.batch_id, []).append(task)

    max_workers = len(batches)

    def run_batch(batch_id, batch_tasks: list) -> list:
        """Run every table in one batch sequentially, in priority order."""
        batch_results = []
        print(
            f"[Batch {batch_id}] Starting {len(batch_tasks)} table(s) sequentially: "
            f"{[t.source_object_name for t in batch_tasks]}"
        )
        for task in batch_tasks:
            try:
                batch_results.append(run_one(task))
            except Exception as exc:
                print(f"Task {task.source_object_name} (Config ID: {task.config_id}) failed with exception: {exc}")
                batch_results.append(make_task_result(
                    config_id = task.config_id,
                    status    = AUDIT_STATUS_FAILED,
                    error     = str(exc),
                ))
        return batch_results

    results: list = []
    print(
        f"\nStarting {len(tasks)} tasks across {len(batches)} batch(es) with "
        f"ThreadPoolExecutor (max_workers={max_workers})..."
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(run_batch, batch_id, batch_tasks): batch_id
            for batch_id, batch_tasks in batches.items()
        }

        for future in as_completed(future_to_batch):
            batch_id = future_to_batch[future]
            try:
                results.extend(future.result())
            except Exception as exc:
                print(f"Batch {batch_id} failed with exception: {exc}")
                for task in batches[batch_id]:
                    results.append(make_task_result(
                        config_id = task.config_id,
                        status    = AUDIT_STATUS_FAILED,
                        error     = str(exc),
                    ))
    return results
