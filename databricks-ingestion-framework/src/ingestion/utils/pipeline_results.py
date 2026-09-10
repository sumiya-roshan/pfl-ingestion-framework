"""
Shared task-result contract + end-of-run summary printing for the ingestion
entry-point notebook (``src/main/main.py``).

Both task runners — the connector path (``IngestionOrchestrator.run``) and the
API-export path (``MavisApiExtractor``) — hand back a ``TaskResult``-shaped dict,
so the fan-out, the summary and the exit logic never branch on source type.

``make_task_result`` is the single constructor for results the notebook builds
itself (the API-export success case, and every fault-tolerant failure case),
replacing the ``run_id=None`` / ``rows_read=0`` literals that were repeated at
each call site.

``print_results_summary`` renders the two tables printed at the end of a run —
per-table ingestion status, then Silver-trigger status. Formatting is kept
identical to the previous inline notebook version.
"""

from __future__ import annotations

from typing import Optional, TypedDict

from .config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_SKIPPED,
    AUDIT_STATUS_SUCCESS,
)


class TaskResult(TypedDict, total=False):
    """The dict shape every per-task runner returns. ``total=False`` because the
    orchestrator omits ``silver_result`` on its failure path and ``error_code``
    on its success path; consumers read the optional keys with ``.get``."""

    config_id: int
    run_id: Optional[str]
    status: str
    rows_read: int
    error_code: Optional[str]
    error: Optional[str]
    silver_result: Optional[dict]


def make_task_result(
    *,
    config_id: int,
    status: str,
    run_id: Optional[str] = None,
    rows_read: int = 0,
    error_code: Optional[str] = None,
    error: Optional[str] = None,
    silver_result: Optional[dict] = None,
) -> TaskResult:
    """
    Build a result in the shape every task runner returns. Keyword-only so call
    sites read as ``make_task_result(config_id=..., status=...)``.

    The defaults reproduce exactly what the notebook used to hardcode inline for
    the API-export success case and the fault-tolerant failure cases
    (``run_id=None``, ``rows_read=0``, no error, no Silver payload).
    """
    return {
        "config_id": config_id,
        "run_id": run_id,
        "status": status,
        "rows_read": rows_read,
        "error_code": error_code,
        "error": error,
        "silver_result": silver_result,
    }


STATUS_ICONS = {
    AUDIT_STATUS_SUCCESS: "✅",
    AUDIT_STATUS_SKIPPED: "⏭️",
}


def print_results_summary(results: list, silver_results: list) -> None:
    """
    Print the two end-of-run tables exactly as the notebook did inline: the
    per-table ingestion results (CONF ID / STATUS / ROWS / ERROR) followed by a
    totals line, then — only when there is at least one Silver payload — the
    Silver results table, followed by a Silver totals line.

    ``results``        : every task's ``TaskResult`` (any order — sorted here by
                         ``config_id``).
    ``silver_results`` : the non-empty ``silver_result`` payloads the caller
                         pulled out of ``results`` (passed in rather than
                         re-derived because the caller also needs that list for
                         the final failure check).
    """
    print(f"\n{'='*75}")
    print(f"{'CONF ID':>8}  {'STATUS':<10}  {'ROWS':>8}  ERROR")
    print(f"{'='*75}")
    for r in sorted(results, key=lambda x: x["config_id"]):
        icon = STATUS_ICONS.get(r["status"], "❌")
        error = (r.get("error") or "")[:50]
        print(f"{r['config_id']:>8}  {icon} {r['status']:<8}  {r.get('rows_read', 0):>8}  {error}")
    print(f"{'='*75}")

    succeeded = [r for r in results if r["status"] == AUDIT_STATUS_SUCCESS]
    skipped = [r for r in results if r["status"] == AUDIT_STATUS_SKIPPED]
    failed = [r for r in results if r["status"] == AUDIT_STATUS_FAILED]
    print(
        f"Total: {len(results)} | ✅ Succeeded: {len(succeeded)} | "
        f"⏭️ Skipped (0 rows): {len(skipped)} | ❌ Failed: {len(failed)}\n"
    )

    if silver_results:
        print(f"{'='*75}")
        print(f"{'CONF ID':>8}  {'SILVER STATUS':<14}  TARGET")
        print(f"{'='*75}")
        for r in sorted(silver_results, key=lambda x: x["config_id"]):
            icon = "✅" if r["status"] == "SUCCESS" else "❌"
            print(f"{r['config_id']:>8}  {icon} {r['status']:<12}  {r.get('target', '')}")
        print(f"{'='*75}")

    silver_failed = [r for r in silver_results if r["status"] == "FAILED"]
    print(
        f"Silver — Total: {len(silver_results)} | "
        f"✅ Succeeded: {len(silver_results) - len(silver_failed)} | ❌ Failed: {len(silver_failed)}\n"
    )
