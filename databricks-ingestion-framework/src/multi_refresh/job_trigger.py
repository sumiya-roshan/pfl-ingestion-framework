"""
job_trigger.py
==============
Databricks REST API helper for the multi-refresh orchestrator.

Responsibilities:
  1. Resolve a Databricks Job NAME to its numeric Job ID at runtime
     (so we never hardcode IDs in config tables).
  2. Fire a fire-and-forget run of that job via POST /api/2.1/jobs/run-now,
     passing notebook_params such as batch_start_date.

Usage:
    from multi_refresh.job_trigger import JobTrigger

    trigger = JobTrigger(
        workspace_url = "https://<workspace>.azuredatabricks.net",
        token         = "<pat-token>",
    )
    run_id = trigger.run_now_by_name(
        job_name       = "PFL_LSQ_Full_Load",
        notebook_params = {"batch_start_date": "2026-08-20 09:00:00"},
    )
"""
from __future__ import annotations

import json
import logging
from typing import Dict, Optional

import urllib.request
import urllib.error

logger = logging.getLogger(__name__)


class JobTrigger:
    """
    Thin wrapper around the Databricks Jobs REST API 2.1.
    Uses only stdlib (urllib) so no extra packages needed on the cluster.
    """

    def __init__(self, workspace_url: str, token: str):
        # Strip trailing slash for consistency
        self.workspace_url = workspace_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }
        # Cache: job_name -> job_id, populated lazily
        self._job_name_cache: Dict[str, int] = {}

    # -- Private helpers ------------------------------------------------------

    def _api(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        """Make a JSON REST call, return parsed response dict."""
        url = f"{self.workspace_url}{path}"
        data = json.dumps(body).encode("utf-8") if body else None
        req = urllib.request.Request(url, data=data, headers=self._headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"[JobTrigger] HTTP {exc.code} calling {method} {path}: {error_body}"
            ) from exc

    def _resolve_job_id(self, job_name: str) -> int:
        """
        Resolve a job name to its numeric job_id by paging through /api/2.1/jobs/list.
        Result is cached in-process so subsequent calls for the same name are free.
        """
        if job_name in self._job_name_cache:
            return self._job_name_cache[job_name]

        page_token: Optional[str] = None
        while True:
            params = {"limit": 25, "name": job_name}
            # Build query string
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            if page_token:
                qs += f"&page_token={page_token}"

            resp = self._api("GET", f"/api/2.1/jobs/list?{qs}")
            for job in resp.get("jobs", []):
                if job.get("settings", {}).get("name") == job_name:
                    job_id = int(job["job_id"])
                    self._job_name_cache[job_name] = job_id
                    logger.info(f"[JobTrigger] Resolved job '{job_name}' ? job_id={job_id}")
                    return job_id

            if resp.get("has_more") and resp.get("next_page_token"):
                page_token = resp["next_page_token"]
            else:
                break

        raise ValueError(
            f"[JobTrigger] No Databricks job found with name='{job_name}'. "
            f"Check the job name in tb_multi_refresh_job_config."
        )

    # -- Public API -----------------------------------------------------------

    def run_now_by_name(
        self,
        job_name: str,
        notebook_params: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        Trigger a job by NAME (fire-and-forget).

        Returns the run_id of the triggered run.
        Does NOT wait for completion.
        """
        job_id = self._resolve_job_id(job_name)
        body: dict = {"job_id": job_id}
        if notebook_params:
            body["notebook_params"] = {k: str(v) for k, v in notebook_params.items()}

        resp = self._api("POST", "/api/2.1/jobs/run-now", body)
        run_id = int(resp.get("run_id", 0))
        logger.info(
            f"[JobTrigger] Triggered job '{job_name}' (job_id={job_id}) ? run_id={run_id}"
        )
        return run_id
