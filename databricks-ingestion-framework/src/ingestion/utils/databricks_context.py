"""
Generic Databricks Job / Run metadata retrieval.

Not pipeline-specific — this only reads job/run identifiers and trigger info off
the notebook runtime context so callers never hardcode them. Shared by the
ingestion entry-point notebooks (``src/main/main.py`` and friends).

``dbutils`` is a notebook-scoped object that cannot be imported as a module, so
every caller passes its own. ``job_id`` is passed in already-resolved (it is the
``job_id`` widget value) so it is read exactly once by the caller rather than
re-fetched here.
"""

from __future__ import annotations


def get_databricks_job_context(dbutils, job_id: str | None = None) -> dict:
    """
    Return a dict of Databricks job/run metadata pulled from the notebook
    context: ``job_id``, ``job_name``, ``notebook_name``, ``databricks_url``,
    ``trigger_type``, ``trigger_id``, ``trigger_name``.

    ``job_id`` (the widget value) is only used to build ``databricks_url``; the
    ``"job_id"`` key in the returned dict is the runtime ``jobId`` from the
    context. Any value that cannot be read comes back as ``None``.
    """
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    def get_context_value(method_name):
        try:
            return getattr(context, method_name)().get()
        except Exception:
            return None

    databricks_url = get_context_value("apiUrl")
    databricks_url = (
        f"{databricks_url}/#job/{job_id}" if databricks_url and job_id else None
    )

    return {
        "job_id": get_context_value("jobId"),
        "job_name": get_context_value("jobName"),
        "notebook_name": get_context_value("notebookPath"),  # Can change it to point silver notebook path later
        "databricks_url": databricks_url,
        "trigger_type": get_context_value("triggerType"),
        "trigger_id": get_context_value("triggerId"),
        "trigger_name": get_context_value("triggerName"),
    }
