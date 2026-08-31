"""
lookup_query_builder — derive a cheap row-presence "lookup query" from the
main extraction "source query".

The lookup query is executed via JDBC directly against the source system
(Postgres, Oracle, ...) to check whether any rows exist before running a full
or incremental extraction. It must therefore be a single valid SQL string the
source database can run as-is (no outer ``SELECT key FROM (...) _src`` wrapper).

Transformation rules
--------------------
* SELECT list  -> replaced with ``key_col`` (keeping a ``alias.`` prefix).
* generic pattern:
    - any existing WHERE clause is UNCONDITIONALLY stripped (Case 4);
    - full        -> just ``SELECT key FROM src`` + row-limit clause;
    - incremental -> the delta predicate becomes the ONLY WHERE clause.
* special_trigger_time pattern (Oracle header/detail join): the templated
  ``to_date('trigger_time', <fmt>)`` predicates are filled with ``cutoff``,
  the OR is wrapped in parens, and ``FETCH NEXT 1 ROWS ONLY`` is appended.

Examples (input -> output)
--------------------------
Case 1 — generic, full load, key_col="id":
    select * from public.contracts
 -> select id from public.contracts LIMIT 1

Case 2 — generic, incremental, key_col="id", delta_col="updated_at",
         delta_col_2="created_at", cutoff="2026-08-28 07:00:00":
    select * from public.contracts
 -> select id from public.contracts WHERE (updated_at >= '2026-08-28 07:00:00'
        OR created_at >= '2026-08-28 07:00:00') LIMIT 1
    (delta_col_2=None -> single predicate: "WHERE updated_at >= '...'", no OR/parens)

Case 3 — special_trigger_time, key_col="ID", cutoff="2026-08-28 07:00:00":
    SELECT ard.* FROM T.DTL ard INNER JOIN (SELECT ID FROM T.HDR WHERE
      CREATION_TIME_STAMP >= to_date('trigger_time','YYYY-MM-DD HH24:MI:SS') OR
      LAST_UPDATED_TIME_STAMP >= to_date('trigger_time','YYYY-MM-DD HH24:MI:SS'))
      arh ON arh.ID = ard.HDRID
 -> SELECT ard.ID FROM T.DTL ard INNER JOIN (SELECT ID FROM T.HDR WHERE
      (CREATION_TIME_STAMP >= to_date('2026-08-28 07:00:00','YYYY-MM-DD HH24:MI:SS')
      OR LAST_UPDATED_TIME_STAMP >= to_date('2026-08-28 07:00:00','YYYY-MM-DD HH24:MI:SS'))
      arh ON arh.ID = ard.HDRID FETCH NEXT 1 ROWS ONLY

Case 4a — generic full load, existing WHERE (incl. subquery) stripped,
          key_col="LOANID", dialect="oracle":
    SELECT * FROM T.RS_DTL WHERE LOANID IN
      (SELECT ID FROM T.LA_DTL WHERE STATUS = 'A' OR STATUS = 'C')
 -> SELECT LOANID FROM T.RS_DTL FETCH NEXT 1 ROWS ONLY

Case 4b — generic incremental, existing WHERE stripped and replaced,
          key_col="LOANID", delta_col="UPDATED_DATE",
          cutoff="2026-08-28 07:00:00", dialect="oracle":
    (same input as 4a)
 -> SELECT LOANID FROM T.RS_DTL WHERE UPDATED_DATE >= '2026-08-28 07:00:00'
      FETCH NEXT 1 ROWS ONLY
"""
from __future__ import annotations

import re

__all__ = ["build_lookup_query", "detect_pattern_type"]


# "SELECT <list> FROM" — g1 the SELECT keyword+ws (original case preserved),
# g2 an optional "alias." prefix from the first item, g3 the FROM keyword+ws.
_SELECT_RE = re.compile(r"(SELECT\s+)((?:\w+\.)?)(?:\*|.+?)(\s+FROM\s+)",
                        re.IGNORECASE | re.DOTALL)

# The special_trigger_time predicate: two to_date('trigger_time', <fmt>) calls
# joined by OR. Group 1 = col1, group 2 = shared date format, group 3 = col2.
# The second to_date(...) is fully consumed so it can be rewritten cleanly.
_TRIGGER_RE = re.compile(
    r"(\w+)\s*>=\s*to_date\(\s*'trigger_time'\s*,\s*'([^']+)'\s*\)\s*OR\s*"
    r"(\w+)\s*>=\s*to_date\(\s*'trigger_time'\s*,\s*'[^']+'\s*\)",
    re.IGNORECASE,
)


def detect_pattern_type(source_query: str) -> str:
    """
    Infer ``pattern_type`` from a source query by *structural* match, not a
    substring test.

    Returns ``"special_trigger_time"`` only when the query contains the exact
    shape ``<col> >= to_date('trigger_time', <fmt>) OR <col2> >= to_date(
    'trigger_time', <fmt>)``. A query that merely mentions the word
    ``trigger_time`` in a column/table name (``trigger_time_log``) or an
    unrelated literal (``STATUS = 'trigger_time'``) returns ``"generic"``.

    >>> detect_pattern_type("select * from trigger_time_log")
    'generic'
    >>> detect_pattern_type("select * from t where status = 'trigger_time'")
    'generic'
    """
    return "special_trigger_time" if _TRIGGER_RE.search(source_query or "") else "generic"


def _row_limit_clause(dialect: str) -> str:
    """postgres -> ``LIMIT 1``; oracle / anything else -> ``FETCH NEXT 1 ROWS ONLY``."""
    return "LIMIT 1" if (dialect or "").lower() == "postgres" else "FETCH NEXT 1 ROWS ONLY"


def _replace_select_list(query: str, key_col: str) -> str:
    """Replace everything between the first SELECT and FROM with ``key_col``,
    keeping the leading ``alias.`` of the first select item when present."""
    new, n = _SELECT_RE.subn(
        lambda m: f"{m.group(1)}{m.group(2)}{key_col}{m.group(3)}", query, count=1
    )
    if n == 0:
        raise ValueError(f"No SELECT...FROM clause found in source query: {query!r}")
    return new


def _strip_existing_where(query: str) -> str:
    """Remove everything from the first WHERE keyword onward (Case 4).

    Lookup queries are existence-checks only, so no trailing clause (ORDER BY,
    GROUP BY, etc.) needs to be preserved, and splitting on the first ``WHERE``
    is sufficient for the flat ``SELECT ... FROM <table> [WHERE ...]`` source
    queries this path handles.
    """
    return re.split(r"\bWHERE\b", query, maxsplit=1, flags=re.IGNORECASE)[0].rstrip()


def _fill_trigger_predicate(query: str, cutoff: str) -> str:
    """Fill both 'trigger_time' placeholders with ``cutoff`` and wrap the OR in
    parens. The shared date format is taken from the source query itself."""
    def repl(m: re.Match) -> str:
        col1, fmt, col2 = m.group(1), m.group(2), m.group(3)
        return (f"({col1} >= to_date('{cutoff}','{fmt}') "
                f"OR {col2} >= to_date('{cutoff}','{fmt}')")

    new, n = _TRIGGER_RE.subn(repl, query, count=1)
    if n == 0:
        raise ValueError(
            "pattern_type='special_trigger_time' but the expected dual-column "
            "to_date('trigger_time', ...) OR to_date('trigger_time', ...) "
            f"predicate was not found in source query: {query!r}"
        )
    return new


def build_lookup_query(source_query: str, key_col: str, load_type: str,
                       cutoff: str | None = None,
                       delta_col: str | None = None,
                       delta_col_2: str | None = None,
                       dialect: str = "postgres",
                       pattern_type: str = "generic") -> str:
    """
    Build a row-presence lookup query from ``source_query``.

    Parameters
    ----------
    source_query : the main extraction query.
    key_col      : column to project (existence probe only needs one column).
    load_type    : ``"full"`` or ``"incremental"``.
    cutoff       : watermark timestamp; required for incremental generic and
                   for the special_trigger_time pattern.
    delta_col    : primary watermark column (incremental generic).
    delta_col_2  : optional second watermark column -> ``(a >= c OR b >= c)``.
    dialect      : ``"postgres"`` -> ``LIMIT 1``; else ``FETCH NEXT 1 ROWS ONLY``.
                   Ignored for special_trigger_time (always Oracle-style).
    pattern_type : ``"generic"`` or ``"special_trigger_time"`` — supplied by the
                   caller (see :func:`detect_pattern_type`), never sniffed here.

    Raises
    ------
    ValueError
        No SELECT...FROM clause; special_trigger_time predicate not found;
        incremental generic without ``delta_col``; or a residual
        ``'trigger_time'`` literal in the result.
    """
    load_type = (load_type or "").strip().lower()
    if load_type not in ("full", "incremental"):
        raise ValueError(f"load_type must be 'full' or 'incremental', got {load_type!r}")

    query = _replace_select_list(source_query, key_col)

    if pattern_type == "special_trigger_time":
        if not cutoff:
            raise ValueError("special_trigger_time pattern requires 'cutoff'.")
        query = _fill_trigger_predicate(query, cutoff)
        result = f"{query.strip()} FETCH NEXT 1 ROWS ONLY"

    elif pattern_type == "generic":
        query = _strip_existing_where(query).strip()
        if load_type == "incremental":
            if not delta_col:
                raise ValueError(
                    "Incremental generic lookup requires 'delta_col'."
                )
            if not cutoff:
                raise ValueError(
                    "Incremental generic lookup requires 'cutoff'."
                )
            pred = f"{delta_col} >= '{cutoff}'"
            if delta_col_2:
                pred = f"({pred} OR {delta_col_2} >= '{cutoff}')"
            query = f"{query} WHERE {pred}"
        result = f"{query} {_row_limit_clause(dialect)}"

    else:
        raise ValueError(f"Unknown pattern_type: {pattern_type!r}")

    if "trigger_time" in result:
        raise ValueError(f"Lookup query still contains 'trigger_time': {result!r}")
    return result
