"""
lookup_query_builder — derive a cheap row-presence "lookup query" from the
main extraction "source query".

The lookup query runs via JDBC directly against the source system to check
whether any rows exist before a full / incremental extraction, so it must be a
single valid SQL string the source DB can run as-is (no "(...) _src" wrapper).

Rules
-----
* SELECT list -> replaced with key_col (keeping an "alias." prefix).
* generic pattern:
    - any existing WHERE is unconditionally stripped;
    - full        -> SELECT key FROM src  + row-limit clause;
    - incremental -> the delta predicate becomes the ONLY WHERE clause.
* special_trigger_time pattern (Oracle header/detail join): the templated
  to_date('trigger_time', <fmt>) predicates are filled with cutoff, the OR is
  wrapped in parens, and FETCH NEXT 1 ROWS ONLY is appended.

Examples (input -> output)
    select * from public.contracts
 -> select id from public.contracts LIMIT 1                       # full

    select * from public.contracts                                # incremental,
 -> select id from public.contracts WHERE (updated_at >= '<c>'    # 2 delta cols
        OR created_at >= '<c>') LIMIT 1

    SELECT * FROM t WHERE x IN (SELECT ... )                      # existing WHERE
 -> SELECT id FROM t FETCH NEXT 1 ROWS ONLY                       # stripped
"""

import re

__all__ = ["build_lookup_query", "detect_pattern_type"]

# "SELECT <list> FROM": g1 = SELECT + ws, g2 = optional "alias." on the first
# item, g3 = FROM + ws. Non-greedy so a nested "(SELECT ... FROM ...)" is left be.
_SELECT_RE = re.compile(
    r"(SELECT\s+)((?:\w+\.)?)(?:\*|.+?)(\s+FROM\s+)", re.IGNORECASE | re.DOTALL
)

# special_trigger_time predicate: two to_date('trigger_time', <fmt>) calls ORed.
# g1 = col1, g2 = shared date format, g3 = col2 (2nd to_date fully consumed).
_TRIGGER_RE = re.compile(
    r"(\w+)\s*>=\s*to_date\(\s*'trigger_time'\s*,\s*'([^']+)'\s*\)\s*OR\s*"
    r"(\w+)\s*>=\s*to_date\(\s*'trigger_time'\s*,\s*'[^']+'\s*\)",
    re.IGNORECASE,
)


def detect_pattern_type(source_query):
    """ "special_trigger_time" if the query structurally matches the dual
    to_date('trigger_time', ...) OR ... predicate (not a mere substring), else
    "generic" — so a column/table named trigger_time_log or a literal
    STATUS = 'trigger_time' stays generic."""
    return (
        "special_trigger_time" if _TRIGGER_RE.search(source_query or "") else "generic"
    )


def build_lookup_query(
    source_query,
    key_col,
    load_type,
    cutoff=None,
    delta_col=None,
    delta_col_2=None,
    dialect="postgres",
    pattern_type="generic",
    row_limit=True,
):
    """
    Build a row-presence lookup query from source_query.

    load_type    : "full" or "incremental".
    cutoff       : watermark timestamp; required for incremental / trigger_time.
    delta_col(_2): watermark column(s); delta_col_2 -> "(a >= c OR b >= c)".
    dialect      : row-limit syntax — "postgres"/"mysql" -> LIMIT 1,
                   "mssql" -> SELECT TOP 1, anything else -> FETCH NEXT 1 ROWS
                   ONLY (ignored for special_trigger_time — always Oracle-style).
    pattern_type : "generic" or "special_trigger_time" — from the caller
                   (see detect_pattern_type), never sniffed here.
    row_limit    : generic only — set False for the Staging_Flag=1 key extract,
                   which pulls ALL key rows unfiltered ("SELECT <keys> FROM src").
    """
    load_type = (load_type or "").strip().lower()
    if load_type not in ("full", "incremental"):
        raise ValueError(
            f"load_type must be 'full' or 'incremental', got {load_type!r}"
        )

    # ── SELECT list -> key_col (keep the first item's "alias." prefix) ──────
    query, _n = _SELECT_RE.subn(
        lambda m: f"{m.group(1)}{m.group(2)}{key_col}{m.group(3)}",
        source_query,
        count=1,
    )

    if pattern_type == "special_trigger_time":
        if not cutoff:
            raise ValueError(
                "cutoff is required when pattern_type='special_trigger_time'"
            )
        query, _n = _TRIGGER_RE.subn(
            lambda m: (
                f"({m.group(1)} >= to_date('{cutoff}','{m.group(2)}') "
                f"OR {m.group(3)} >= to_date('{cutoff}','{m.group(2)}'))"
            ),
            query,
            count=1,
        )

        result = f"{query.strip()} FETCH NEXT 1 ROWS ONLY"

    elif pattern_type == "generic":
        # Existing WHERE (at any nesting) is dropped — existence checks need no
        # trailing ORDER BY / GROUP BY, so splitting on the first WHERE is enough.
        query = re.split(r"\bWHERE\b", query, maxsplit=1, flags=re.IGNORECASE)[
            0
        ].strip()
        if load_type == "incremental":
            if not delta_col or not cutoff:
                raise ValueError(
                    "delta_col and cutoff are required when load_type='incremental'"
                )
            pred = f"{delta_col} >= '{cutoff}'"
            if delta_col_2:
                pred = f"({pred} OR {delta_col_2} >= '{cutoff}')"
            query = f"{query} WHERE {pred}"
        d = (dialect or "").lower()

        if not row_limit:
            result = query
        elif d in ("postgres", "mysql"):
            result = f"{query} LIMIT 1"
        elif d == "mssql":
            result = re.sub(r"(?i)^(\s*SELECT\s+)", r"\1TOP 1 ", query, count=1)
        else:
            result = f"{query} FETCH NEXT 1 ROWS ONLY"

    else:
        raise ValueError(f"Unknown pattern_type: {pattern_type!r}")

    if "trigger_time" in result:
        raise ValueError(f"Lookup query still contains 'trigger_time': {result!r}")
    return result
