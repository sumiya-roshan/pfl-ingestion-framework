"""
lookup_query_builder — build a cheap row-presence "lookup query" from the
main extraction "source query" by rewriting it in place with regex capture
groups (no outer ``SELECT key FROM (...) _src`` wrapper).
"""
import re

# "SELECT <list> FROM" — keep the optional "alias." prefix from the first item.
select_pattern = re.compile(r"SELECT\s+((?:\w+\.)?)(?:\*|.+?)\s+FROM\s+",
                            re.IGNORECASE | re.DOTALL)

# Case 3: the pre-templated "trigger_time" predicate (both to_date() calls share
# the same format string — captured once from group 2 and reused).
trigger_pattern = re.compile(
    r"(\w+)\s*>=\s*to_date\(\s*'trigger_time'\s*,\s*'([^']+)'\s*\)\s*OR\s*"
    r"(\w+)\s*>=\s*to_date\(\s*'trigger_time'\s*,\s*'[^']+'\s*\)",
    re.IGNORECASE,
)


def detect_pattern_type(query):
    """
    "special_trigger_time" when the source query carries the templated
    "<col> >= to_date('trigger_time', <fmt>) OR <col2> >= to_date('trigger_time'..."
    predicate structure, else "generic". Structural match, not a substring check.
    """
    return "special_trigger_time" if trigger_pattern.search(query or "") else "generic"


def replace_select(query, key_col):
    """Replace everything between SELECT and FROM with key_col (keeping alias)."""
    new, n = select_pattern.subn(
        lambda m: f"SELECT {m.group(1)}{key_col} FROM ", query, count=1)
    if n == 0:
        raise ValueError(f"No SELECT...FROM clause found in: {query!r}")
    return new


def fill_special_predicate(query, cutoff):
    def repl(m):
        col1, fmt, col2 = m.group(1), m.group(2), m.group(3)
        return (f"({col1} >= to_date('{cutoff}','{fmt}') "
                f"OR {col2} >= to_date('{cutoff}','{fmt}')")
    new, n = trigger_pattern.subn(repl, query, count=1)
    if n == 0:
        raise ValueError(f"Expected 'trigger_time' predicate not found in: {query!r}")
    return new


def build_lookup_query(source_query, key_col, load_type, cutoff=None,
                       delta_col=None, delta_col_2=None, dialect="postgres",
                       pattern_type="generic"):
    q = replace_select(source_query, key_col)

    # Case 3: special pre-templated Oracle-style pattern.
    if pattern_type == "special_trigger_time":
        if not cutoff:
            raise ValueError("special_trigger_time pattern requires a cutoff value")
        q = fill_special_predicate(q, cutoff).strip() + " FETCH NEXT 1 ROWS ONLY"
        if "trigger_time" in q:
            raise ValueError(f"Unresolved 'trigger_time' in: {q!r}")
        return q

    # Case 1 & 2: generic full / incremental.
    if load_type == "incremental" and delta_col and cutoff:
        if re.search(r"\bWHERE\b", q, re.IGNORECASE):
            raise ValueError(f"Source query already has a WHERE clause: {q!r}")
        predicate = f"{delta_col} >= '{cutoff}'"
        if delta_col_2:
            predicate = f"({predicate} OR {delta_col_2} >= '{cutoff}')"
        q = f"{q} WHERE {predicate}"

    limit_clause = "LIMIT 1" if dialect == "postgres" else "FETCH NEXT 1 ROWS ONLY"
    return f"{q} {limit_clause}"


def run_lookup_query(spark, jdbc_options, lookup_query):
    """
    Execute the lookup query against the source system over JDBC (same
    mechanism as LookupExecutor._lookup_jdbc) and return 1 if any row exists,
    else 0. ``jdbc_options`` is the connector's resolved options dict
    (url / driver / user / password ...).
    """
    print(f"[lookup_query_builder] running against source: {lookup_query}")
    df = (
        spark.read.format("jdbc")
        .options(**jdbc_options)
        .option("query", lookup_query)  # Spark JDBC 'query' — no subquery wrap needed
        .load()
    )
    count = 1 if df.collect() else 0
    print(f"[lookup_query_builder] rows present = {bool(count)} (count={count})")
    return count
