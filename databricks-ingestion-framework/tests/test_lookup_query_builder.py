"""
pytest suite for lookup.lookup_query_builder.

    pytest databricks-ingestion-framework/tests/test_lookup_query_builder.py
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lookup.lookup_query_builder import build_lookup_query, detect_pattern_type

CUTOFF = "2026-08-28 07:00:00"

SPECIAL_SRC = (
    "SELECT ard.* FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_DTL ard INNER JOIN "
    "(SELECT ID FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_HDR WHERE "
    "CREATION_TIME_STAMP >= to_date('trigger_time','YYYY-MM-DD HH24:MI:SS') OR "
    "LAST_UPDATED_TIME_STAMP >= to_date('trigger_time','YYYY-MM-DD HH24:MI:SS')) "
    "arh ON arh.ID = ard.ASSET_REPO_HDRID"
)

WHERE_SUBQUERY_SRC = (
    "SELECT * FROM TAB_NEO_CAS_LMS.LMS_REPAYSCHEDULE_DTL WHERE LOANID IN "
    "(SELECT ID FROM TAB_NEO_CAS_LMS.LMS_LOANACCOUNT_DTL WHERE STATUS = 'A' OR "
    "(STATUS = 'C' AND CANCEL_CLOSURE_DATE >= TO_DATE(SYSDATE - 30,'DD-MON-RRRR')) "
    "OR (STATUS = 'X' AND CANCEL_CLOSURE_DATE >= TO_DATE(SYSDATE - 30,'DD-MON-RRRR')))"
)


# ── Case 1: generic full ────────────────────────────────────────────────────
def test_case1_generic_full():
    assert build_lookup_query("select * from public.contracts", "id", "full") == \
        "select id from public.contracts LIMIT 1"


def test_explicit_column_list_is_replaced():
    assert build_lookup_query("select col1, col2, col3 from public.contracts",
                              "id", "full") == \
        "select id from public.contracts LIMIT 1"


# ── Case 2: generic incremental ─────────────────────────────────────────────
def test_case2_two_delta_columns_or_in_parens():
    assert build_lookup_query("select * from public.contracts", "id", "incremental",
                              cutoff=CUTOFF, delta_col="updated_at",
                              delta_col_2="created_at") == (
        "select id from public.contracts WHERE (updated_at >= '2026-08-28 07:00:00' "
        "OR created_at >= '2026-08-28 07:00:00') LIMIT 1"
    )


def test_case2_single_delta_column_no_or_no_parens():
    out = build_lookup_query("select * from public.contracts", "id", "incremental",
                             cutoff=CUTOFF, delta_col="updated_at")
    assert out == "select id from public.contracts WHERE updated_at >= '2026-08-28 07:00:00' LIMIT 1"
    assert " OR " not in out
    assert "(" not in out


# ── Case 3: special_trigger_time ────────────────────────────────────────────
def test_case3_special_trigger_time():
    out = build_lookup_query(SPECIAL_SRC, "ID", "incremental", cutoff=CUTOFF,
                             pattern_type="special_trigger_time")
    assert out == (
        "SELECT ard.ID FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_DTL ard INNER JOIN "
        "(SELECT ID FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_HDR WHERE "
        "(CREATION_TIME_STAMP >= to_date('2026-08-28 07:00:00','YYYY-MM-DD HH24:MI:SS') "
        "OR LAST_UPDATED_TIME_STAMP >= to_date('2026-08-28 07:00:00','YYYY-MM-DD HH24:MI:SS')) "
        "arh ON arh.ID = ard.ASSET_REPO_HDRID FETCH NEXT 1 ROWS ONLY"
    )
    assert "trigger_time" not in out


def test_case3_shared_date_format_extracted_not_assumed():
    src = SPECIAL_SRC.replace("YYYY-MM-DD HH24:MI:SS", "DD/MM/YYYY")
    out = build_lookup_query(src, "ID", "incremental", cutoff=CUTOFF,
                             pattern_type="special_trigger_time")
    assert out.count("to_date('2026-08-28 07:00:00','DD/MM/YYYY')") == 2


def test_case3_dialect_param_ignored():
    a = build_lookup_query(SPECIAL_SRC, "ID", "incremental", cutoff=CUTOFF,
                           dialect="postgres", pattern_type="special_trigger_time")
    assert a.endswith("FETCH NEXT 1 ROWS ONLY")


# ── Case 4: existing WHERE unconditionally stripped ─────────────────────────
def test_case4a_full_load_strips_existing_where():
    assert build_lookup_query(WHERE_SUBQUERY_SRC, "LOANID", "full",
                              dialect="oracle") == \
        "SELECT LOANID FROM TAB_NEO_CAS_LMS.LMS_REPAYSCHEDULE_DTL FETCH NEXT 1 ROWS ONLY"


def test_case4b_incremental_strips_and_replaces_where():
    assert build_lookup_query(WHERE_SUBQUERY_SRC, "LOANID", "incremental",
                              delta_col="UPDATED_DATE", cutoff=CUTOFF,
                              dialect="oracle") == (
        "SELECT LOANID FROM TAB_NEO_CAS_LMS.LMS_REPAYSCHEDULE_DTL WHERE "
        "UPDATED_DATE >= '2026-08-28 07:00:00' FETCH NEXT 1 ROWS ONLY"
    )


def test_case4_where_with_nested_subqueries_removed_from_first_where_onward():
    # WHERE holds 3 subquery predicates across nested parens; everything from
    # the first WHERE is dropped, leaving just the table reference.
    out = build_lookup_query(WHERE_SUBQUERY_SRC, "LOANID", "full", dialect="oracle")
    assert out == "SELECT LOANID FROM TAB_NEO_CAS_LMS.LMS_REPAYSCHEDULE_DTL FETCH NEXT 1 ROWS ONLY"
    assert "STATUS" not in out and "SELECT ID FROM" not in out


def test_case4_trailing_clause_after_where_is_also_dropped():
    # No trailing clause needs preserving for an existence check.
    src = "SELECT * FROM t WHERE a = (SELECT max(x) FROM u) ORDER BY a"
    assert build_lookup_query(src, "id", "full") == "SELECT id FROM t LIMIT 1"


# ── Row-limit clause ───────────────────────────────────────────────────────
def test_row_limit_per_dialect():
    assert build_lookup_query("select * from t", "id", "full",
                              dialect="postgres").endswith("LIMIT 1")
    assert build_lookup_query("select * from t", "id", "full",
                              dialect="mysql").endswith("LIMIT 1")
    assert build_lookup_query("select * from t", "id", "full",
                              dialect="oracle").endswith("FETCH NEXT 1 ROWS ONLY")
    assert build_lookup_query("select * from t", "id", "full",
                              dialect="mssql") == "select TOP 1 id from t"


# ── detect_pattern_type ────────────────────────────────────────────────────
def test_detect_special_shape():
    assert detect_pattern_type(SPECIAL_SRC) == "special_trigger_time"


def test_detect_generic_for_literal_string_value():
    assert detect_pattern_type("SELECT * FROM t WHERE STATUS = 'trigger_time'") == "generic"


def test_detect_generic_for_identifier_named_trigger_time():
    assert detect_pattern_type("SELECT * FROM trigger_time_log") == "generic"
    assert detect_pattern_type("SELECT trigger_time_col FROM t") == "generic"


# ── Staging_Flag=1 key extract: no row-limit clause ────────────────────────
def test_row_limit_false_omits_limit_clause():
    assert build_lookup_query("SELECT * FROM dbo.contracts", "id, name", "full",
                              row_limit=False) == "SELECT id, name FROM dbo.contracts"


def test_row_limit_false_strips_existing_where():
    assert build_lookup_query("SELECT * FROM dbo.contracts WHERE region = 1",
                              "id", "full", row_limit=False) == "SELECT id FROM dbo.contracts"


# ── Error handling ─────────────────────────────────────────────────────────
def test_bad_load_type_raises():
    with pytest.raises(ValueError):
        build_lookup_query("select * from t", "id", "delta")


def test_trigger_time_left_in_output_raises():
    # special pattern selected but the predicate shape isn't the templated one,
    # so 'trigger_time' survives -> caught by the final guard.
    src = "SELECT * FROM t WHERE ts >= to_date('trigger_time','YYYY')"
    with pytest.raises(ValueError):
        build_lookup_query(src, "id", "full", pattern_type="special_trigger_time")
