"""
Tests for LookupExecutor._build_jdbc_probe_query — the single place the JDBC
row-presence query is assembled (delegates the SQL rewrite to
lookup.lookup_query_builder.build_lookup_query).

    python -m unittest databricks-ingestion-framework/tests/test_special_lookup_query.py -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lookup.lookup_executor import LookupExecutor
from ingestion.utils.config_manager import IngestionTaskConfig


class FakeSourceSystem:
    def __init__(self, source_type="ORACLE"):
        self.source_type = source_type


def make_task(**overrides):
    kwargs = dict(
        config_id=1,
        source_schema="dbo",
        source_object_name="orders",
        custom_query=None,
        load_type="FULL",
        incremental_column=None,
        primary_key_cols=None,
        target_catalog="main",
        target_schema="bronze",
        target_table="orders",
        pipeline_name="pfl_rdbms_ingestion",
        delta_layer="BRONZE",
        data_read_size=None,
        file_format=None,
        write_mode="overwrite",
        priority=0,
        batch_id=1,
        s3_source_bucket_name=None,
        s3_external_path=None,
        s3_column_delimiter=None,
        s3_first_row_header=None,
        s3_raw_sink_bucket_name=None,
        s3_raw_sink_file_path=None,
        schema_evolution_mode=None,
        partition_column=None,
        source_filter=None,
    )
    kwargs.update(overrides)
    return IngestionTaskConfig(**kwargs)


def probe(task, source_type="ORACLE"):
    return LookupExecutor._build_jdbc_probe_query(FakeSourceSystem(source_type), task)


SPECIAL_QUERY = (
    "SELECT ard.* FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_DTL ard INNER JOIN "
    "(SELECT ID FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_HDR WHERE "
    "CREATION_TIME_STAMP >= to_date('trigger_time','YYYY-MM-DD HH24:MI:SS') OR "
    "LAST_UPDATED_TIME_STAMP >= to_date('trigger_time','YYYY-MM-DD HH24:MI:SS')) "
    "arh ON arh.ID = ard.ASSET_REPO_HDRID"
)


class TestGeneric(unittest.TestCase):

    def test_full_load_is_unfiltered(self):
        query = probe(make_task(load_type="FULL", primary_key_cols="id"))
        self.assertIn("SELECT id FROM dbo.orders", query)
        self.assertNotIn("WHERE", query)
        self.assertIn("FETCH NEXT 1 ROWS ONLY", query)

    def test_full_load_postgres_uses_limit(self):
        query = probe(make_task(load_type="FULL", primary_key_cols="id"), source_type="POSTGRES")
        self.assertTrue(query.strip().endswith("LIMIT 1"))

    def test_incremental_single_delta_column(self):
        query = probe(make_task(
            load_type="INCREMENTAL",
            incremental_column="Delta_Column_1",
            silver_last_sink_date="2026-08-26 10:00:00",
            lookback_hours=3,
            primary_key_cols="id",
        ))
        self.assertIn("WHERE Delta_Column_1 >= '2026-08-26 07:00:00'", query)
        self.assertIn("FETCH NEXT 1 ROWS ONLY", query)

    def test_incremental_two_delta_columns_are_ORed_in_parens(self):
        query = probe(make_task(
            load_type="INCREMENTAL",
            incremental_column="Delta_Column_1",
            delta_column_2="Delta_Column_2",
            silver_last_sink_date="2026-08-26 10:00:00",
            lookback_hours=3,
            primary_key_cols="id",
        ))
        self.assertIn(
            "WHERE (Delta_Column_1 >= '2026-08-26 07:00:00' "
            "OR Delta_Column_2 >= '2026-08-26 07:00:00')",
            query,
        )

    def test_custom_query_with_existing_where_is_stripped(self):
        task = make_task(
            custom_query="SELECT * FROM dbo.orders WHERE region = 'APAC'",
            load_type="FULL",
            primary_key_cols="id",
        )
        query = probe(task)
        self.assertEqual(query, "SELECT id FROM dbo.orders FETCH NEXT 1 ROWS ONLY")


class TestSpecialTriggerTime(unittest.TestCase):

    def _task(self, **extra):
        base = dict(
            custom_query=SPECIAL_QUERY,
            load_type="INCREMENTAL",
            incremental_column="CREATION_TIME_STAMP",
            silver_last_sink_date="2026-08-26 10:00:00",
            lookback_hours=3,
            primary_key_cols="ID",
        )
        base.update(extra)
        return make_task(**base)

    def test_placeholder_replaced_and_or_wrapped(self):
        query = probe(self._task())
        self.assertIn("SELECT ard.ID FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_DTL ard INNER JOIN", query)
        self.assertIn(
            "WHERE (CREATION_TIME_STAMP >= to_date('2026-08-26 07:00:00','YYYY-MM-DD HH24:MI:SS') "
            "OR LAST_UPDATED_TIME_STAMP >= to_date('2026-08-26 07:00:00','YYYY-MM-DD HH24:MI:SS')",
            query,
        )
        self.assertIn("ON arh.ID = ard.ASSET_REPO_HDRID", query)
        self.assertTrue(query.strip().endswith("FETCH NEXT 1 ROWS ONLY"))

    def test_never_leaks_trigger_time_literal(self):
        self.assertNotIn("trigger_time", probe(self._task()))


if __name__ == "__main__":
    unittest.main()
