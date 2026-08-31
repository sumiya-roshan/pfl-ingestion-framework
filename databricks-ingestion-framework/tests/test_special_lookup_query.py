"""
Tests for JdbcConnector.build_probe_query(), which delegates to
lookup.lookup_query_builder.build_lookup_query.

Run with (no pytest needed):
    python -m unittest databricks-ingestion-framework/tests/test_special_lookup_query.py -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingestion.connectors.jdbc_connector import JdbcConnector
from ingestion.utils.config_manager import IngestionTaskConfig


class FakeSourceSystem:
    def __init__(self, source_type="ORACLE"):
        self.source_type = source_type
        self.host = "localhost"
        self.port = 1521
        self.database_name = "db"
        self.driver_class = None
        self.secret_scope = "scope"
        self.secret_key_credentials = "creds"


class FakeSecrets:
    def get_credentials(self, scope, key):
        return "user", "pass"


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


def make_connector(task, source_type="ORACLE"):
    return JdbcConnector(
        spark=None,
        source_system=FakeSourceSystem(source_type=source_type),
        ingest_obj=task,
        secrets=FakeSecrets(),
    )


SPECIAL_QUERY = (
    "SELECT ard.* FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_DTL ard INNER JOIN "
    "(SELECT ID FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_HDR WHERE "
    "CREATION_TIME_STAMP >= to_date('trigger_time','YYYY-MM-DD HH24:MI:SS') OR "
    "LAST_UPDATED_TIME_STAMP >= to_date('trigger_time','YYYY-MM-DD HH24:MI:SS')) "
    "arh ON arh.ID = ard.ASSET_REPO_HDRID"
)


class TestGeneric(unittest.TestCase):

    def test_full_load_is_unfiltered(self):
        task = make_task(load_type="FULL", primary_key_cols="id")
        query = make_connector(task).build_probe_query()
        self.assertIn("SELECT id FROM dbo.orders", query)
        self.assertNotIn("WHERE", query)
        self.assertIn("FETCH NEXT 1 ROWS ONLY", query)

    def test_full_load_postgres_uses_limit(self):
        task = make_task(load_type="FULL", primary_key_cols="id")
        query = make_connector(task, source_type="POSTGRES").build_probe_query()
        self.assertTrue(query.strip().endswith("LIMIT 1"))

    def test_incremental_single_delta_column(self):
        task = make_task(
            load_type="INCREMENTAL",
            incremental_column="Delta_Column_1",
            silver_last_sink_date="2026-08-26 10:00:00",
            lookback_hours=3,
            primary_key_cols="id",
        )
        query = make_connector(task).build_probe_query()
        self.assertIn("WHERE Delta_Column_1 >= '2026-08-26 07:00:00'", query)
        self.assertIn("FETCH NEXT 1 ROWS ONLY", query)

    def test_incremental_two_delta_columns_are_ORed_in_parens(self):
        task = make_task(
            load_type="INCREMENTAL",
            incremental_column="Delta_Column_1",
            delta_column_2="Delta_Column_2",
            silver_last_sink_date="2026-08-26 10:00:00",
            lookback_hours=3,
            primary_key_cols="id",
        )
        query = make_connector(task).build_probe_query()
        self.assertIn(
            "WHERE (Delta_Column_1 >= '2026-08-26 07:00:00' "
            "OR Delta_Column_2 >= '2026-08-26 07:00:00')",
            query,
        )

    def test_no_select_from_clause_raises(self):
        task = make_task(custom_query="EXEC some_proc", primary_key_cols="id")
        with self.assertRaises(ValueError):
            make_connector(task).build_probe_query()


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
        query = make_connector(self._task()).build_probe_query()
        self.assertIn("SELECT ard.ID FROM TAB_NEO_CAS_LMS.LMS_ASSET_REPOSSESSION_DTL ard INNER JOIN", query)
        self.assertIn(
            "WHERE (CREATION_TIME_STAMP >= to_date('2026-08-26 07:00:00','YYYY-MM-DD HH24:MI:SS') "
            "OR LAST_UPDATED_TIME_STAMP >= to_date('2026-08-26 07:00:00','YYYY-MM-DD HH24:MI:SS')",
            query,
        )
        self.assertIn("ON arh.ID = ard.ASSET_REPO_HDRID", query)
        self.assertTrue(query.strip().endswith("FETCH NEXT 1 ROWS ONLY"))

    def test_never_leaks_trigger_time_literal(self):
        query = make_connector(self._task()).build_probe_query()
        self.assertNotIn("trigger_time", query)

    def test_full_load_with_trigger_time_query_raises(self):
        # No watermark cutoff on a FULL load -> builder fails loudly rather
        # than emitting SQL that still contains 'trigger_time'.
        task = self._task(load_type="FULL", incremental_column=None,
                          silver_last_sink_date=None)
        with self.assertRaises(ValueError):
            make_connector(task).build_probe_query()


if __name__ == "__main__":
    unittest.main()
