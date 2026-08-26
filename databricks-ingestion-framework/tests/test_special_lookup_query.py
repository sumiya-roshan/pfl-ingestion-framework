"""
Tests for the special-case lookup query mechanism in JdbcConnector.build_probe_query().

Run with (no pytest needed):
    python -m unittest databricks-ingestion-framework/tests/test_special_lookup_query.py -v
"""
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingestion.connectors import jdbc_connector
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


class TestGenericFallback(unittest.TestCase):
    """No special_lookup_queries.json entry for this config_id -> unchanged generic logic."""

    def setUp(self):
        self.patcher = patch.object(jdbc_connector, "_load_special_lookup_queries", return_value={})
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_full_load_is_unfiltered_and_generic(self):
        task = make_task(load_type="FULL", config_id=999, primary_key_cols="id")
        connector = make_connector(task)
        query = connector.build_probe_query()
        self.assertIn("SELECT id FROM", query)
        self.assertNotIn("WHERE", query)
        self.assertIn("FETCH FIRST 1 ROWS ONLY", query)

    def test_incremental_no_special_entry_uses_lookback_predicate(self):
        task = make_task(
            load_type="INCREMENTAL",
            incremental_column="Delta_Column_1",
            silver_last_sink_date="2026-08-26 10:00:00",
            lookback_hours=3,
            config_id=999,
            primary_key_cols="id",
        )
        connector = make_connector(task)
        query = connector.build_probe_query()
        self.assertIn("Delta_Column_1 >= '2026-08-26 07:00:00'", query)
        self.assertIn("FETCH FIRST 1 ROWS ONLY", query)


class TestSpecialCase(unittest.TestCase):
    """A matching special_lookup_queries.json entry (keyed by config_id) short-circuits the generic logic."""

    TEMPLATE = (
        "SELECT ard.{key_column} FROM DTL ard INNER JOIN "
        "(SELECT ID FROM HDR WHERE CREATION_TIME_STAMP >= TO_DATE('{lookback_timestamp}','YYYY-MM-DD HH24:MI:SS')) arh "
        "ON arh.ID = ard.HDRID FETCH NEXT 1 ROWS ONLY"
    )

    def setUp(self):
        self.patcher = patch.object(
            jdbc_connector,
            "_load_special_lookup_queries",
            return_value={"123": {"pipeline_name": "pfl_rdbms_ingestion", "lookup_query": self.TEMPLATE}},
        )
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_matching_config_id_uses_template(self):
        task = make_task(
            config_id=123,
            load_type="INCREMENTAL",
            incremental_column="CREATION_TIME_STAMP",
            silver_last_sink_date="2026-08-26 10:00:00",
            lookback_hours=3,
            primary_key_cols="id",
        )
        connector = make_connector(task)
        query = connector.build_probe_query()

        self.assertIn("SELECT ard.id FROM DTL ard INNER JOIN", query)
        self.assertIn("TO_DATE('2026-08-26 07:00:00','YYYY-MM-DD HH24:MI:SS')", query)
        self.assertIn("FETCH NEXT 1 ROWS ONLY", query)
        # required join structure preserved verbatim
        self.assertIn("ON arh.ID = ard.HDRID", query)

    def test_full_load_never_uses_special_template(self):
        task = make_task(config_id=123, load_type="FULL", primary_key_cols="id")
        connector = make_connector(task)
        query = connector.build_probe_query()
        self.assertNotIn("DTL ard INNER JOIN", query)

    def test_different_table_same_pipeline_falls_back_to_generic(self):
        """
        config_id uniquely identifies one table row. A different table in the
        SAME pipeline (config_id=456, not in the special map) must NOT pick up
        the config_id=123 template — proves the override is table-scoped, not
        pipeline- or config_master_id-scoped.
        """
        task = make_task(
            config_id=456,
            load_type="INCREMENTAL",
            incremental_column="Delta_Column_1",
            silver_last_sink_date="2026-08-26 10:00:00",
            primary_key_cols="id",
        )
        connector = make_connector(task)
        query = connector.build_probe_query()
        self.assertNotIn("DTL ard INNER JOIN", query)
        self.assertIn("Delta_Column_1 >=", query)

    def test_missing_lookup_query_key_raises(self):
        self.patcher.stop()
        with patch.object(
            jdbc_connector, "_load_special_lookup_queries", return_value={"123": {}}
        ):
            task = make_task(config_id=123, load_type="INCREMENTAL", primary_key_cols="id")
            connector = make_connector(task)
            with self.assertRaises(ValueError):
                connector.build_probe_query()
        self.patcher.start()


class TestLoadSpecialLookupQueries(unittest.TestCase):
    """The actual file-loading helper, independent of JdbcConnector."""

    def setUp(self):
        jdbc_connector._special_lookup_queries_cache = None

    def tearDown(self):
        jdbc_connector._special_lookup_queries_cache = None

    def test_missing_file_returns_empty_dict(self):
        from pathlib import Path
        with patch.object(jdbc_connector, "_SPECIAL_LOOKUP_CONFIG_PATH", Path("/no/such/file.json")):
            self.assertEqual(jdbc_connector._load_special_lookup_queries(), {})

    def test_invalid_json_raises_clear_error(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            f.write("{not valid json")
            path = f.name
        try:
            with patch.object(jdbc_connector, "_SPECIAL_LOOKUP_CONFIG_PATH", __import__("pathlib").Path(path)):
                with self.assertRaises(ValueError):
                    jdbc_connector._load_special_lookup_queries()
        finally:
            os.remove(path)


if __name__ == "__main__":
    unittest.main()
