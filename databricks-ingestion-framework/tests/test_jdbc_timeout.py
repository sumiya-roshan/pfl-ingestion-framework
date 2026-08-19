"""
Tests for the source query timeout mechanism in JdbcConnector.

Run with (no pytest needed):
    python -m unittest databricks-ingestion-framework/tests/test_jdbc_timeout.py -v
"""
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingestion.connectors.jdbc_connector import JdbcConnector, _parse_timeout_to_seconds
from ingestion.utils.config_manager import IngestionTaskConfig


class FakeSourceSystem:
    def __init__(self):
        self.source_type = "POSTGRES"
        self.host = "localhost"
        self.port = 5432
        self.database_name = "db"
        self.driver_class = None
        self.secret_scope = "scope"
        self.secret_key_credentials = "creds"


class FakeIngestObj:
    def __init__(self, query_timeout=None):
        self.data_read_size = None
        self.query_timeout = query_timeout


class FakeSecrets:
    def get_credentials(self, scope, key):
        return "user", "pass"


class TestParseTimeoutToSeconds(unittest.TestCase):

    def test_none_returns_none(self):
        self.assertIsNone(_parse_timeout_to_seconds(None))

    def test_blank_returns_none(self):
        self.assertIsNone(_parse_timeout_to_seconds(""))
        self.assertIsNone(_parse_timeout_to_seconds("   "))

    def test_zero_returns_none(self):
        self.assertIsNone(_parse_timeout_to_seconds("00:00:00"))

    def test_full_hms(self):
        self.assertEqual(_parse_timeout_to_seconds("12:00:00"), 43200)

    def test_minutes_and_seconds(self):
        self.assertEqual(_parse_timeout_to_seconds("00:01:30"), 90)

    def test_invalid_format_raises(self):
        with self.assertRaises(ValueError):
            _parse_timeout_to_seconds("not-a-time")

    def test_wrong_segment_count_raises(self):
        with self.assertRaises(ValueError):
            _parse_timeout_to_seconds("12:00")


class TestReadOptionsIncludesQueryTimeout(unittest.TestCase):

    def _make_connector(self, query_timeout):
        return JdbcConnector(
            spark=None,
            source_system=FakeSourceSystem(),
            ingest_obj=FakeIngestObj(query_timeout=query_timeout),
            secrets=FakeSecrets(),
        )

    def test_timeout_configured_sets_query_timeout_option(self):
        connector = self._make_connector("01:00:00")
        opts = connector._read_options()
        self.assertEqual(opts["queryTimeout"], "3600")

    def test_no_timeout_configured_omits_option(self):
        connector = self._make_connector(None)
        opts = connector._read_options()
        self.assertNotIn("queryTimeout", opts)

    def test_zero_timeout_omits_option(self):
        connector = self._make_connector("00:00:00")
        opts = connector._read_options()
        self.assertNotIn("queryTimeout", opts)


class TestQueryTimeoutSurvivesTaskSerialization(unittest.TestCase):
    """
    lookup.py stamps query_timeout (from pipeline_lookup_config) onto each
    IngestionTaskConfig, then main.py rebuilds tasks from the JSON published
    via dbutils.jobs.taskValues — confirm the field round-trips through
    to_dict()/from_dict() so main.py -> JdbcConnector sees it.
    """

    def _make_task(self, **overrides):
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

    def test_query_timeout_round_trips_through_dict(self):
        task = self._make_task()
        task.query_timeout = "02:30:00"   # set by lookup.py after task is built

        rebuilt = IngestionTaskConfig.from_dict(task.to_dict())

        self.assertEqual(rebuilt.query_timeout, "02:30:00")

    def test_query_timeout_defaults_to_none(self):
        task = self._make_task()
        rebuilt = IngestionTaskConfig.from_dict(task.to_dict())
        self.assertIsNone(rebuilt.query_timeout)


if __name__ == "__main__":
    unittest.main()
