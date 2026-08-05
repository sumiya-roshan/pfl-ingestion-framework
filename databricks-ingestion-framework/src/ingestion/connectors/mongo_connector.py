"""
MongoDB connector using the MongoDB Spark Connector (org.mongodb.spark).
The cluster must have the connector JAR attached
(Maven: org.mongodb.spark:mongo-spark-connector_2.12:<version>).

Field mapping from new config tables
-------------------------------------
  config_source_system
    .connection_uri           → used verbatim when set (Atlas SRV strings, etc.)
    .nosql_collection_name    → default collection (overridable per ingestion object)
    .nosql_replica_set        → replica-set name appended to URI options when set
    .secret_scope             → Databricks secret scope
    .secret_key_credentials   → key holding JSON {"username":..,"password":..}
    .host / .port             → used to build URI when connection_uri is NULL

  ingestion_config
    .source_object_name       → overrides nosql_collection_name when different
    .source_schema            → maps to MongoDB database name (overrides database_name)
    .custom_query             → raw aggregation pipeline JSON string (optional)
    .source_filter            → simple field=value filter injected into $match stage
    .load_type                → FULL | INCREMENTAL
    .incremental_column       → field used for incremental $match (e.g. _id, updatedAt)
    .incremental_end_value    → initial value for first run

URI construction priority
--------------------------
1. connection_uri from config_source_system  (verbatim, recommended for Atlas)
2. Built: mongodb://<username>:<password>@<host>:<port>/?<options>
"""
from typing import Optional, Tuple
from urllib.parse import quote_plus

from pyspark.sql import DataFrame

from .base_connector import BaseConnector


class MongoConnector(BaseConnector):

    # ── Private helpers ───────────────────────────────────────────────────────

    def _build_connection_uri(self) -> str:
        """
        Construct a MongoDB connection URI.
        If connection_uri is set in config_source_system, it is treated as a
        template that may contain the literal placeholders {username} and
        {password}; they are substituted with the credential values from the
        secret.  If no placeholders are present, credentials are prepended in
        the standard  mongodb://user:pass@  fashion.
        """
        ss = self.source_system
        username, password = self.secrets.get_credentials(
            ss.secret_scope, ss.secret_key_credentials
        )
        enc_user = quote_plus(username)
        enc_pass = quote_plus(password)

        if ss.connection_uri:
            uri = ss.connection_uri
            if "{username}" in uri or "{password}" in uri:
                return uri.format(username=enc_user, password=enc_pass)
            # Assume the URI already contains credentials (e.g. injected at provisioning)
            return uri

        # Build a standard URI from host/port
        host = ss.host or "localhost"
        port = ss.port or 27017
        options = "retryWrites=true&w=majority"
        if ss.nosql_replica_set:
            options += f"&replicaSet={ss.nosql_replica_set}"
        return f"mongodb://{enc_user}:{enc_pass}@{host}:{port}/?{options}"

    def _resolve_database(self) -> str:
        """
        MongoDB database name.
        source_schema from ingestion_config overrides database_name from source_system.
        """
        return self.ingest_obj.source_schema or self.source_system.database_name or ""

    def _resolve_collection(self) -> str:
        """
        MongoDB collection name.
        source_object_name from ingestion_config overrides nosql_collection_name
        from source_system so different objects can target different collections.
        """
        io = self.ingest_obj
        ss = self.source_system
        # Use source_object_name unless it is the generic placeholder token
        if io.source_object_name and io.source_object_name != "__default__":
            return io.source_object_name
        return ss.nosql_collection_name or ""

    def _build_aggregation_pipeline(self, watermark_start: Optional[str]) -> Optional[str]:
        """
        Build a MongoDB aggregation pipeline JSON string injected via
        the Spark connector's aggregation.pipeline option.

        Priority:
          1. custom_query  — used verbatim if set (must be a valid pipeline JSON array)
          2. Constructed from incremental predicate + source_filter
        """
        io = self.ingest_obj

        if io.custom_query:
            return io.custom_query

        stages = []

        # Incremental $match stage
        if io.load_type == "INCREMENTAL" and io.incremental_column:
            wm = watermark_start if watermark_start is not None else io.incremental_end_value
            if wm is not None:
                stages.append(
                    f'{{\"$match\": {{\"{io.incremental_column}\": {{\"$gt\": \"{wm}\"}}}}}}'
                )

        # Additional filter from source_filter (expected as a valid $match expression JSON)
        if io.source_filter:
            stages.append(f'{{\"$match\": {io.source_filter}}}')

        if stages:
            return "[" + ", ".join(stages) + "]"
        return None

    # ── Public extract ────────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        uri        = self._build_connection_uri()
        database   = self._resolve_database()
        collection = self._resolve_collection()
        pipeline   = self._build_aggregation_pipeline(watermark_start)

        reader = (
            self.spark.read.format("mongodb")
            .option("connection.uri", uri)
            .option("database",   database)
            .option("collection", collection)
        )

        if pipeline:
            reader = reader.option("aggregation.pipeline", pipeline)

        df = reader.load()

        max_watermark: Optional[str] = None
        io = self.ingest_obj
        if io.load_type == "INCREMENTAL" and io.incremental_column:
            max_row = df.agg({io.incremental_column: "max"}).collect()
            if max_row and max_row[0][0] is not None:
                max_watermark = str(max_row[0][0])

        return df, max_watermark
