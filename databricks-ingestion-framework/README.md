# Databricks Ingestion Framework (Source → S3 Raw / Bronze)

A config-driven ingestion framework for pulling data from heterogeneous
sources (Postgres, MySQL, Oracle, SFTP, MongoDB) and landing it in S3 raw
and/or a Databricks bronze (Delta) layer — per-source configurable.

## How it works

1. **`connection_config`** — one row per physical source system (host, port,
   secret scope/keys). Credentials always live in Databricks secrets, never
   in the config table itself.
2. **`source_config`** — one row per object to ingest (a table, an SFTP file
   pattern, a Mongo collection). Defines load type (FULL/INCREMENTAL) and
   target (S3 / BRONZE / BOTH).
3. **`ingestion_audit_log`** — one row per run. Tracks status, record counts,
   and the watermark used for the next incremental run.
4. **Orchestrator** (`src/ingestion/orchestrator.py`) — given a `source_id`,
   resolves config → picks the right connector → extracts → writes → logs.
5. **Notebooks** wrap the orchestrator for Databricks Jobs:
   - `00_setup_config_tables.py` — one-time DDL setup
   - `01_run_ingestion.py` — runs a single `source_id` (job task unit)
   - `02_run_all_sources.py` — ad-hoc fan-out notebook (dev/testing)
   - `list_active_sources.py` + `jobs/ingestion_job.json` — production
     pattern: a Databricks Jobs **for-each task** fans out one run per
     active source, with per-source retries and concurrency control.

## Project layout

```
databricks-ingestion-framework/
├── config/ddl/                  # connection_config, source_config, audit_log DDL
├── src/ingestion/
│   ├── config_manager.py        # reads config tables into typed objects
│   ├── audit.py                 # run logging + watermark resolution
│   ├── orchestrator.py          # main driver
│   ├── connectors/
│   │   ├── base_connector.py    # interface every connector implements
│   │   ├── jdbc_connector.py    # Postgres / MySQL / Oracle
│   │   ├── sftp_connector.py    # SFTP (paramiko)
│   │   ├── mongo_connector.py   # MongoDB (spark connector)
│   │   └── factory.py           # source_type -> connector class
│   ├── writers/
│   │   ├── s3_writer.py         # S3 raw layer, partitioned by ingest_date
│   │   └── bronze_writer.py     # Delta bronze table, append/merge/overwrite
│   └── utils/
│       ├── logger.py
│       └── secrets.py           # wraps dbutils.secrets
├── notebooks/                   # entry points, deployed via Databricks Repos/Bundles
├── jobs/ingestion_job.json      # Databricks Jobs API definition (for-each pattern)
└── requirements.txt
```

## Setup

1. **Deploy the repo** to Databricks via Repos or Databricks Asset Bundles.
2. **Attach cluster libraries** (Maven):
   - `org.postgresql:postgresql:42.7.3`
   - `com.mysql:mysql-connector-j:8.4.0`
   - `com.oracle.database.jdbc:ojdbc11:23.4.0.24.05`
   - `org.mongodb.spark:mongo-spark-connector_2.12:10.3.0`
   - `paramiko` via `%pip install paramiko` (already in `01_run_ingestion.py`)
3. **Create a Databricks secret scope** and add username/password (or SSH key)
   per connection, e.g.:
   ```bash
   databricks secrets create-scope ingestion-secrets
   databricks secrets put-secret ingestion-secrets pg_sales_user
   databricks secrets put-secret ingestion-secrets pg_sales_password
   ```
4. Run `notebooks/00_setup_config_tables.py` once to create the metadata
   schema/tables.
5. Insert rows into `connection_config` and `source_config` for each source
   (examples commented at the bottom of `00_setup_config_tables.py`).
6. Deploy `jobs/ingestion_job.json` (via CLI, UI import, or a Databricks
   Asset Bundle) and update `node_type_id` / notebook paths for your workspace.

## Adding a new source type

1. Implement a new class in `src/ingestion/connectors/` extending
   `BaseConnector`.
2. Register it in `connectors/factory.py`'s `_CONNECTOR_MAP`.
3. Add any new columns needed in `source_config` DDL.
No other code changes required — the orchestrator and notebooks are
source-type agnostic.

## Notes / things to adapt for your environment

- `CONFIG_CATALOG_SCHEMA` in `config_manager.py` — set to your actual Unity
  Catalog catalog.schema.
- `STAGING_ROOT` in `sftp_connector.py` — point at a Unity Catalog Volume
  path instead of `/tmp` for production durability.
- The JDBC incremental watermark aggregation (`MAX(incremental_column)`) adds
  an extra pass over the extracted batch. For very large tables, consider
  pushing this down as a separate lightweight `SELECT MAX(...)` query instead.
- `bronze_write_mode = 'merge'` requires `bronze_merge_keys` to be set in
  `source_config`.
- This framework does not include schema drift handling, data quality
  checks, or PII masking — layer these in as your bronze validation step
  before promoting to silver.
