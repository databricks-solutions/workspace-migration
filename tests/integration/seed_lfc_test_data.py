# Databricks notebook source

# COMMAND ----------

import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass

# COMMAND ----------
# Seed for the live LFC query-based migration integration test.
#
# SOURCE:  a UC connection (TYPE sqlserver) → an LFC query-based pipeline
#          ``lfc_it_orders`` that ingests ``dbo.orders`` from Azure SQL
#          (sqlsrv-wsm-test-ne.database.windows.net) into
#          ``integration_test_lfc.sqlsrv.orders`` (SCD1, cursor placed_at).
#          The pipeline is started and polled until the destination table
#          has rows — that proves the worker can compute a boundary and
#          clone it.
# TARGET:  the same UC connection + catalog + schema so the worker can
#          create the recreated pipeline and unified view cold.
#
# Everything is idempotent: if the pipeline already exists it is deleted
# and recreated so each run starts from a known state.

import json
import time

from databricks.sdk.service.pipelines import IngestionPipelineDefinition

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SQL_HOST = "sqlsrv-wsm-test-ne.database.windows.net"
_SQL_PORT = "1433"
_SQL_USER = "wsmtestadmin"
_SQL_DB = "sqldb-wsm-test"
_SQL_SOURCE_TABLE = "dbo.orders"
_CONNECTION_NAME = "integration_test_sqlserver"
_CATALOG = "integration_test_lfc"
_SCHEMA = "sqlsrv"
_PIPELINE_NAME = "lfc_it_orders"
_DEST_TABLE = "orders"
_CURSOR_COLUMN = "placed_at"
_PRIMARY_KEY = "order_id"
_POLL_TIMEOUT_SECONDS = 360
_POLL_INTERVAL_SECONDS = 10

# ---------------------------------------------------------------------------
# Bootstrap config / auth
# ---------------------------------------------------------------------------
_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821

_src_wh = find_warehouse(_auth, use_source=True)
_tgt_wh = find_warehouse(_auth, use_source=False)

# ---------------------------------------------------------------------------
# Read SQL password from secret scope
# ---------------------------------------------------------------------------
_sql_password = dbutils.secrets.get(scope="migration", key="sqltest-password")  # noqa: F821

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source_sql(sql: str) -> dict:
    res = execute_and_poll(_auth, _src_wh, sql, use_source=True)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"[seed-lfc] source SQL failed: {sql!r} -> {res}")
    return res


def _target_sql(sql: str) -> dict:
    res = execute_and_poll(_auth, _tgt_wh, sql, use_source=False)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"[seed-lfc] target SQL failed: {sql!r} -> {res}")
    return res


# COMMAND ----------
# --- SOURCE: UC connection + catalog + schema ---
_conn_ddl = (
    f"CREATE CONNECTION IF NOT EXISTS {_CONNECTION_NAME} "
    f"TYPE sqlserver "
    f"OPTIONS ("
    f"host '{_SQL_HOST}', "
    f"port '{_SQL_PORT}', "
    f"user '{_SQL_USER}', "
    f"password '{_sql_password}'"
    f")"
)
_source_sql(_conn_ddl)
print(f"[seed-lfc] source connection {_CONNECTION_NAME!r} ensured")

_source_sql(f"CREATE CATALOG IF NOT EXISTS {_CATALOG}")
_source_sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
print(f"[seed-lfc] source catalog+schema {_CATALOG}.{_SCHEMA} ensured")

# COMMAND ----------
# --- TARGET: same UC connection + catalog + schema ---
# The recreated pipeline on target needs a connection with the same name.
_target_sql(_conn_ddl)
print(f"[seed-lfc] target connection {_CONNECTION_NAME!r} ensured")

_target_sql(f"CREATE CATALOG IF NOT EXISTS {_CATALOG}")
_target_sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
print(f"[seed-lfc] target catalog+schema {_CATALOG}.{_SCHEMA} ensured")

# COMMAND ----------
# --- SOURCE: create (or recreate) the query-based LFC pipeline ---
# Idempotent: delete existing pipeline by name before creating so each
# re-run starts from a clean state.

_src_client = _auth.source_client

# Find and delete any existing pipeline named _PIPELINE_NAME
_existing_pid: str | None = None
for _p in _src_client.pipelines.list_pipelines():
    if getattr(_p, "name", None) == _PIPELINE_NAME:
        _existing_pid = getattr(_p, "pipeline_id", None)
        break

if _existing_pid:
    _src_client.pipelines.delete(_existing_pid)
    print(f"[seed-lfc] deleted existing source pipeline {_existing_pid!r} ({_PIPELINE_NAME!r})")

# Build the ingestion_definition dict.
# cursor MUST be nested under query_based_connector_config.cursor_columns (plural).
_idef = {
    "connection_name": _CONNECTION_NAME,
    "source_type": "SQLSERVER",
    "objects": [
        {
            "table": {
                "source_catalog": _SQL_DB,
                "source_schema": "dbo",
                "source_table": "orders",
                "destination_catalog": _CATALOG,
                "destination_schema": _SCHEMA,
                "destination_table": _DEST_TABLE,
                "table_configuration": {
                    "primary_keys": [_PRIMARY_KEY],
                    "scd_type": "SCD_TYPE_1",
                    "query_based_connector_config": {
                        "cursor_columns": [_CURSOR_COLUMN],
                    },
                },
            }
        }
    ],
}

_created = _src_client.pipelines.create(
    name=_PIPELINE_NAME,
    catalog=_CATALOG,
    schema=_SCHEMA,
    channel="PREVIEW",
    ingestion_definition=IngestionPipelineDefinition.from_dict(_idef),
)
_pipeline_id: str = _created.pipeline_id  # type: ignore[union-attr]
print(f"[seed-lfc] source pipeline created: {_pipeline_id!r} ({_PIPELINE_NAME!r})")

# COMMAND ----------
# --- Trigger first update and wait for orders table to have rows ---
_src_client.pipelines.start_update(_pipeline_id)
print(f"[seed-lfc] triggered pipeline update for {_pipeline_id!r}, polling up to {_POLL_TIMEOUT_SECONDS}s ...")

_dest_fqn = f"`{_CATALOG}`.`{_SCHEMA}`.`{_DEST_TABLE}`"
_orders_rows = 0
_elapsed = 0
while _elapsed < _POLL_TIMEOUT_SECONDS:
    time.sleep(_POLL_INTERVAL_SECONDS)
    _elapsed += _POLL_INTERVAL_SECONDS
    try:
        _count_res = execute_and_poll(
            _auth, _src_wh,
            f"SELECT COUNT(*) FROM {_dest_fqn}",
            use_source=True,
        )
        # execute_and_poll does not return rows; use spark directly on source
        # (seed runs on the source workspace driver).
        _cnt_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {_dest_fqn}").collect()  # noqa: F821
        _orders_rows = _cnt_rows[0]["n"]
        if _orders_rows > 0:
            print(f"[seed-lfc] {_dest_fqn} has {_orders_rows} rows after {_elapsed}s — pipeline landed data")
            break
    except Exception as _exc:  # noqa: BLE001
        print(f"[seed-lfc] table not yet queryable after {_elapsed}s: {_exc}")

if _orders_rows == 0:
    raise RuntimeError(
        f"[seed-lfc] {_dest_fqn} has 0 rows after {_POLL_TIMEOUT_SECONDS}s — "
        "pipeline did not land data in time; check the LFC pipeline logs."
    )

# COMMAND ----------
# Emit provable exit value (notebook stdout is not retrievable via Jobs API).
dbutils.jobs.taskValues.set(key="has_query_based", value="true")  # noqa: F821
dbutils.jobs.taskValues.set(key="source_pipeline_id", value=_pipeline_id)  # noqa: F821
dbutils.jobs.taskValues.set(key="orders_rows", value=str(_orders_rows))  # noqa: F821
print(f"[seed-lfc] flags: has_query_based=true pipeline_id={_pipeline_id!r} orders_rows={_orders_rows}")
dbutils.notebook.exit(  # noqa: F821
    json.dumps({"has_query_based": True, "source_pipeline_id": _pipeline_id, "orders_rows": _orders_rows})
)
