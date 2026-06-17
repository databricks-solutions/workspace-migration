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
# Seed for the live LFC CDC (Tier-2, SQL Server + Change Tracking) migration test.
#
# SOURCE: an ingestion GATEWAY (gateway_definition → integration_test_sqlserver,
#         staging in integration_test_lfc_cdc.cdc) + a CDC ingestion pipeline
#         (ingestion_gateway_id → the gateway) ingesting dbo.orders. Change
#         Tracking must already be enabled on the SQL DB + dbo.orders (done via
#         Terraform/sqlcmd in the lab). The gateway auto-starts (continuous); we
#         leave the source side as-is.
# TARGET: the same connection + catalog + schema so the worker can recreate the
#         gateway + ingestion pipeline (create-only + validate, not started).
#
# We do NOT need the source ingestion pipeline to land data — the CDC migration
# is create-only (no data clone). Discovery just needs the gateway + ingestion
# pipeline to EXIST (and the gateway's staging volume, which exists on create).
#
# PREREQUISITE: the UC connection `integration_test_sqlserver` must exist on BOTH
# workspaces (it does, from the query-based stage). The seed fails fast if not.

import json

from databricks.sdk.service.pipelines import (
    IngestionGatewayPipelineDefinition,
    IngestionPipelineDefinition,
)

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CONNECTION_NAME = "integration_test_sqlserver"
_CATALOG = "integration_test_lfc_cdc"
_SCHEMA = "cdc"
_GATEWAY_NAME = "lfc_it_cdc_gateway"
_INGESTION_NAME = "lfc_it_cdc_orders"
_GW_STORAGE_VOLUME = "cdc_gateway_staging"   # logical; the platform names the real volume after the gateway id
_SQL_DB = "sqldb-wsm-test"
_SOURCE_TABLE = "orders"
_PRIMARY_KEY = "order_id"
_STAGING_VOLUME_PREFIX = "__databricks_ingestion_gateway_staging_data-"

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_src_wh = find_warehouse(_auth, use_source=True)
_tgt_wh = find_warehouse(_auth, use_source=False)
_src_client = _auth.source_client
_tgt_client = _auth.target_client


def _source_sql(sql: str) -> dict:
    res = execute_and_poll(_auth, _src_wh, sql, use_source=True)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"[seed-cdc] source SQL failed: {sql!r} -> {res}")
    return res


def _target_sql(sql: str) -> dict:
    res = execute_and_poll(_auth, _tgt_wh, sql, use_source=False)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"[seed-cdc] target SQL failed: {sql!r} -> {res}")
    return res


def _assert_connection(client, where: str) -> None:
    names = [c.name for c in client.connections.list()]
    if _CONNECTION_NAME not in names:
        raise RuntimeError(
            f"[seed-cdc] PREREQUISITE MISSING: connection {_CONNECTION_NAME!r} not on the "
            f"{where} workspace. Connections: {names}"
        )
    print(f"[seed-cdc] {where} connection {_CONNECTION_NAME!r} present")


def _delete_pipeline_by_name(client, name: str) -> None:
    for p in client.pipelines.list_pipelines():
        if getattr(p, "name", None) == name:
            client.pipelines.delete(p.pipeline_id)
            print(f"[seed-cdc] deleted existing pipeline {p.pipeline_id!r} ({name!r})")


# COMMAND ----------
# --- Verify connection on BOTH workspaces; create catalog + schema on BOTH ---
_assert_connection(_src_client, "source")
_assert_connection(_tgt_client, "target")
for _runner, _where in ((_source_sql, "source"), (_target_sql, "target")):
    _runner(f"CREATE CATALOG IF NOT EXISTS {_CATALOG}")
    _runner(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
    print(f"[seed-cdc] {_where} catalog+schema {_CATALOG}.{_SCHEMA} ensured")

# COMMAND ----------
# --- SOURCE: create the gateway (idempotent) ---
_delete_pipeline_by_name(_src_client, _INGESTION_NAME)   # ingestion first (references gateway)
_delete_pipeline_by_name(_src_client, _GATEWAY_NAME)

_gw_def = {
    "connection_name": _CONNECTION_NAME,
    "gateway_storage_catalog": _CATALOG,
    "gateway_storage_schema": _SCHEMA,
    "gateway_storage_name": _GW_STORAGE_VOLUME,
}
_gw = _src_client.pipelines.create(
    name=_GATEWAY_NAME,
    gateway_definition=IngestionGatewayPipelineDefinition.from_dict(_gw_def),
)
_gateway_id = _gw.pipeline_id  # type: ignore[union-attr]
_staging_volume_fqn = f"{_CATALOG}.{_SCHEMA}.{_STAGING_VOLUME_PREFIX}{_gateway_id}"
print(f"[seed-cdc] source gateway created: {_gateway_id!r} ({_GATEWAY_NAME!r})")
print(f"[seed-cdc] staging volume: {_staging_volume_fqn}")

# COMMAND ----------
# --- SOURCE: create the CDC ingestion pipeline referencing the gateway ---
_idef = {
    "ingestion_gateway_id": _gateway_id,
    "objects": [{"table": {
        "source_catalog": _SQL_DB, "source_schema": "dbo", "source_table": _SOURCE_TABLE,
        "destination_catalog": _CATALOG, "destination_schema": _SCHEMA, "destination_table": _SOURCE_TABLE,
        "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": [_PRIMARY_KEY]}}}],
}
_ing = _src_client.pipelines.create(
    name=_INGESTION_NAME, catalog=_CATALOG, schema=_SCHEMA,
    ingestion_definition=IngestionPipelineDefinition.from_dict(_idef),
)
_ingestion_id = _ing.pipeline_id  # type: ignore[union-attr]
print(f"[seed-cdc] source ingestion pipeline created: {_ingestion_id!r} ({_INGESTION_NAME!r})")

# COMMAND ----------
# Emit provable exit values for the assertion notebook.
dbutils.jobs.taskValues.set(key="has_cdc", value="true")  # noqa: F821
dbutils.jobs.taskValues.set(key="source_gateway_id", value=_gateway_id)  # noqa: F821
dbutils.jobs.taskValues.set(key="source_ingestion_id", value=_ingestion_id)  # noqa: F821
dbutils.jobs.taskValues.set(key="staging_volume_fqn", value=_staging_volume_fqn)  # noqa: F821
print(f"[seed-cdc] flags: gateway={_gateway_id} ingestion={_ingestion_id} staging={_staging_volume_fqn}")
dbutils.notebook.exit(  # noqa: F821
    json.dumps({"has_cdc": True, "source_gateway_id": _gateway_id,
                "source_ingestion_id": _ingestion_id, "staging_volume_fqn": _staging_volume_fqn})
)
