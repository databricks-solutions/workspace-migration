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
# Seed for the live LFC SaaS (Salesforce, Tier-1 row_filter) migration test.
#
# SOURCE:  an LFC Salesforce pipeline ``lfc_it_sf_account`` that ingests the
#          Salesforce ``Account`` object into
#          ``integration_test_lfc_sf.sf.account`` (SCD1, PK Id). Account carries
#          ``SystemModstamp`` — the cursor the migration's resolver will pick.
#          The pipeline is started and polled until the destination table has
#          rows (proves the cursor boundary + clone will work).
# TARGET:  the same-named UC connection + catalog + schema so the worker can
#          create the recreated pipeline + unified view.
#
# PREREQUISITE — interactive, done once OUTSIDE this notebook:
#   A Salesforce UC connection named ``integration_test_salesforce`` must
#   already exist on BOTH the source AND target workspaces. Salesforce LFC
#   connections use OAuth (the connected app's first authorization must be an
#   admin — see reference_lfc_salesforce_connected_app_install), so they cannot
#   be created non-interactively here. This seed VERIFIES the connection exists
#   and fails fast with guidance if it does not. Everything else is idempotent.
#
# NOTE (confirm live): the Salesforce object TableSpec uses source_table =
#   the SObject API name ("Account"); source_catalog/source_schema are omitted.
#   If a given workspace's connector requires a source_schema, set _SF_SOURCE_SCHEMA.

import json
import time

from databricks.sdk.service.pipelines import IngestionPipelineDefinition

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_CONNECTION_NAME = "integration_test_salesforce"
_CATALOG = "integration_test_lfc_sf"
_SCHEMA = "sf"
_PIPELINE_NAME = "lfc_it_sf_account"
_SF_SOURCE_OBJECT = "Account"     # Salesforce SObject API name
_SF_SOURCE_SCHEMA = None          # set if your connector requires it (else omitted)
_DEST_TABLE = "account"
_PRIMARY_KEY = "Id"
_POLL_TIMEOUT_SECONDS = 600
_POLL_INTERVAL_SECONDS = 15

# ---------------------------------------------------------------------------
# Bootstrap config / auth
# ---------------------------------------------------------------------------
_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821

_src_wh = find_warehouse(_auth, use_source=True)
_tgt_wh = find_warehouse(_auth, use_source=False)

_src_client = _auth.source_client
_tgt_client = _auth.target_client


def _source_sql(sql: str) -> dict:
    res = execute_and_poll(_auth, _src_wh, sql, use_source=True)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"[seed-lfc-sf] source SQL failed: {sql!r} -> {res}")
    return res


def _target_sql(sql: str) -> dict:
    res = execute_and_poll(_auth, _tgt_wh, sql, use_source=False)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"[seed-lfc-sf] target SQL failed: {sql!r} -> {res}")
    return res


def _assert_connection(client, where: str) -> None:
    names = [c.name for c in client.connections.list()]
    if _CONNECTION_NAME not in names:
        raise RuntimeError(
            f"[seed-lfc-sf] PREREQUISITE MISSING: Salesforce UC connection "
            f"{_CONNECTION_NAME!r} not found on the {where} workspace. Create it "
            f"(OAuth, admin-authorized) before running this test. Connections on "
            f"{where}: {names}"
        )
    print(f"[seed-lfc-sf] {where} connection {_CONNECTION_NAME!r} present")


# COMMAND ----------
# --- Verify the OAuth Salesforce connection exists on BOTH workspaces ---
_assert_connection(_src_client, "source")
_assert_connection(_tgt_client, "target")

# COMMAND ----------
# --- SOURCE + TARGET: catalog + schema ---
for _runner, _where in ((_source_sql, "source"), (_target_sql, "target")):
    _runner(f"CREATE CATALOG IF NOT EXISTS {_CATALOG}")
    _runner(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
    print(f"[seed-lfc-sf] {_where} catalog+schema {_CATALOG}.{_SCHEMA} ensured")

# COMMAND ----------
# --- SOURCE: create (or recreate) the Salesforce LFC pipeline ---
_existing_pid = None
for _p in _src_client.pipelines.list_pipelines():
    if getattr(_p, "name", None) == _PIPELINE_NAME:
        _existing_pid = getattr(_p, "pipeline_id", None)
        break
if _existing_pid:
    _src_client.pipelines.delete(_existing_pid)
    print(f"[seed-lfc-sf] deleted existing source pipeline {_existing_pid!r} ({_PIPELINE_NAME!r})")

# SaaS table spec: NO query_based_connector_config, NO row_filter (the migration
# adds the row_filter on the resolved cursor). scd_type SCD1, PK Id.
_table = {
    "source_table": _SF_SOURCE_OBJECT,
    "destination_catalog": _CATALOG,
    "destination_schema": _SCHEMA,
    "destination_table": _DEST_TABLE,
    "table_configuration": {"primary_keys": [_PRIMARY_KEY], "scd_type": "SCD_TYPE_1"},
}
if _SF_SOURCE_SCHEMA:
    _table["source_schema"] = _SF_SOURCE_SCHEMA

_idef = {
    "connection_name": _CONNECTION_NAME,
    "source_type": "SALESFORCE",
    "objects": [{"table": _table}],
}

_created = _src_client.pipelines.create(
    name=_PIPELINE_NAME,
    catalog=_CATALOG,
    schema=_SCHEMA,
    channel="PREVIEW",
    ingestion_definition=IngestionPipelineDefinition.from_dict(_idef),
)
_pipeline_id = _created.pipeline_id  # type: ignore[union-attr]
print(f"[seed-lfc-sf] source pipeline created: {_pipeline_id!r} ({_PIPELINE_NAME!r})")

# COMMAND ----------
# --- Trigger first update and wait for the account table to have rows ---
_src_client.pipelines.start_update(_pipeline_id)
print(f"[seed-lfc-sf] triggered update for {_pipeline_id!r}, polling up to {_POLL_TIMEOUT_SECONDS}s ...")

_dest_fqn = f"`{_CATALOG}`.`{_SCHEMA}`.`{_DEST_TABLE}`"
_account_rows = 0
_elapsed = 0
while _elapsed < _POLL_TIMEOUT_SECONDS:
    time.sleep(_POLL_INTERVAL_SECONDS)
    _elapsed += _POLL_INTERVAL_SECONDS
    try:
        _cnt_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {_dest_fqn}").collect()  # noqa: F821
        _account_rows = _cnt_rows[0]["n"]
        if _account_rows > 0:
            print(f"[seed-lfc-sf] {_dest_fqn} has {_account_rows} rows after {_elapsed}s — pipeline landed data")
            break
    except Exception as _exc:  # noqa: BLE001
        print(f"[seed-lfc-sf] table not yet queryable after {_elapsed}s: {_exc}")

if _account_rows == 0:
    raise RuntimeError(
        f"[seed-lfc-sf] {_dest_fqn} has 0 rows after {_POLL_TIMEOUT_SECONDS}s — "
        "pipeline did not land data in time; check the Salesforce LFC pipeline logs."
    )

# COMMAND ----------
# Emit provable exit value (notebook stdout is not retrievable via Jobs API).
dbutils.jobs.taskValues.set(key="has_saas", value="true")  # noqa: F821
dbutils.jobs.taskValues.set(key="source_pipeline_id", value=_pipeline_id)  # noqa: F821
dbutils.jobs.taskValues.set(key="account_rows", value=str(_account_rows))  # noqa: F821
print(f"[seed-lfc-sf] flags: has_saas=true pipeline_id={_pipeline_id!r} account_rows={_account_rows}")
dbutils.notebook.exit(  # noqa: F821
    json.dumps({"has_saas": True, "source_pipeline_id": _pipeline_id, "account_rows": _account_rows})
)
