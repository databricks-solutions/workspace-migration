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
#   A Salesforce UC connection named ``hs_salesforce`` must
#   already exist on BOTH the source AND target workspaces. Salesforce LFC
#   connections use OAuth (the connected app's first authorization must be an
#   admin — see reference_lfc_salesforce_connected_app_install), so they cannot
#   be created non-interactively here. This seed VERIFIES the connection exists
#   and fails fast with guidance if it does not. Everything else is idempotent.
#
# NOTE: the Salesforce object TableSpec uses source_schema = "objects" and
#   source_table = the SObject API name ("Account") — confirmed against the
#   Databricks Salesforce ingestion docs (the connector resolves SObjects under
#   the "objects" schema; omitting it fails analysis with SCHEMA_NOT_FOUND ``).

import json
import time

from databricks.sdk.service.pipelines import IngestionPipelineDefinition

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_CONNECTION_NAME = "hs_salesforce"
_CATALOG = "integration_test_lfc_sf"
_SCHEMA = "sf"
_PIPELINE_NAME = "lfc_it_sf_account"
_SF_SOURCE_SCHEMA = "objects"     # Salesforce LFC addresses SObjects under schema "objects"
_PRIMARY_KEY = "Id"
# Two objects in one pipeline to exercise BOTH SaaS paths in one migration:
#   account  → a cursor IS supplied in lfc_saas_cursor_columns → Tier-1 (clone +
#              row_filter + unified view).
#   contact  → NO cursor supplied → full-load (recreated pipeline reloads it,
#              no _history, no unified view; status lfc_view_skipped_no_cursor).
# (SObject API name, destination table)
_OBJECTS = [("Account", "account"), ("Contact", "contact")]
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

# SaaS table specs: NO query_based_connector_config, NO row_filter (the migration
# adds the row_filter on the operator-supplied cursor, per-table). scd_type SCD1, PK Id.
def _table_spec(sobject: str, dest: str) -> dict:
    t = {
        "source_table": sobject,
        "source_schema": _SF_SOURCE_SCHEMA,
        "destination_catalog": _CATALOG,
        "destination_schema": _SCHEMA,
        "destination_table": dest,
        "table_configuration": {"primary_keys": [_PRIMARY_KEY], "scd_type": "SCD_TYPE_1"},
    }
    return t


_idef = {
    "connection_name": _CONNECTION_NAME,
    "source_type": "SALESFORCE",
    "objects": [{"table": _table_spec(_obj, _dest)} for _obj, _dest in _OBJECTS],
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
# --- Trigger first update and wait for ALL destination tables to have rows ---
_src_client.pipelines.start_update(_pipeline_id)
print(f"[seed-lfc-sf] triggered update for {_pipeline_id!r}, polling up to {_POLL_TIMEOUT_SECONDS}s ...")

_counts: dict[str, int] = {dest: 0 for _, dest in _OBJECTS}
_elapsed = 0
while _elapsed < _POLL_TIMEOUT_SECONDS:
    time.sleep(_POLL_INTERVAL_SECONDS)
    _elapsed += _POLL_INTERVAL_SECONDS
    for _, _dest in _OBJECTS:
        if _counts[_dest] > 0:
            continue
        _fqn = f"`{_CATALOG}`.`{_SCHEMA}`.`{_dest}`"
        try:
            _counts[_dest] = spark.sql(f"SELECT COUNT(*) AS n FROM {_fqn}").collect()[0]["n"]  # noqa: F821
        except Exception as _exc:  # noqa: BLE001
            print(f"[seed-lfc-sf] {_dest} not yet queryable after {_elapsed}s: {_exc}")
    if all(_counts[d] > 0 for _, d in _OBJECTS):
        print(f"[seed-lfc-sf] all tables landed after {_elapsed}s: {_counts}")
        break

_missing = [d for _, d in _OBJECTS if _counts[d] == 0]
if _missing:
    raise RuntimeError(
        f"[seed-lfc-sf] tables {_missing} had 0 rows after {_POLL_TIMEOUT_SECONDS}s — "
        "pipeline did not land data in time; check the Salesforce LFC pipeline logs."
    )

# COMMAND ----------
# Emit provable exit value (notebook stdout is not retrievable via Jobs API).
# Per-table row counts so the assertion notebook can match target == source.
dbutils.jobs.taskValues.set(key="has_saas", value="true")  # noqa: F821
dbutils.jobs.taskValues.set(key="source_pipeline_id", value=_pipeline_id)  # noqa: F821
dbutils.jobs.taskValues.set(key="account_rows", value=str(_counts["account"]))  # noqa: F821
dbutils.jobs.taskValues.set(key="contact_rows", value=str(_counts["contact"]))  # noqa: F821
print(f"[seed-lfc-sf] flags: has_saas=true pipeline_id={_pipeline_id!r} counts={_counts}")
dbutils.notebook.exit(  # noqa: F821
    json.dumps({"has_saas": True, "source_pipeline_id": _pipeline_id, "counts": _counts})
)
