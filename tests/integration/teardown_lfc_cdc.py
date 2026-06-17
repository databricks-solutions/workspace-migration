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
# Best-effort teardown for the live LFC CDC integration test. Nothing here raises.
# The UC connection + the SQL server (and its Change Tracking) are LEFT in place.

import contextlib

from databricks.sdk import WorkspaceClient

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_lfc_cdc"
_SOURCE_PIPELINES = ["lfc_it_cdc_orders", "lfc_it_cdc_gateway"]            # ingestion first, then gateway
_TARGET_PIPELINES = ["lfc_it_cdc_orders_migrated", "cdc_gateway_staging_migrated"]


def _source_client() -> WorkspaceClient:
    return WorkspaceClient()


def _target_client() -> WorkspaceClient:
    return AuthManager(MigrationConfig.from_workspace_file(), dbutils).target_client  # noqa: F821


# COMMAND ----------
# --- Delete source + target pipelines (ingestion before gateway) ---
for _client_fn, _names, _where in (
    (_source_client, _SOURCE_PIPELINES, "source"),
    (_target_client, _TARGET_PIPELINES, "target"),
):
    try:
        _c = _client_fn()
        _by_name = {getattr(p, "name", None): p.pipeline_id for p in _c.pipelines.list_pipelines()}
        for _n in _names:
            if _n in _by_name:
                with contextlib.suppress(Exception):
                    _c.pipelines.delete(_by_name[_n])
                    print(f"[teardown-cdc] deleted {_where} pipeline {_by_name[_n]!r} ({_n!r})")
    except Exception as _exc:  # noqa: BLE001
        print(f"[teardown-cdc] could not list/delete {_where} pipelines: {_exc}")

# COMMAND ----------
# --- Drop the CDC catalog (CASCADE) on source + target ---
with contextlib.suppress(Exception):
    spark.sql(f"DROP CATALOG IF EXISTS `{_CATALOG}` CASCADE")  # noqa: F821
    print(f"[teardown-cdc] dropped source catalog {_CATALOG}")
try:
    _auth = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _tgt_wh = find_warehouse(_auth, use_source=False)
    with contextlib.suppress(Exception):
        execute_and_poll(_auth, _tgt_wh, f"DROP CATALOG IF EXISTS `{_CATALOG}` CASCADE", use_source=False)
        print(f"[teardown-cdc] dropped target catalog {_CATALOG}")
except Exception as _exc:  # noqa: BLE001
    print(f"[teardown-cdc] could not drop target catalog: {_exc}")

# COMMAND ----------
# --- Clear tracking rows ---
_cfg = MigrationConfig.from_workspace_file()
_tracking_fqn = f"{_cfg.tracking_catalog}.{_cfg.tracking_schema}"  # type: ignore[attr-defined]
for _tbl in ("migration_status", "discovery_inventory"):
    with contextlib.suppress(Exception):
        spark.sql(f"DELETE FROM {_tracking_fqn}.{_tbl} WHERE object_name LIKE '{_CATALOG}.%'")  # noqa: F821
    for _n in _SOURCE_PIPELINES:
        with contextlib.suppress(Exception):
            spark.sql(f"DELETE FROM {_tracking_fqn}.{_tbl} WHERE object_name = '{_n}'")  # noqa: F821

print("[teardown-cdc] tracking rows cleared")
