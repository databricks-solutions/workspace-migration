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
# Best-effort teardown for the live LFC query-based integration test.
#
# Each cleanup step is individually try/excepted — nothing here raises.
# Order: pipelines → views/tables → catalogs → share artifacts.
# The UC connection ``integration_test_sqlserver`` and the Azure SQL
# database itself are LEFT in place (shared across test runs).

import contextlib

from databricks.sdk import WorkspaceClient

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_lfc"
_SCHEMA = "sqlsrv"
_SOURCE_PIPELINE_NAME = "lfc_it_orders"
_TARGET_PIPELINE_NAME = "lfc_it_orders_migrated"
_SHARE_NAME = "cp_migration_share"
_CONSUMER_CATALOG = "cp_migration_share_consumer"

# COMMAND ----------
# --- Lazily build workspace clients ---


def _source_client() -> WorkspaceClient:
    return WorkspaceClient()


def _target_client() -> WorkspaceClient:
    return AuthManager(MigrationConfig.from_workspace_file(), dbutils).target_client  # noqa: F821


# COMMAND ----------
# --- Delete source pipeline lfc_it_orders ---
try:
    _src = _source_client()
    _src_pid: str | None = None
    for _p in _src.pipelines.list_pipelines():
        if getattr(_p, "name", None) == _SOURCE_PIPELINE_NAME:
            _src_pid = getattr(_p, "pipeline_id", None)
            break
    if _src_pid:
        _src.pipelines.delete(_src_pid)
        print(f"[teardown-lfc] deleted source pipeline {_src_pid!r} ({_SOURCE_PIPELINE_NAME!r})")
    else:
        print(f"[teardown-lfc] source pipeline {_SOURCE_PIPELINE_NAME!r} not found — nothing to delete")
except Exception as _exc:  # noqa: BLE001
    print(f"[teardown-lfc] could not delete source pipeline: {_exc}")

# COMMAND ----------
# --- Delete target recreated pipeline lfc_it_orders_migrated ---
try:
    _tgt = _target_client()
    _tgt_pid: str | None = None
    for _p in _tgt.pipelines.list_pipelines():
        if getattr(_p, "name", None) == _TARGET_PIPELINE_NAME:
            _tgt_pid = getattr(_p, "pipeline_id", None)
            break
    if _tgt_pid:
        _tgt.pipelines.delete(_tgt_pid)
        print(f"[teardown-lfc] deleted target pipeline {_tgt_pid!r} ({_TARGET_PIPELINE_NAME!r})")
    else:
        print(f"[teardown-lfc] target pipeline {_TARGET_PIPELINE_NAME!r} not found — nothing to delete")
except Exception as _exc:  # noqa: BLE001
    print(f"[teardown-lfc] could not delete target pipeline: {_exc}")

# COMMAND ----------
# --- Drop source-side tables / view, then catalog CASCADE ---

# The unified view sits at orders; orders_history and orders_incr are tables.
_src_drops = [
    f"DROP VIEW IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`orders`",
    f"DROP TABLE IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`orders_history`",
    f"DROP TABLE IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`orders_incr`",
]
for _ddl in _src_drops:
    with contextlib.suppress(Exception):
        spark.sql(_ddl)  # noqa: F821
        print(f"[teardown-lfc] source: {_ddl}")

with contextlib.suppress(Exception):
    spark.sql(f"DROP CATALOG IF EXISTS `{_CATALOG}` CASCADE")  # noqa: F821
    print(f"[teardown-lfc] dropped source catalog {_CATALOG}")

# COMMAND ----------
# --- Drop target-side tables / view, then catalog CASCADE ---
try:
    _auth = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _tgt_wh = find_warehouse(_auth, use_source=False)
    _tgt_drops = [
        f"DROP VIEW IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`orders`",
        f"DROP TABLE IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`orders_history`",
        f"DROP TABLE IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`orders_incr`",
        f"DROP CATALOG IF EXISTS `{_CATALOG}` CASCADE",
    ]
    for _ddl in _tgt_drops:
        with contextlib.suppress(Exception):
            execute_and_poll(_auth, _tgt_wh, _ddl, use_source=False)
            print(f"[teardown-lfc] target: {_ddl}")
except Exception as _exc:  # noqa: BLE001
    print(f"[teardown-lfc] could not drop target catalog/objects: {_exc}")

# COMMAND ----------
# --- Best-effort: drop migration share + consumer catalog on target ---
with contextlib.suppress(Exception):
    _auth2 = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _tgt_wh2 = find_warehouse(_auth2, use_source=False)
    execute_and_poll(_auth2, _tgt_wh2, f"DROP CATALOG IF EXISTS `{_CONSUMER_CATALOG}` CASCADE", use_source=False)
    print(f"[teardown-lfc] dropped target consumer catalog {_CONSUMER_CATALOG!r}")

with contextlib.suppress(Exception):
    _auth3 = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _auth3.source_client.shares.delete(_SHARE_NAME)
    print(f"[teardown-lfc] dropped migration share {_SHARE_NAME!r}")

# COMMAND ----------
# --- Clear migration_status and discovery_inventory tracking rows ---
_tracking_catalog = MigrationConfig.from_workspace_file().tracking_catalog  # type: ignore[attr-defined]
_tracking_schema = MigrationConfig.from_workspace_file().tracking_schema  # type: ignore[attr-defined]
_tracking_fqn = f"{_tracking_catalog}.{_tracking_schema}"

_lfc_names = [
    f"{_CATALOG}.{_SCHEMA}.orders_history",
    f"{_CATALOG}.{_SCHEMA}.orders",
    _SOURCE_PIPELINE_NAME,
]
for _name in _lfc_names:
    _safe = _name.replace("'", "''")
    with contextlib.suppress(Exception):
        spark.sql(f"DELETE FROM {_tracking_fqn}.migration_status WHERE object_name = '{_safe}'")  # noqa: F821
    with contextlib.suppress(Exception):
        spark.sql(f"DELETE FROM {_tracking_fqn}.discovery_inventory WHERE object_name = '{_safe}'")  # noqa: F821

# Also clear by catalog prefix so any extra rows (incr table etc.) are removed.
with contextlib.suppress(Exception):
    spark.sql(  # noqa: F821
        f"DELETE FROM {_tracking_fqn}.migration_status "
        f"WHERE object_name LIKE '{_CATALOG}.%'"
    )
with contextlib.suppress(Exception):
    spark.sql(  # noqa: F821
        f"DELETE FROM {_tracking_fqn}.discovery_inventory "
        f"WHERE object_name LIKE '{_CATALOG}.%'"
    )

print("[teardown-lfc] tracking rows cleared")
