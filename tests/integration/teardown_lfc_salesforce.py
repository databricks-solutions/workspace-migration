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
# Best-effort teardown for the live LFC Salesforce integration test.
#
# Each step is individually try/excepted — nothing here raises. Order:
# pipelines → views/tables → catalogs → share artifacts → tracking rows.
# The OAuth Salesforce UC connection ``hs_salesforce`` is LEFT in
# place (interactive/admin to recreate; shared across runs), as is the SF org.

import contextlib

from databricks.sdk import WorkspaceClient

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_lfc_sf"
_SCHEMA = "sf"
_SOURCE_PIPELINE_NAME = "lfc_it_sf_account"
_TARGET_PIPELINE_NAME = "lfc_it_sf_account_migrated"
_SHARE_NAME = "cp_migration_share"
_CONSUMER_CATALOG = "cp_migration_share_consumer"


def _source_client() -> WorkspaceClient:
    return WorkspaceClient()


def _target_client() -> WorkspaceClient:
    return AuthManager(MigrationConfig.from_workspace_file(), dbutils).target_client  # noqa: F821


# COMMAND ----------
# --- Delete source + target pipelines ---
for _client_fn, _name, _where in (
    (_source_client, _SOURCE_PIPELINE_NAME, "source"),
    (_target_client, _TARGET_PIPELINE_NAME, "target"),
):
    try:
        _c = _client_fn()
        _pid = None
        for _p in _c.pipelines.list_pipelines():
            if getattr(_p, "name", None) == _name:
                _pid = getattr(_p, "pipeline_id", None)
                break
        if _pid:
            _c.pipelines.delete(_pid)
            print(f"[teardown-lfc-sf] deleted {_where} pipeline {_pid!r} ({_name!r})")
        else:
            print(f"[teardown-lfc-sf] {_where} pipeline {_name!r} not found")
    except Exception as _exc:  # noqa: BLE001
        print(f"[teardown-lfc-sf] could not delete {_where} pipeline: {_exc}")

# COMMAND ----------
# --- Drop source-side view/tables, then catalog CASCADE ---
_src_drops = [
    f"DROP VIEW IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`account`",
    f"DROP TABLE IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`account_history`",
    f"DROP TABLE IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`account_incr`",
]
for _ddl in _src_drops:
    with contextlib.suppress(Exception):
        spark.sql(_ddl)  # noqa: F821
        print(f"[teardown-lfc-sf] source: {_ddl}")

with contextlib.suppress(Exception):
    spark.sql(f"DROP CATALOG IF EXISTS `{_CATALOG}` CASCADE")  # noqa: F821
    print(f"[teardown-lfc-sf] dropped source catalog {_CATALOG}")

# COMMAND ----------
# --- Drop target-side view/tables, then catalog CASCADE ---
try:
    _auth = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _tgt_wh = find_warehouse(_auth, use_source=False)
    for _ddl in (
        f"DROP VIEW IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`account`",
        f"DROP TABLE IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`account_history`",
        f"DROP TABLE IF EXISTS `{_CATALOG}`.`{_SCHEMA}`.`account_incr`",
        f"DROP CATALOG IF EXISTS `{_CATALOG}` CASCADE",
    ):
        with contextlib.suppress(Exception):
            execute_and_poll(_auth, _tgt_wh, _ddl, use_source=False)
            print(f"[teardown-lfc-sf] target: {_ddl}")
except Exception as _exc:  # noqa: BLE001
    print(f"[teardown-lfc-sf] could not drop target catalog/objects: {_exc}")

# COMMAND ----------
# --- Best-effort: drop migration share + consumer catalog on target ---
with contextlib.suppress(Exception):
    _auth2 = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _tgt_wh2 = find_warehouse(_auth2, use_source=False)
    execute_and_poll(_auth2, _tgt_wh2, f"DROP CATALOG IF EXISTS `{_CONSUMER_CATALOG}` CASCADE", use_source=False)
    print(f"[teardown-lfc-sf] dropped target consumer catalog {_CONSUMER_CATALOG!r}")

with contextlib.suppress(Exception):
    _auth3 = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _auth3.source_client.shares.delete(_SHARE_NAME)
    print(f"[teardown-lfc-sf] dropped migration share {_SHARE_NAME!r}")

# COMMAND ----------
# --- Clear tracking rows ---
_cfg = MigrationConfig.from_workspace_file()
_tracking_fqn = f"{_cfg.tracking_catalog}.{_cfg.tracking_schema}"  # type: ignore[attr-defined]

for _tbl in ("migration_status", "discovery_inventory"):
    with contextlib.suppress(Exception):
        spark.sql(f"DELETE FROM {_tracking_fqn}.{_tbl} WHERE object_name LIKE '{_CATALOG}.%'")  # noqa: F821
    with contextlib.suppress(Exception):
        spark.sql(f"DELETE FROM {_tracking_fqn}.{_tbl} WHERE object_name = '{_SOURCE_PIPELINE_NAME}'")  # noqa: F821

print("[teardown-lfc-sf] tracking rows cleared")
