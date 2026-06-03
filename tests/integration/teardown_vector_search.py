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
# Best-effort teardown for the live Vector Search integration test. Deletes the
# indexes + endpoint on BOTH source and target (endpoints are a paid resource),
# drops the test catalog on both sides, and clears tracking rows. Every step is
# individually try/excepted — nothing here raises.

import contextlib

from databricks.sdk import WorkspaceClient

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_src"
_ENDPOINT = "cp_migration_vs_it"
_DELTA_IDX = f"{_CATALOG}.vs_test.vs_delta_idx"
_DIRECT_IDX = f"{_CATALOG}.vs_test.vs_direct_idx"

# COMMAND ----------
# Lazily build each workspace's client only when that side is being cleaned, so
# a broken target config can never prevent source-side (paid endpoint) cleanup.


def _source_client():
    return WorkspaceClient()


def _target_client():
    return AuthManager(MigrationConfig.from_workspace_file(), dbutils).target_client  # noqa: F821


# COMMAND ----------
# --- Delete indexes + endpoint on BOTH workspaces (each side independent) ---
for _make_client, _label in ((_source_client, "source"), (_target_client, "target")):
    try:
        _client = _make_client()
    except Exception as _exc:  # noqa: BLE001
        print(f"[teardown-vs] could not build {_label} client — skipping {_label} cleanup: {_exc}")
        continue
    for _idx in (_DELTA_IDX, _DIRECT_IDX):
        with contextlib.suppress(Exception):
            _client.vector_search_indexes.delete_index(_idx)
            print(f"[teardown-vs] deleted index {_idx} on {_label}")
    with contextlib.suppress(Exception):
        _client.vector_search_endpoints.delete_endpoint(_ENDPOINT)
        print(f"[teardown-vs] deleted endpoint {_ENDPOINT} on {_label}")

# COMMAND ----------
# --- Drop the source test catalog ---
with contextlib.suppress(Exception):
    spark.sql(f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")  # noqa: F821
    print(f"[teardown-vs] dropped source catalog {_CATALOG}")

# --- Drop the target test catalog (created by the seed) ---
with contextlib.suppress(Exception):
    _auth = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _wh = find_warehouse(_auth)
    execute_and_poll(_auth, _wh, f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")
    print(f"[teardown-vs] dropped target catalog {_CATALOG}")

# COMMAND ----------
# --- Clear tracking rows for the two indexes ---
for _fqn in (_DELTA_IDX, _DIRECT_IDX):
    with contextlib.suppress(Exception):
        spark.sql(  # noqa: F821
            "DELETE FROM migration_tracking.cp_migration.migration_status "
            f"WHERE object_name = '{_fqn}'"
        )
    with contextlib.suppress(Exception):
        spark.sql(  # noqa: F821
            "DELETE FROM migration_tracking.cp_migration.discovery_inventory "
            f"WHERE object_name = '{_fqn}'"
        )
print("[teardown-vs] tracking rows cleared")
