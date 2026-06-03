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
# Best-effort teardown for the live Online Tables → Synced Tables integration test.
# Deletes the synced table + Lakebase instance on target, drops the target test
# catalog, and clears tracking rows. Every step is individually suppressed —
# nothing here raises.

import contextlib

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_src"
_OT_FQN = f"{_CATALOG}.ot_test.ot_online"


def _target():
    return AuthManager(MigrationConfig.from_workspace_file(), dbutils).target_client  # noqa: F821


# COMMAND ----------
# --- Delete synced table on target ---
with contextlib.suppress(Exception):
    _target().database.delete_synced_database_table(_OT_FQN)
    print(f"[teardown-ot] deleted synced table {_OT_FQN} on target")

# COMMAND ----------
# --- Delete Lakebase instance on target (shared resource; best-effort) ---
with contextlib.suppress(Exception):
    _instance_name = MigrationConfig.from_workspace_file().lakebase_instance_name
    _target().database.delete_database_instance(_instance_name)
    print(f"[teardown-ot] deleted Lakebase instance {_instance_name} on target")

# COMMAND ----------
# --- Drop the target test catalog ---
with contextlib.suppress(Exception):
    _auth = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _wh = find_warehouse(_auth)
    execute_and_poll(_auth, _wh, f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")
    print(f"[teardown-ot] dropped target catalog {_CATALOG}")

# COMMAND ----------
# --- Clear tracking rows ---
with contextlib.suppress(Exception):
    spark.sql(  # noqa: F821
        "DELETE FROM migration_tracking.cp_migration.migration_status "
        f"WHERE object_name = '{_OT_FQN}'"
    )
with contextlib.suppress(Exception):
    spark.sql(  # noqa: F821
        "DELETE FROM migration_tracking.cp_migration.discovery_inventory "
        f"WHERE object_name = '{_OT_FQN}'"
    )
print("[teardown-ot] tracking rows cleared")
