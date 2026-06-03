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
# Best-effort teardown for the live Online Tables integration test. Deletes the
# online table on BOTH source and target, drops the test catalog on both sides,
# clears tracking rows. Every step try/excepted — nothing here raises.

import contextlib

from databricks.sdk import WorkspaceClient

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_src"
_OT_FQN = f"{_CATALOG}.ot_test.ot_online"


def _source_client():
    return WorkspaceClient()


def _target_client():
    return AuthManager(MigrationConfig.from_workspace_file(), dbutils).target_client  # noqa: F821


# COMMAND ----------
for _make_client, _label in ((_source_client, "source"), (_target_client, "target")):
    try:
        _client = _make_client()
    except Exception as _exc:  # noqa: BLE001
        print(f"[teardown-ot] could not build {_label} client — skipping {_label}: {_exc}")
        continue
    with contextlib.suppress(Exception):
        _client.online_tables.delete(_OT_FQN)
        print(f"[teardown-ot] deleted online table {_OT_FQN} on {_label}")

# COMMAND ----------
with contextlib.suppress(Exception):
    spark.sql(f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")  # noqa: F821
    print(f"[teardown-ot] dropped source catalog {_CATALOG}")

with contextlib.suppress(Exception):
    _auth = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _wh = find_warehouse(_auth)
    execute_and_poll(_auth, _wh, f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")
    print(f"[teardown-ot] dropped target catalog {_CATALOG}")

# COMMAND ----------
with contextlib.suppress(Exception):
    spark.sql(  # noqa: F821
        f"DELETE FROM migration_tracking.cp_migration.migration_status WHERE object_name = '{_OT_FQN}'"
    )
with contextlib.suppress(Exception):
    spark.sql(  # noqa: F821
        f"DELETE FROM migration_tracking.cp_migration.discovery_inventory WHERE object_name = '{_OT_FQN}'"
    )
print("[teardown-ot] tracking rows cleared")
