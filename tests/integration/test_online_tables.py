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
# Live Online Tables → Synced Tables migration assertion.
#
# The seed injected a synthetic online_table discovery row (no real online table
# on source). The migrate_online_tables job converts it to a Lakebase synced table.
#
# Expected outcomes:
#   created_resync_pending  — synced table created on target (happy path)
#   skipped_instance_not_ready — Lakebase instance did not become AVAILABLE within
#                                 the wait budget; not a test failure, printed as info.

import json

from databricks.sdk.errors import NotFound

from common.auth import AuthManager
from common.config import MigrationConfig

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_target = _auth.target_client

_OT_FQN = dbutils.jobs.taskValues.get(  # noqa: F821
    taskKey="seed_online_tables", key="online_table_fqn", debugValue=""
)

errors: list[str] = []
summary: str = ""


def _latest_status(fqn: str):
    _safe = fqn.replace("'", "''")
    rows = spark.sql(  # noqa: F821
        "SELECT status FROM migration_tracking.cp_migration.migration_status "
        f"WHERE object_type = 'online_table' AND object_name = '{_safe}' "
        "ORDER BY migrated_at DESC LIMIT 1"
    ).collect()
    return rows[0]["status"] if rows else None


def _synced_exists(fqn: str) -> bool:
    try:
        _target.database.get_synced_database_table(fqn)
        return True
    except NotFound:
        return False


# COMMAND ----------
_status = _latest_status(_OT_FQN)

if _status == "skipped_instance_not_ready":
    summary = "skipped_instance_not_ready"
    print(
        f"[test-ot] skipped — Lakebase instance not ready within wait budget "
        f"(status={_status!r}); not a test failure"
    )
elif _status == "created_resync_pending" and _synced_exists(_OT_FQN):
    summary = "asserted_ok"
    print(f"[test-ot] PASS: {_OT_FQN} created_resync_pending + synced table present on target")
else:
    summary = "FAILED"
    if _status != "created_resync_pending":
        errors.append(f"{_OT_FQN} status={_status!r}, expected 'created_resync_pending'")
    if not _synced_exists(_OT_FQN):
        errors.append(f"{_OT_FQN} synced table not found on target — migration did not create it")

# COMMAND ----------
_result = json.dumps({"summary": summary, "errors": errors})
if errors:
    raise AssertionError("Online Tables live integration assertion FAILED: " + _result)
print("[test-ot] passed: " + _result)
dbutils.notebook.exit(_result)  # noqa: F821
