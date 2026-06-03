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
# Live Online Tables migration assertion (positive case).
#   migration_status == created_resync_pending AND the online table exists on
#   the TARGET (online_tables.get succeeds). Skipped (not failed) if the seed
#   could not create the source online table (preview unavailable).

import json

from databricks.sdk.errors import NotFound

from common.auth import AuthManager
from common.config import MigrationConfig

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_target = _auth.target_client

_has_ot = dbutils.jobs.taskValues.get(taskKey="seed_online_tables", key="has_online_table", debugValue="false")  # noqa: F821
_ot_fqn = dbutils.jobs.taskValues.get(taskKey="seed_online_tables", key="online_table_fqn", debugValue="")  # noqa: F821

errors: list[str] = []
summary: dict = {}


def _latest_status(fqn: str):
    _safe = fqn.replace("'", "''")
    rows = spark.sql(  # noqa: F821
        "SELECT status FROM migration_tracking.cp_migration.migration_status "
        f"WHERE object_type = 'online_table' AND object_name = '{_safe}' "
        "ORDER BY migrated_at DESC LIMIT 1"
    ).collect()
    return rows[0]["status"] if rows else None


def _exists_on_target(fqn: str) -> bool:
    try:
        _target.online_tables.get(fqn)
        return True
    except NotFound:
        return False


# COMMAND ----------
if _has_ot == "true":
    _n = len(errors)
    _status = _latest_status(_ot_fqn)
    if _status != "created_resync_pending":
        errors.append(f"POSITIVE: {_ot_fqn} status={_status!r}, expected 'created_resync_pending'")
    if not _exists_on_target(_ot_fqn):
        errors.append(f"POSITIVE: {_ot_fqn} not found on target — migration did not create the online table")
    if len(errors) == _n:
        summary["online_table"] = "asserted_ok"
        print(f"[test-ot] POSITIVE ok: {_ot_fqn} created_resync_pending + present on target")
    else:
        summary["online_table"] = "FAILED"
else:
    summary["online_table"] = "skipped_no_seed"
    print("[test-ot] skipped — seed did not create the online table (preview unavailable?)")

# COMMAND ----------
_result = json.dumps({"summary": summary, "errors": errors})
if errors:
    raise AssertionError("Online Tables live integration assertion FAILED: " + _result)
print("[test-ot] passed: " + _result)
dbutils.notebook.exit(_result)  # noqa: F821
