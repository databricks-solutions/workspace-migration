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
# Live Vector Search migration assertion.
#   Positive (Delta Sync): migration_status == created_resync_pending AND the
#     index exists on the TARGET (get_index succeeds).
#   Negative (Direct Access): migration_status == skipped_direct_access_unsupported
#     AND the index does NOT exist on the target (get_index raises NotFound).
# Each case is gated on the seed's has_* flag — skipped (not failed) if the seed
# could not create that object (e.g. VS unavailable).

from databricks.sdk.errors import NotFound

from common.auth import AuthManager
from common.config import MigrationConfig

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_target = _auth.target_client

_has_delta = dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key="has_delta_index", debugValue="false")  # noqa: F821
_has_direct = dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key="has_direct_index", debugValue="false")  # noqa: F821
_delta_fqn = dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key="delta_index_fqn", debugValue="")  # noqa: F821
_direct_fqn = dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key="direct_index_fqn", debugValue="")  # noqa: F821

errors: list[str] = []


def _latest_status(fqn: str):
    rows = spark.sql(  # noqa: F821
        "SELECT status FROM migration_tracking.cp_migration.migration_status "
        f"WHERE object_type = 'vector_search_index' AND object_name = '{fqn}' "
        "ORDER BY migrated_at DESC LIMIT 1"
    ).collect()
    return rows[0]["status"] if rows else None


def _exists_on_target(fqn: str) -> bool:
    try:
        _target.vector_search_indexes.get_index(fqn)
        return True
    except NotFound:
        return False


# COMMAND ----------
# --- Positive case: Delta Sync ---
if _has_delta == "true":
    _status = _latest_status(_delta_fqn)
    if _status != "created_resync_pending":
        errors.append(f"POSITIVE: {_delta_fqn} status={_status!r}, expected 'created_resync_pending'")
    if not _exists_on_target(_delta_fqn):
        errors.append(f"POSITIVE: {_delta_fqn} not found on target — migration did not create the index")
    else:
        print(f"[test-vs] POSITIVE ok: {_delta_fqn} created_resync_pending + present on target")
else:
    print("[test-vs] POSITIVE skipped — seed did not create the Delta Sync index (VS unavailable?)")

# COMMAND ----------
# --- Negative case: Direct Access ---
if _has_direct == "true":
    _status = _latest_status(_direct_fqn)
    if _status != "skipped_direct_access_unsupported":
        errors.append(f"NEGATIVE: {_direct_fqn} status={_status!r}, expected 'skipped_direct_access_unsupported'")
    if _exists_on_target(_direct_fqn):
        errors.append(f"NEGATIVE: {_direct_fqn} unexpectedly EXISTS on target — Direct Access must not be migrated")
    else:
        print(f"[test-vs] NEGATIVE ok: {_direct_fqn} skipped + absent on target")
else:
    print("[test-vs] NEGATIVE skipped — seed did not create the Direct Access index (VS unavailable?)")

# COMMAND ----------
if errors:
    raise AssertionError("Vector Search live integration assertion failed:\n" + "\n".join(errors))
print("[test-vs] all asserted cases passed")
