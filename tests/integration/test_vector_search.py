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
# Tolerant VS migration assertion: any vector_search_index rows in
# migration_status must carry a known terminal VS status. Zero rows is a pass
# (VS preview may be disabled in the test workspace).

_VALID_VS_STATUSES = {
    "created_resync_pending",
    "skipped_direct_access_unsupported",
    "skipped_target_exists",
    "skipped_endpoint_not_ready",
    "failed",
}

_rows = spark.sql(  # noqa: F821
    "SELECT object_name, status "
    "FROM migration_tracking.cp_migration.migration_status "
    "WHERE object_type = 'vector_search_index'"
).collect()

_errors: list[str] = []
if not _rows:
    print("[vector_search] no vector_search_index migration rows — treating as pass (preview likely disabled)")
else:
    for r in _rows:
        if r["status"] not in _VALID_VS_STATUSES:
            _errors.append(f"{r['object_name']}: unexpected status {r['status']}")
        if r["status"] == "failed":
            _errors.append(f"{r['object_name']}: migration failed")

if _errors:
    raise AssertionError("Vector Search migration assertion failed:\n" + "\n".join(_errors))
print(f"[vector_search] assertion passed ({len(_rows)} row(s))")
