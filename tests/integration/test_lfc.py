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
# Live LFC query-based migration assertion.
#
# Asserts three migration_status rows produced by lfc_worker:
#   lfc_table   — <orders_history fqn> with status "validated"
#   lfc_pipeline — <pipeline_name> with status "lfc_pipeline_created_incremental"
#   lfc_view     — <orders fqn>    with status "lfc_view_created"
#
# Also verifies on TARGET:
#   integration_test_lfc.sqlsrv.orders_history exists with the same row
#   count as the source orders table, and the unified view
#   integration_test_lfc.sqlsrv.orders exists.
#
# Coverage guard: if ZERO lfc_* rows exist in migration_status the test
# raises immediately (the query-based scenario was never exercised, which
# is a test-infrastructure failure, not a skippable environment limit).
#
# NOTE: the recreated TARGET pipeline is asserted as created only (status
# row + pipeline object existence). It is NOT asserted to have ingested
# data — the target workspace has no NCC PE to the SQL server, so the
# pipeline can be created but cannot run. Data validation is via the
# cloned _history table only.

import json

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_fetch, find_warehouse

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_tgt_client = _auth.target_client

_src_wh = find_warehouse(_auth, use_source=True)
_tgt_wh = find_warehouse(_auth, use_source=False)

_CATALOG = "integration_test_lfc"
_SCHEMA = "sqlsrv"
_ORDERS_FQN = f"{_CATALOG}.{_SCHEMA}.orders"
_HISTORY_FQN = f"{_CATALOG}.{_SCHEMA}.orders_history"
_PIPELINE_NAME = "lfc_it_orders"

_tracking_fqn = f"{_config.tracking_catalog}.{_config.tracking_schema}"

errors: list[str] = []
summary: dict = {}

# COMMAND ----------
# --- Read all lfc_* rows from migration_status ---
_status_rows = spark.sql(  # noqa: F821
    f"SELECT object_name, object_type, status, error_message "
    f"FROM {_tracking_fqn}.migration_status "
    f"WHERE object_type IN ('lfc_table', 'lfc_pipeline', 'lfc_view') "
    f"ORDER BY migrated_at DESC"
).collect()

print(f"[test-lfc] found {len(_status_rows)} lfc_* migration_status rows")

# Coverage guard: zero rows means the query-based stage was never exercised.
if not _status_rows:
    raise AssertionError(
        "[test-lfc] COVERAGE GUARD: zero lfc_table / lfc_pipeline / lfc_view rows in "
        "migration_status — the query-based LFC stage did not run. Check that "
        "discovery found the 'lfc_it_orders' pipeline and lfc_worker was triggered."
    )

# Index rows by (object_type, object_name_suffix) for assertion.
_by_type: dict[str, list] = {"lfc_table": [], "lfc_pipeline": [], "lfc_view": []}
for _r in _status_rows:
    _ot = _r["object_type"]
    if _ot in _by_type:
        _by_type[_ot].append(_r)

# COMMAND ----------
# --- Assert lfc_table: orders_history validated ---
_table_rows = [r for r in _by_type["lfc_table"] if "orders_history" in (r["object_name"] or "")]
if not _table_rows:
    errors.append(
        f"lfc_table: no migration_status row with object_name containing 'orders_history'. "
        f"All lfc_table rows: {[r['object_name'] for r in _by_type['lfc_table']]}"
    )
    summary["lfc_table"] = "FAILED_no_row"
else:
    _tr = _table_rows[0]
    _ts = _tr["status"]
    _te = _tr["error_message"]
    if _ts != "validated":
        errors.append(f"lfc_table orders_history: status={_ts!r}, expected 'validated'. error_message={_te!r}")
        summary["lfc_table"] = "FAILED_wrong_status"
    elif _te:
        errors.append(f"lfc_table orders_history: status='validated' but error_message set: {_te!r}")
        summary["lfc_table"] = "FAILED_error_message"
    else:
        summary["lfc_table"] = "asserted_ok"
        print(f"[test-lfc] lfc_table ok: {_tr['object_name']} validated")

# COMMAND ----------
# --- Assert lfc_pipeline: lfc_it_orders with lfc_pipeline_created_incremental ---
_pipeline_rows = [r for r in _by_type["lfc_pipeline"] if _PIPELINE_NAME in (r["object_name"] or "")]
if not _pipeline_rows:
    errors.append(
        f"lfc_pipeline: no migration_status row with object_name containing {_PIPELINE_NAME!r}. "
        f"All lfc_pipeline rows: {[r['object_name'] for r in _by_type['lfc_pipeline']]}"
    )
    summary["lfc_pipeline"] = "FAILED_no_row"
else:
    _pr = _pipeline_rows[0]
    _ps = _pr["status"]
    _pe = _pr["error_message"]
    if _ps != "lfc_pipeline_created_incremental":
        errors.append(f"lfc_pipeline {_PIPELINE_NAME!r}: status={_ps!r}, expected 'lfc_pipeline_created_incremental'. error_message={_pe!r}")
        summary["lfc_pipeline"] = "FAILED_wrong_status"
    else:
        summary["lfc_pipeline"] = "asserted_ok"
        print(f"[test-lfc] lfc_pipeline ok: {_pr['object_name']} lfc_pipeline_created_incremental")

# COMMAND ----------
# --- Assert lfc_view: orders unified view created ---
_view_rows = [r for r in _by_type["lfc_view"] if _ORDERS_FQN in (r["object_name"] or "")]
if not _view_rows:
    errors.append(
        f"lfc_view: no migration_status row with object_name={_ORDERS_FQN!r}. "
        f"All lfc_view rows: {[r['object_name'] for r in _by_type['lfc_view']]}"
    )
    summary["lfc_view"] = "FAILED_no_row"
else:
    _vr = _view_rows[0]
    _vs = _vr["status"]
    _ve = _vr["error_message"]
    # End-to-end: the recreated pipeline must run (target reaches the source DB via
    # the NCC PE + NSP service-tag rule) so <t>_incr lands and the unified view is
    # CREATED. A deferred view is NOT accepted here — that would mean forward
    # ingestion didn't complete, which is a failure of the end-to-end path.
    if _vs == "lfc_view_created":
        summary["lfc_view"] = "asserted_ok"
        print(f"[test-lfc] lfc_view ok: {_vr['object_name']} lfc_view_created")
    else:
        errors.append(f"lfc_view {_ORDERS_FQN!r}: status={_vs!r}, expected 'lfc_view_created' (end-to-end). error_message={_ve!r}")
        summary["lfc_view"] = "FAILED_not_created"

# COMMAND ----------
# --- Assert TARGET: orders_history exists with expected row count ---
# Source orders row count (read via seed task values).
_seed_orders_rows_str = dbutils.jobs.taskValues.get(taskKey="seed_lfc", key="orders_rows", debugValue="0")  # noqa: F821
_seed_orders_rows = int(_seed_orders_rows_str) if str(_seed_orders_rows_str).isdigit() else 0

_hist_res = execute_and_fetch(
    _auth, _tgt_wh,
    f"SELECT COUNT(*) AS n FROM `{_CATALOG}`.`{_SCHEMA}`.`orders_history`",
    use_source=False,
)
if _hist_res["state"] != "SUCCEEDED":
    errors.append(f"target orders_history: count query failed: {_hist_res.get('error', _hist_res['state'])}")
    summary["target_history"] = "FAILED_query_error"
else:
    _hist_rows_raw = (_hist_res.get("rows") or [[None]])[0]
    _hist_count = int(_hist_rows_raw[0]) if _hist_rows_raw and _hist_rows_raw[0] is not None else 0
    if _hist_count == 0:
        errors.append("target orders_history: 0 rows — clone did not land data on target")
        summary["target_history"] = "FAILED_zero_rows"
    elif _seed_orders_rows > 0 and _hist_count != _seed_orders_rows:
        errors.append(
            f"target orders_history: row count mismatch — source orders has {_seed_orders_rows} rows, "
            f"target orders_history has {_hist_count} rows"
        )
        summary["target_history"] = "FAILED_row_count_mismatch"
    else:
        summary["target_history"] = f"asserted_ok ({_hist_count} rows)"
        print(f"[test-lfc] target orders_history ok: {_hist_count} rows")

# COMMAND ----------
# --- Assert TARGET: unified view orders exists AND its row count matches source ---
# SCD1 view = orders_history UNION orders_incr, deduped by PK → COUNT(*) must equal
# the source orders row count (distinct order_id). This proves the view resolves
# (both legs queryable) and the union/dedup produces the right cardinality.
_view_cnt_res = execute_and_fetch(
    _auth, _tgt_wh,
    f"SELECT COUNT(*) AS n FROM `{_CATALOG}`.`{_SCHEMA}`.`orders`",
    use_source=False,
)
if _view_cnt_res["state"] != "SUCCEEDED":
    errors.append(f"target unified view {_ORDERS_FQN!r} not queryable: {_view_cnt_res.get('error', _view_cnt_res['state'])}")
    summary["target_view"] = "FAILED_not_queryable"
else:
    _vc_raw = (_view_cnt_res.get("rows") or [[None]])[0]
    _view_count = int(_vc_raw[0]) if _vc_raw and _vc_raw[0] is not None else 0
    if _seed_orders_rows > 0 and _view_count != _seed_orders_rows:
        errors.append(
            f"target unified view {_ORDERS_FQN!r}: row count {_view_count} != source orders {_seed_orders_rows} "
            "(SCD1 view should dedup to the source cardinality)"
        )
        summary["target_view"] = f"FAILED_count_mismatch ({_view_count} vs {_seed_orders_rows})"
    else:
        summary["target_view"] = f"asserted_ok ({_view_count} rows)"
        print(f"[test-lfc] target unified view {_ORDERS_FQN!r} ok: {_view_count} rows == source {_seed_orders_rows}")

# COMMAND ----------
# --- Assert TARGET: recreated pipeline object exists (creation only, not ingestion) ---
_migrated_pipeline_name = "lfc_it_orders_migrated"
_tgt_pipeline_found = False
try:
    for _tp in _tgt_client.pipelines.list_pipelines():
        if getattr(_tp, "name", None) == _migrated_pipeline_name:
            _tgt_pipeline_found = True
            break
except Exception as _exc:  # noqa: BLE001
    errors.append(f"target pipeline list failed: {_exc}")

if not _tgt_pipeline_found:
    errors.append(
        f"target recreated pipeline {_migrated_pipeline_name!r} not found — "
        "lfc_worker should have created it even if it cannot run (no NCC PE)"
    )
    summary["target_pipeline_created"] = "FAILED_not_found"
else:
    summary["target_pipeline_created"] = "asserted_ok"
    print(f"[test-lfc] target pipeline {_migrated_pipeline_name!r} exists (creation verified)")

# COMMAND ----------
# Emit a retrievable result so the run outcome is provable via the Jobs API.
_result = json.dumps({"summary": summary, "errors": errors})
if errors:
    raise AssertionError("LFC live integration assertion FAILED: " + _result)
print("[test-lfc] all asserted cases passed: " + _result)
dbutils.notebook.exit(_result)  # noqa: F821
