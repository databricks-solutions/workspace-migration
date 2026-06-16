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
# Live LFC SaaS (Salesforce, Tier-1 row_filter) migration assertion.
#
# Asserts three migration_status rows produced by lfc_worker:
#   lfc_table    — <account_history fqn> with status "validated"
#   lfc_pipeline — lfc_it_sf_account     with "lfc_pipeline_created_incremental"
#   lfc_view     — <account fqn>         with "lfc_view_created" (accept the
#                  deferred "lfc_view_pending_forward_ingest" only when the
#                  target can't run forward ingestion).
#
# Also verifies on TARGET:
#   integration_test_lfc_sf.sf.account_history exists with the same row count as
#   the seeded source account table, and the unified view ...sf.account exists.
#
# Coverage guard: zero lfc_* rows ⇒ RED (the SaaS scenario never ran).

import json

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_fetch, find_warehouse

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_tgt_client = _auth.target_client
_tgt_wh = find_warehouse(_auth, use_source=False)

_CATALOG = "integration_test_lfc_sf"
_SCHEMA = "sf"
_ACCOUNT_FQN = f"{_CATALOG}.{_SCHEMA}.account"
_HISTORY_FQN = f"{_CATALOG}.{_SCHEMA}.account_history"
_PIPELINE_NAME = "lfc_it_sf_account"
_MIGRATED_PIPELINE_NAME = "lfc_it_sf_account_migrated"

_tracking_fqn = f"{_config.tracking_catalog}.{_config.tracking_schema}"

errors: list[str] = []
summary: dict = {}

# COMMAND ----------
_status_rows = spark.sql(  # noqa: F821
    f"SELECT object_name, object_type, status, error_message "
    f"FROM {_tracking_fqn}.migration_status "
    f"WHERE object_type IN ('lfc_table', 'lfc_pipeline', 'lfc_view') "
    f"ORDER BY migrated_at DESC"
).collect()
print(f"[test-lfc-sf] found {len(_status_rows)} lfc_* migration_status rows")

if not _status_rows:
    raise AssertionError(
        "[test-lfc-sf] COVERAGE GUARD: zero lfc_table / lfc_pipeline / lfc_view rows — "
        "the SaaS LFC stage did not run. Check discovery found 'lfc_it_sf_account' and "
        "lfc_worker was triggered."
    )

_by_type: dict[str, list] = {"lfc_table": [], "lfc_pipeline": [], "lfc_view": []}
for _r in _status_rows:
    if _r["object_type"] in _by_type:
        _by_type[_r["object_type"]].append(_r)

# COMMAND ----------
# --- lfc_table: account_history validated ---
_table_rows = [r for r in _by_type["lfc_table"] if "account_history" in (r["object_name"] or "")]
if not _table_rows:
    errors.append(f"lfc_table: no row with 'account_history'. rows: {[r['object_name'] for r in _by_type['lfc_table']]}")
    summary["lfc_table"] = "FAILED_no_row"
else:
    _tr = _table_rows[0]
    if _tr["status"] != "validated":
        errors.append(f"lfc_table account_history: status={_tr['status']!r}, expected 'validated'. err={_tr['error_message']!r}")
        summary["lfc_table"] = "FAILED_wrong_status"
    else:
        summary["lfc_table"] = "asserted_ok"
        print(f"[test-lfc-sf] lfc_table ok: {_tr['object_name']} validated")

# COMMAND ----------
# --- lfc_pipeline: lfc_it_sf_account created_incremental ---
_pipeline_rows = [r for r in _by_type["lfc_pipeline"] if _PIPELINE_NAME in (r["object_name"] or "")]
if not _pipeline_rows:
    errors.append(f"lfc_pipeline: no row with {_PIPELINE_NAME!r}. rows: {[r['object_name'] for r in _by_type['lfc_pipeline']]}")
    summary["lfc_pipeline"] = "FAILED_no_row"
else:
    _pr = _pipeline_rows[0]
    if _pr["status"] != "lfc_pipeline_created_incremental":
        errors.append(f"lfc_pipeline {_PIPELINE_NAME!r}: status={_pr['status']!r}, expected 'lfc_pipeline_created_incremental'. err={_pr['error_message']!r}")
        summary["lfc_pipeline"] = "FAILED_wrong_status"
    else:
        summary["lfc_pipeline"] = "asserted_ok"
        print(f"[test-lfc-sf] lfc_pipeline ok: {_pr['object_name']} created_incremental")

# COMMAND ----------
# --- lfc_view: account unified view created (accept deferred) ---
_view_rows = [r for r in _by_type["lfc_view"] if _ACCOUNT_FQN in (r["object_name"] or "")]
if not _view_rows:
    errors.append(f"lfc_view: no row with object_name={_ACCOUNT_FQN!r}. rows: {[r['object_name'] for r in _by_type['lfc_view']]}")
    summary["lfc_view"] = "FAILED_no_row"
else:
    _vs = _view_rows[0]["status"]
    if _vs == "lfc_view_created":
        summary["lfc_view"] = "asserted_ok"
        print(f"[test-lfc-sf] lfc_view ok: {_ACCOUNT_FQN} lfc_view_created")
    elif _vs == "lfc_view_pending_forward_ingest":
        summary["lfc_view"] = "asserted_ok (deferred — no target forward ingestion)"
        print(f"[test-lfc-sf] lfc_view deferred: {_view_rows[0]['error_message']}")
    else:
        errors.append(f"lfc_view {_ACCOUNT_FQN!r}: status={_vs!r}, expected created or pending. err={_view_rows[0]['error_message']!r}")
        summary["lfc_view"] = "FAILED_wrong_status"

_view_deferred = bool(_view_rows) and _view_rows[0]["status"] == "lfc_view_pending_forward_ingest"

# COMMAND ----------
# --- TARGET: account_history exists with expected row count ---
_seed_rows_str = dbutils.jobs.taskValues.get(taskKey="seed_lfc_salesforce", key="account_rows", debugValue="0")  # noqa: F821
_seed_rows = int(_seed_rows_str) if str(_seed_rows_str).isdigit() else 0

_hist_res = execute_and_fetch(
    _auth, _tgt_wh, f"SELECT COUNT(*) AS n FROM `{_CATALOG}`.`{_SCHEMA}`.`account_history`", use_source=False,
)
if _hist_res["state"] != "SUCCEEDED":
    errors.append(f"target account_history: count query failed: {_hist_res.get('error', _hist_res['state'])}")
    summary["target_history"] = "FAILED_query_error"
else:
    _hist_raw = (_hist_res.get("rows") or [[None]])[0]
    _hist_count = int(_hist_raw[0]) if _hist_raw and _hist_raw[0] is not None else 0
    if _hist_count == 0:
        errors.append("target account_history: 0 rows — clone did not land data on target")
        summary["target_history"] = "FAILED_zero_rows"
    elif _seed_rows > 0 and _hist_count != _seed_rows:
        errors.append(f"target account_history: row count mismatch — source {_seed_rows}, target {_hist_count}")
        summary["target_history"] = "FAILED_row_count_mismatch"
    else:
        summary["target_history"] = f"asserted_ok ({_hist_count} rows)"
        print(f"[test-lfc-sf] target account_history ok: {_hist_count} rows")

# COMMAND ----------
# --- TARGET: unified view exists (skipped when deferred) ---
if _view_deferred:
    summary["target_view"] = "skipped (view deferred)"
    print("[test-lfc-sf] target unified view check skipped — view deferred")
else:
    _view_res = execute_and_fetch(
        _auth, _tgt_wh, f"SELECT 1 AS _e FROM `{_CATALOG}`.`{_SCHEMA}`.`account` LIMIT 1", use_source=False,
    )
    if _view_res["state"] != "SUCCEEDED":
        errors.append(f"target unified view {_ACCOUNT_FQN!r} not queryable: {_view_res.get('error', _view_res['state'])}")
        summary["target_view"] = "FAILED_not_found"
    else:
        summary["target_view"] = "asserted_ok"
        print(f"[test-lfc-sf] target unified view {_ACCOUNT_FQN!r} exists")

# COMMAND ----------
# --- TARGET: recreated pipeline object exists ---
_tgt_pipeline_found = False
try:
    for _tp in _tgt_client.pipelines.list_pipelines():
        if getattr(_tp, "name", None) == _MIGRATED_PIPELINE_NAME:
            _tgt_pipeline_found = True
            break
except Exception as _exc:  # noqa: BLE001
    errors.append(f"target pipeline list failed: {_exc}")

if not _tgt_pipeline_found:
    errors.append(f"target recreated pipeline {_MIGRATED_PIPELINE_NAME!r} not found")
    summary["target_pipeline_created"] = "FAILED_not_found"
else:
    summary["target_pipeline_created"] = "asserted_ok"
    print(f"[test-lfc-sf] target pipeline {_MIGRATED_PIPELINE_NAME!r} exists")

# COMMAND ----------
_result = json.dumps({"summary": summary, "errors": errors})
if errors:
    raise AssertionError("LFC Salesforce live assertion FAILED: " + _result)
print("[test-lfc-sf] all asserted cases passed: " + _result)
dbutils.notebook.exit(_result)  # noqa: F821
