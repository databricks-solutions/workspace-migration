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
# Live LFC CDC (Tier-2, SQL Server) migration assertion.
#
# Asserts (create-only + validate; pipelines NOT started — D2):
#   lfc_gateway   — gateway recreated on target (status lfc_gateway_created)
#   lfc_pipeline  — ingestion recreated (lfc_pipeline_created_fullreload, NOT failed)
# On TARGET:
#   recreated gateway pipeline + ingestion pipeline EXIST, and the ingestion's
#   ingestion_gateway_id is REMAPPED to the new target gateway (topology mirrored).
# Discovery:
#   the source gateway staging volume is EXCLUDED (object_type=gateway_staging_volume
#   in discovery_inventory; NOT a migrated 'volume').
#
# NOT asserted: destination-table data (we don't start the pipelines — by design).
# Coverage guard: zero lfc_gateway/lfc_pipeline rows ⇒ RED.

import json

from common.auth import AuthManager
from common.config import MigrationConfig

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_tgt_client = _auth.target_client

_GATEWAY_MIGRATED = "cdc_gateway_staging_migrated"     # worker: f"{gateway_storage_name}_migrated"
_INGESTION_MIGRATED = "lfc_it_cdc_orders_migrated"     # worker: f"{object_name}_migrated"
_tracking_fqn = f"{_config.tracking_catalog}.{_config.tracking_schema}"

errors: list[str] = []
summary: dict = {}

# COMMAND ----------
_status_rows = spark.sql(  # noqa: F821
    f"SELECT object_name, object_type, status, error_message "
    f"FROM {_tracking_fqn}.migration_status "
    f"WHERE object_type IN ('lfc_gateway', 'lfc_pipeline') ORDER BY migrated_at DESC"
).collect()
print(f"[test-cdc] found {len(_status_rows)} lfc_gateway/lfc_pipeline rows")
if not _status_rows:
    raise AssertionError("[test-cdc] COVERAGE GUARD: zero lfc_gateway/lfc_pipeline rows — CDC stage did not run.")

_gw_rows = [r for r in _status_rows if r["object_type"] == "lfc_gateway"]
_ing_rows = [r for r in _status_rows if r["object_type"] == "lfc_pipeline" and "lfc_it_cdc_orders" in (r["object_name"] or "")]

# COMMAND ----------
# --- lfc_gateway: created ---
if not _gw_rows:
    errors.append("lfc_gateway: no row")
    summary["lfc_gateway"] = "FAILED_no_row"
elif _gw_rows[0]["status"] != "lfc_gateway_created":
    errors.append(f"lfc_gateway: status={_gw_rows[0]['status']!r}, expected 'lfc_gateway_created'")
    summary["lfc_gateway"] = "FAILED_wrong_status"
else:
    summary["lfc_gateway"] = "asserted_ok"
    print("[test-cdc] lfc_gateway ok: lfc_gateway_created")

# COMMAND ----------
# --- lfc_pipeline: created_fullreload, validate not a config failure ---
if not _ing_rows:
    errors.append("lfc_pipeline: no row for lfc_it_cdc_orders")
    summary["lfc_pipeline"] = "FAILED_no_row"
else:
    _ir = _ing_rows[0]
    if _ir["status"] != "lfc_pipeline_created_fullreload":
        errors.append(f"lfc_pipeline lfc_it_cdc_orders: status={_ir['status']!r}, expected "
                      f"'lfc_pipeline_created_fullreload'. err={_ir['error_message']!r}")
        summary["lfc_pipeline"] = "FAILED_wrong_status"
    else:
        summary["lfc_pipeline"] = f"asserted_ok (validate: {_ir['error_message'] or 'validated'})"
        print(f"[test-cdc] lfc_pipeline ok: created_fullreload (validate: {_ir['error_message'] or 'validated'})")

# COMMAND ----------
# --- TARGET: gateway + ingestion exist; gateway id REMAPPED (topology mirrored) ---
_tgt = {getattr(p, "name", None): p for p in _tgt_client.pipelines.list_pipelines()}
_gw_p = _tgt.get(_GATEWAY_MIGRATED)
_ing_p = _tgt.get(_INGESTION_MIGRATED)
if not _gw_p:
    errors.append(f"target gateway {_GATEWAY_MIGRATED!r} not found")
    summary["target_gateway"] = "FAILED_not_found"
else:
    summary["target_gateway"] = "asserted_ok"
if not _ing_p:
    errors.append(f"target ingestion {_INGESTION_MIGRATED!r} not found")
    summary["target_ingestion"] = "FAILED_not_found"
else:
    summary["target_ingestion"] = "asserted_ok"

if _gw_p and _ing_p:
    _ing_spec = _tgt_client.pipelines.get(_ing_p.pipeline_id).spec.as_dict()
    _remapped = (_ing_spec.get("ingestion_definition") or {}).get("ingestion_gateway_id")
    if _remapped == _gw_p.pipeline_id:
        summary["gateway_remapped"] = "asserted_ok"
        print(f"[test-cdc] gateway mapping mirrored: ingestion → {_remapped}")
    else:
        errors.append(f"ingestion_gateway_id={_remapped!r} != target gateway id {_gw_p.pipeline_id!r}")
        summary["gateway_remapped"] = "FAILED_mapping"

# COMMAND ----------
# --- Discovery: source gateway staging volume EXCLUDED ---
_staging_fqn = dbutils.jobs.taskValues.get(taskKey="seed_lfc_cdc", key="staging_volume_fqn", debugValue="")  # noqa: F821
if not _staging_fqn:
    errors.append("staging_volume_fqn taskValue missing from seed")
    summary["staging_excluded"] = "FAILED_no_taskvalue"
else:
    _inv = spark.sql(  # noqa: F821
        f"SELECT object_type FROM {_tracking_fqn}.discovery_inventory WHERE object_name = '{_staging_fqn}'"
    ).collect()
    _types = {r["object_type"] for r in _inv}
    if "gateway_staging_volume" in _types and "volume" not in _types:
        summary["staging_excluded"] = "asserted_ok"
        print(f"[test-cdc] staging volume excluded: {_staging_fqn} tagged gateway_staging_volume")
    else:
        errors.append(f"staging volume {_staging_fqn}: object_types={_types}, expected only gateway_staging_volume")
        summary["staging_excluded"] = "FAILED_not_excluded"

# COMMAND ----------
_result = json.dumps({"summary": summary, "errors": errors})
if errors:
    raise AssertionError("LFC CDC live assertion FAILED: " + _result)
print("[test-cdc] all asserted cases passed: " + _result)
dbutils.notebook.exit(_result)  # noqa: F821
