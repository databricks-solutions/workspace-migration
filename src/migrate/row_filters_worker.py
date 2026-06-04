# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

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
# Row Filters Worker (Phase 3 Task 29).
#
# Replays ALTER TABLE ... SET ROW FILTER. The filter function itself is
# expected to exist on target — functions_worker runs before this one.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("row_filters_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def apply_row_filter(
    rf: dict,
    *,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
) -> dict:
    table_fqn = rf["table_fqn"]
    filter_fqn = rf["filter_function_fqn"]
    cols = rf.get("filter_columns") or []
    cols_clause = ", ".join(f"`{c}`" for c in cols)
    sql = f"ALTER TABLE {table_fqn} SET ROW FILTER {filter_fqn} ON ({cols_clause})"
    # C6: object_name must match discovery's key (table_fqn) so
    # ``get_pending_objects`` LEFT JOIN matches and the row is treated as
    # terminal on re-run. The historical ``ROW_FILTER_`` prefix collapsed
    # this LEFT JOIN to NULL — every run reprocessed every filter.
    obj_key = table_fqn

    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] %s", sql)
        return {
            "object_name": obj_key,
            "object_type": "row_filter",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }

    logger.info("Executing: %s", sql)
    result = execute_and_poll(auth, wh_id, sql)
    duration = time.time() - start
    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_key,
            "object_type": "row_filter",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }
    return {
        "object_name": obj_key,
        "object_type": "row_filter",
        "status": "validated",
        "error_message": None,
        "duration_seconds": duration,
    }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="row_filter_list")
    row_rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d row_filter records.", len(row_rows))

    wh_id = find_warehouse(auth)
    results: list[dict] = []
    for r in row_rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if not isinstance(meta, dict):
            continue
        try:
            res = apply_row_filter(meta, auth=auth, wh_id=wh_id, dry_run=config.dry_run)
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": meta.get("table_fqn", "?"),
                "object_type": "row_filter",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Row filters worker complete. %d validated, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
