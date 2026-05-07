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
# Column Masks Worker (Phase 3 Task 30).
#
# Replays ALTER TABLE t ALTER COLUMN c SET MASK mask_fqn USING COLUMNS (...).
# Depends on the mask function existing on target (functions_worker runs first).

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("column_masks_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def apply_column_mask(
    cm: dict,
    *,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
) -> dict:
    table_fqn = cm["table_fqn"]
    column = cm["column_name"]
    mask_fqn = cm["mask_function_fqn"]
    using_cols = cm.get("mask_using_columns") or []

    sql = f"ALTER TABLE {table_fqn} ALTER COLUMN `{column}` SET MASK {mask_fqn}"
    if using_cols:
        using = ", ".join(f"`{c}`" for c in using_cols)
        sql += f" USING COLUMNS ({using})"

    obj_key = f"COLUMN_MASK_{table_fqn}.{column}"
    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] %s", sql)
        return {
            "object_name": obj_key,
            "object_type": "column_mask",
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
            "object_type": "column_mask",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }
    return {
        "object_name": obj_key,
        "object_type": "column_mask",
        "status": "validated",
        "error_message": None,
        "duration_seconds": duration,
    }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="column_mask_list")
    cm_rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d column_mask records.", len(cm_rows))

    wh_id = find_warehouse(auth)
    results: list[dict] = []
    for r in cm_rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if not isinstance(meta, dict):
            continue
        try:
            res = apply_column_mask(meta, auth=auth, wh_id=wh_id, dry_run=config.dry_run)
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": f"COLUMN_MASK_{meta.get('table_fqn', '?')}",
                "object_type": "column_mask",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Column masks worker complete. %d validated, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
