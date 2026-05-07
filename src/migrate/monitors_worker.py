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
# Lakehouse Monitors Worker (Phase 3 Task 33).
#
# POSTs the captured monitor definition to target via
# /api/2.1/unity-catalog/tables/{name}/monitor. Target table must exist
# (Phase 1 workers + Phase 2.5 MV/ST ran before this). Metric history
# does NOT transfer — monitoring restarts on target.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monitors_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def apply_monitor(mon: dict, *, auth: AuthManager, dry_run: bool) -> dict:
    table_fqn = mon.get("table_fqn", "")
    definition = mon.get("definition") or {}
    obj_key = f"MONITOR_{table_fqn}"

    # Strip fields that only apply to the source (table_name, status,
    # dashboard_id) — POST will re-populate these on target.
    body = {
        k: v
        for k, v in definition.items()
        if k
        not in (
            "table_name",
            "monitor_version",
            "status",
            "dashboard_id",
            "drift_metrics_table_name",
            "profile_metrics_table_name",
            "assets_dir",  # re-created on target
        )
    }

    # Strip outer backticks + collapse the `.` separators to dots; preserves
    # any literal backtick inside a name (``fqn.replace("`", "")`` would
    # silently collapse ``cat.sch.foo`bar`` -> ``cat.sch.foobar``).
    clean = table_fqn.strip("`").replace("`.`", ".")
    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] Would POST monitor for %s", table_fqn)
        return {
            "object_name": obj_key,
            "object_type": "monitor",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }
    try:
        auth.target_client.api_client.do(
            "POST",
            f"/api/2.1/unity-catalog/tables/{clean}/monitor",
            body=body,
        )
        return {
            "object_name": obj_key,
            "object_type": "monitor",
            "status": "validated",
            "error_message": "Metric history not transferred — target restarts from scratch.",
            "duration_seconds": time.time() - start,
        }
    except Exception as exc:  # noqa: BLE001
        # Idempotency: on retry, the monitor may already exist on target.
        # Treat "already exists" as validated so a resumed run doesn't mark
        # an otherwise-complete migration as failed.
        err_text = str(exc).lower()
        if "already" in err_text and "exists" in err_text:
            return {
                "object_name": obj_key, "object_type": "monitor",
                "status": "validated",
                "error_message": "already existed on target",
                "duration_seconds": time.time() - start,
            }
        return {
            "object_name": obj_key,
            "object_type": "monitor",
            "status": "failed",
            "error_message": str(exc),
            "duration_seconds": time.time() - start,
        }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="monitor_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d monitor records.", len(rows))

    results: list[dict] = []
    for r in rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if not isinstance(meta, dict):
            continue
        results.append(apply_monitor(meta, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Monitors worker complete. %d validated, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
