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
# Online Tables Worker (Phase 3 Task 36).
#
# Replays online tables via REST POST /api/2.0/online-tables. Depends on
# the source managed table existing on target (managed_table_worker ran
# first). Online-table index state does NOT transfer — target rebuilds.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("online_tables_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def apply_online_table(ot: dict, *, auth: AuthManager, dry_run: bool) -> dict:
    name = ot.get("online_table_fqn", "")
    definition = ot.get("definition") or {}
    obj_key = f"ONLINE_TABLE_{name}"

    # Strip fields that belong to the source runtime — target POST assigns
    # new IDs / status / unity_catalog_provisioning_state.
    body = {
        "name": name,
        "spec": definition.get("spec", {}),
    }

    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] Would POST online table %s", name)
        return {
            "object_name": obj_key,
            "object_type": "online_table",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }

    try:
        auth.target_client.api_client.do(
            "POST",
            "/api/2.0/online-tables",
            body=body,
        )
        return {
            "object_name": obj_key,
            "object_type": "online_table",
            "status": "validated",
            "error_message": "Index state rebuilds on target — initial sync runs async.",
            "duration_seconds": time.time() - start,
        }
    except Exception as exc:  # noqa: BLE001
        # Idempotency: POST /online-tables has no upsert semantic. On retry
        # the online table may already exist — treat "already exists" as
        # validated so a resumed run doesn't regress to failed.
        err_text = str(exc).lower()
        if "already" in err_text and "exists" in err_text:
            return {
                "object_name": obj_key, "object_type": "online_table",
                "status": "validated",
                "error_message": "already existed on target",
                "duration_seconds": time.time() - start,
            }
        return {
            "object_name": obj_key,
            "object_type": "online_table",
            "status": "failed",
            "error_message": str(exc),
            "duration_seconds": time.time() - start,
        }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="online_table_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d online_table records.", len(rows))

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
        results.append(apply_online_table(meta, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Online tables worker complete. %d validated, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
