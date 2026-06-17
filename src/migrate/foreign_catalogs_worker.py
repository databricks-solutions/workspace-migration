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
# Foreign Catalogs Worker (Phase 3 Task 35 / 36 depending on plan numbering).
#
# Depends on connections_worker — the connection must exist on target first.
# Uses SDK catalogs.create with connection_name so foreign catalogs federate
# via the newly-replayed connection.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("foreign_catalogs_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def apply_foreign_catalog(fc: dict, *, auth: AuthManager, dry_run: bool) -> dict:
    name = fc["catalog_name"]
    obj_key = f"FOREIGN_CATALOG_{name}"
    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] Would create foreign catalog %s", name)
        return {
            "object_name": obj_key,
            "object_type": "foreign_catalog",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }
    try:
        auth.target_client.catalogs.create(
            name=name,
            connection_name=fc.get("connection_name"),
            options=dict(fc.get("options") or {}),
            comment=fc.get("comment"),
        )
        return {
            "object_name": obj_key,
            "object_type": "foreign_catalog",
            "status": "validated",
            "error_message": None,
            "duration_seconds": time.time() - start,
        }
    except Exception as exc:  # noqa: BLE001
        if "already" in str(exc).lower() and "exists" in str(exc).lower():
            return {
                "object_name": obj_key,
                "object_type": "foreign_catalog",
                "status": "validated",
                "error_message": "already existed on target",
                "duration_seconds": time.time() - start,
            }
        return {
            "object_name": obj_key,
            "object_type": "foreign_catalog",
            "status": "failed",
            "error_message": str(exc),
            "duration_seconds": time.time() - start,
        }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="foreign_catalog_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d foreign_catalog records.", len(rows))

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
        results.append(apply_foreign_catalog(meta, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Foreign catalogs worker complete. %d validated, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
