# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass  # not running under a Databricks notebook (e.g. pytest)

# COMMAND ----------
# External Table Worker: recreates external tables on the target workspace
# using CREATE TABLE statements from the source.

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl
from common.tracking import TrackingManager
from common.validation import Validator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("external_table_worker")

MAX_WORKERS = 4


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Migrate a single external table


def migrate_external_table(
    table_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    explorer: CatalogExplorer,
    validator: Validator,
    wh_id: str,
) -> dict:
    """Recreate an external table on the target workspace."""
    obj_name = table_info["object_name"]

    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "external_table",
                "status": "in_progress",
                "error_message": None,
                "job_run_id": None,
                "task_run_id": None,
                "source_row_count": None,
                "target_row_count": None,
                "duration_seconds": None,
            }
        ]
    )

    start = time.time()

    try:
        ddl = explorer.get_create_statement(obj_name)
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "external_table",
            "status": "failed",
            "error_message": f"Failed to get DDL: {exc}",
            "duration_seconds": duration,
        }

    # Strip any row filter / column mask clauses — filter/mask functions
    # aren't migrated yet at this stage (functions_worker runs after
    # tables), so replaying the DDL with them inline would fail with
    # ROUTINE_NOT_FOUND. row_filters_worker / column_masks_worker apply
    # them later.
    from common.catalog_utils import CatalogExplorer

    ddl = CatalogExplorer.strip_filter_mask_clauses(ddl)

    # Replace CREATE TABLE with CREATE TABLE IF NOT EXISTS
    ddl = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute DDL for %s", obj_name)
        return {
            "object_name": obj_name,
            "object_type": "external_table",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Executing DDL for %s", obj_name)
    result = execute_and_poll(auth, wh_id, ddl)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "external_table",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    # Validate row count
    try:
        validation = validator.validate_row_count(obj_name, obj_name)
        status = "validated" if validation["match"] else "validation_failed"
        return {
            "object_name": obj_name,
            "object_type": "external_table",
            "status": status,
            "error_message": None
            if validation["match"]
            else (f"Row count mismatch: source={validation['source_count']}, target={validation['target_count']}"),
            "source_row_count": validation["source_count"],
            "target_row_count": validation["target_count"],
            "duration_seconds": duration,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "object_name": obj_name,
            "object_type": "external_table",
            "status": "validation_failed",
            "error_message": f"Validation error: {exc}",
            "duration_seconds": duration,
        }


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    spark_session = spark
    tracker = TrackingManager(spark_session, config)
    explorer = CatalogExplorer(spark_session, auth)

    # Build a target explorer for validation (shares the same spark session)
    target_explorer = CatalogExplorer(spark_session, auth)
    validator = Validator(explorer, target_explorer)

    # Parse batch from for_each_task input widget
    dbutils.widgets.text("batch", "[]")
    batch_json = dbutils.widgets.get("batch")
    batch: list[dict] = json.loads(batch_json)
    logger.info("Received batch of %d external tables.", len(batch))

    wh_id = find_warehouse(auth)

    # Process batch with thread pool

    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                migrate_external_table,
                tbl,
                config=config,
                auth=auth,
                tracker=tracker,
                explorer=explorer,
                validator=validator,
                wh_id=wh_id,
            ): tbl
            for tbl in batch
        }
        for future in as_completed(futures):
            tbl_info = futures[future]
            try:
                res = future.result()
            except Exception as exc:  # noqa: BLE001
                res = {
                    "object_name": tbl_info["object_name"],
                    "object_type": "external_table",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            results.append(res)
            logger.info("Table %s -> %s", res["object_name"], res["status"])

    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "External table worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] in ("failed", "validation_failed")),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
