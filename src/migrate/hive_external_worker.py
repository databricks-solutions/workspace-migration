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
# Hive External Table Worker: recreates Hive external tables from
# `hive_metastore` on the source as UC external tables under
# `{hive_target_catalog}` on the target, preserving storage locations.

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl
from common.tracking import TrackingManager
from migrate.hive_common import rewrite_hive_fqn, rewrite_hive_namespace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hive_external_worker")

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
# Migrate a single Hive external table


def migrate_hive_external_table(
    table_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    explorer: CatalogExplorer,
    target_explorer: CatalogExplorer,
    wh_id: str,
) -> dict:
    """Recreate a Hive external table on the target as a UC external table."""
    source_fqn = table_info["object_name"]
    target_fqn = rewrite_hive_fqn(source_fqn, config.hive_target_catalog)

    tracker.append_migration_status(
        [
            {
                "object_name": source_fqn,
                "object_type": "hive_external",
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
        ddl = explorer.get_create_statement(source_fqn)
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": source_fqn,
            "object_type": "hive_external",
            "status": "failed",
            "error_message": f"Failed to get DDL: {exc}",
            "duration_seconds": duration,
        }

    # Rewrite hive_metastore.* -> {hive_target_catalog}.* so the DDL lands on UC
    ddl = rewrite_hive_namespace(ddl, config.hive_target_catalog)
    # Replace CREATE TABLE with CREATE TABLE IF NOT EXISTS
    ddl = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute DDL for %s -> %s", source_fqn, target_fqn)
        return {
            "object_name": source_fqn,
            "object_type": "hive_external",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Executing DDL for %s -> %s", source_fqn, target_fqn)
    result = execute_and_poll(auth, wh_id, ddl)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": source_fqn,
            "object_type": "hive_external",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    # Validate row count: source (hive_metastore) vs target (UC)
    try:
        source_count = explorer.get_table_row_count(source_fqn)
        target_count = target_explorer.get_table_row_count(target_fqn)
        match = source_count == target_count
        status = "validated" if match else "validation_failed"
        return {
            "object_name": source_fqn,
            "object_type": "hive_external",
            "status": status,
            "error_message": None if match else (f"Row count mismatch: source={source_count}, target={target_count}"),
            "source_row_count": source_count,
            "target_row_count": target_count,
            "duration_seconds": duration,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "object_name": source_fqn,
            "object_type": "hive_external",
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

    # Parse batch from for_each_task input widget
    dbutils.widgets.text("batch", "[]")
    batch_json = dbutils.widgets.get("batch")
    batch: list[dict] = json.loads(batch_json)
    logger.info("Received batch of %d Hive external tables.", len(batch))

    wh_id = find_warehouse(auth)

    # Process batch with thread pool

    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                migrate_hive_external_table,
                tbl,
                config=config,
                auth=auth,
                tracker=tracker,
                explorer=explorer,
                target_explorer=target_explorer,
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
                    "object_type": "hive_external",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            results.append(res)
            logger.info("Table %s -> %s", res["object_name"], res["status"])

    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "Hive external worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] in ("failed", "validation_failed")),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
