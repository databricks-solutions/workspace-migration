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
# Hive External Table Worker: replays Hive external table DDL from the source
# `hive_metastore` into the target `hive_metastore` unchanged, preserving
# namespaces and storage locations.

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import (
    append_migration_status_via_warehouse,
    execute_and_poll,
    find_warehouse,
    rewrite_ddl,
    warehouse_table_count,
)
from migrate.hive_common import configure_adls_account_key
from migrate.reconciliation import resolve_current_job_run_id

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
    explorer: CatalogExplorer,
    wh_id: str,
    tracking_fqn: str,
    job_run_id: str | None,
    status_wh_id: str,
) -> dict:
    """Recreate a Hive external table on the target as a UC external table.

    Runs on NON-UC (No Isolation) compute so it can read the source
    hive_metastore table on ADLS via the legacy account key (UC clusters ignore
    fs.azure.account.key). Status writes and the target row-count therefore go
    through the SQL warehouse (UC-capable), not the worker's spark session.
    """
    source_fqn = table_info["object_name"]
    target_fqn = source_fqn  # like-for-like: same FQN in hive_metastore

    append_migration_status_via_warehouse(
        auth,
        status_wh_id,
        tracking_fqn,
        [
            {
                "object_name": source_fqn,
                "object_type": "hive_external",
                "status": "in_progress",
                "error_message": None,
            }
        ],
        job_run_id=job_run_id,
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

    # Like-for-like: replay the DDL as-is into hive_metastore (no rewrite).
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
        target_count = warehouse_table_count(auth, wh_id, target_fqn)
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
    job_run_id = resolve_current_job_run_id(dbutils)
    tracking_fqn = f"{config.tracking_catalog}.{config.tracking_schema}"
    # Source reads only — this worker runs on NON-UC compute (account-key ADLS
    # access). Target reads + status writes go through the warehouse.
    explorer = CatalogExplorer(spark_session, auth)

    # Parse batch from for_each_task input widget
    dbutils.widgets.text("batch", "[]")
    batch_json = dbutils.widgets.get("batch")
    batch: list[dict] = json.loads(batch_json)
    logger.info("Received batch of %d Hive external tables.", len(batch))

    # hive_metastore EXTERNAL tables on ADLS need the legacy account-key Hadoop
    # conf (UC vending doesn't cover hive_metastore LOCATION). Set it per source
    # storage account before reading — only works on classic compute (this task
    # runs on the hive_adls_classic cluster); no-op/warns on serverless.
    for _tbl in batch:
        configure_adls_account_key(spark_session, dbutils, _tbl.get("storage_location"))

    wh_id = find_warehouse(auth)
    # Tracking catalog is on the SOURCE metastore — status writes use a
    # source warehouse; target table ops use wh_id (target).
    status_wh_id = find_warehouse(auth, use_source=True)

    # Process batch with thread pool

    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                migrate_hive_external_table,
                tbl,
                config=config,
                auth=auth,
                explorer=explorer,
                wh_id=wh_id,
                tracking_fqn=tracking_fqn,
                job_run_id=job_run_id,
                status_wh_id=status_wh_id,
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

    # Record final statuses (via warehouse — worker runs on NON-UC compute)

    append_migration_status_via_warehouse(auth, status_wh_id, tracking_fqn, results, job_run_id=job_run_id)
    logger.info(
        "Hive external worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] in ("failed", "validation_failed")),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
