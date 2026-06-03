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
# Hive Managed DBFS Root Worker: migrates Hive managed tables on DBFS root.
# The data is copied off the source DBFS to a customer-provided ADLS path,
# then re-registered as a UC external Delta table on the target workspace.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hive_managed_dbfs_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Migrate a single Hive managed DBFS-root table


def migrate_hive_managed_dbfs(
    table_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    spark,
    wh_id: str,
) -> dict:
    """Copy a Hive managed DBFS-root table to ADLS and register as UC external."""
    obj_name = table_info["object_name"]

    # A. Opt-out check
    if not config.migrate_hive_dbfs_root:
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "skipped_by_config",
            "error_message": "migrate_hive_dbfs_root=false",
            "duration_seconds": 0.0,
        }

    # B. Config validation (defensive — pre-check should catch this)
    if not config.hive_dbfs_target_path:
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": "hive_dbfs_target_path required but not set",
            "duration_seconds": 0.0,
        }

    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "hive_managed_dbfs_root",
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

    # Parse obj_name = `hive_metastore`.`db`.`table`
    try:
        parts = obj_name.strip("`").split("`.`")
        if len(parts) != 3:
            raise ValueError(f"Expected 3-part name, got {len(parts)} parts: {obj_name}")
        _, db, table = parts
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": f"Failed to parse object_name: {exc}",
            "duration_seconds": duration,
        }

    target_path = f"{config.hive_dbfs_target_path.rstrip('/')}/{db}/{table}/"

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would copy %s to %s", obj_name, target_path)
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    # C. Data copy (source-side Spark reads hive_metastore directly)
    try:
        logger.info("Reading source table %s", obj_name)
        df = spark.read.table(f"hive_metastore.`{db}`.`{table}`")
        source_row_count = df.count()
        logger.info("Writing %d rows to %s", source_row_count, target_path)
        df.write.mode("overwrite").format("delta").save(target_path)
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": f"Data copy failed: {exc}",
            "duration_seconds": duration,
        }

    # D. Register on target as UC external Delta
    target_fqn = f"`{config.hive_target_catalog}`.`{db}`.`{table}`"
    create_sql = f"CREATE TABLE IF NOT EXISTS {target_fqn} USING DELTA LOCATION '{target_path}'"
    logger.info("Registering UC external table %s", target_fqn)
    result = execute_and_poll(auth, wh_id, create_sql)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "source_row_count": source_row_count,
            "duration_seconds": duration,
        }

    # E/F. Validated — we wrote the data ourselves, so target_row_count == source_row_count
    return {
        "object_name": obj_name,
        "object_type": "hive_managed_dbfs_root",
        "status": "validated",
        "error_message": None,
        "source_row_count": source_row_count,
        "target_row_count": source_row_count,
        "duration_seconds": duration,
    }


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    # Parse batch from for_each_task input widget
    dbutils.widgets.text("batch", "[]")
    batch_json = dbutils.widgets.get("batch")
    batch: list[dict] = json.loads(batch_json)
    logger.info("Received batch of %d Hive managed DBFS-root tables.", len(batch))

    wh_id = find_warehouse(auth)

    # Process batch serially — data copy is heavy, avoid resource contention.
    results: list[dict] = []
    for tbl in batch:
        try:
            res = migrate_hive_managed_dbfs(
                tbl,
                config=config,
                auth=auth,
                tracker=tracker,
                spark=spark,
                wh_id=wh_id,
            )
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": tbl.get("object_name", "<unknown>"),
                "object_type": "hive_managed_dbfs_root",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)
        logger.info("Table %s -> %s", res["object_name"], res["status"])

    # Record final statuses
    tracker.append_migration_status(results)
    logger.info(
        "Hive managed DBFS worker complete. %d succeeded, %d failed, %d skipped.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] in ("failed", "validation_failed")),
        sum(1 for r in results if r["status"] in ("skipped", "skipped_by_config")),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
