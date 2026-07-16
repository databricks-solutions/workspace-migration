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
# Hive Managed DBFS Root Worker: two-hop staging copy of Hive managed tables
# on the source DBFS root into a MANAGED table in the target's own DBFS root
# (like-for-like — stays managed in hive_metastore, never becomes UC external).
# STAGE 1 (source-side): df.write the table data to a shared abfss staging
# path reachable by both workspaces, preserving partition layout.
# STAGE 2 (target-side): a target-warehouse CTAS reads the staged Delta data
# and writes a MANAGED table (no LOCATION) that lands in the target DBFS root.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse, warehouse_table_count
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

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


def _source_partition_columns(spark, db: str, table: str) -> list[str]:
    """Return the source table's partition column names (best-effort).

    A partitioned source must be written partitioned on the target — a plain
    ``df.write`` flattens it (review finding #4). ``DESCRIBE TABLE`` lists the
    partition columns under a ``# Partition Information`` / ``# col_name``
    section after the regular columns. Any error → treat as unpartitioned.
    """
    try:
        rows = spark.sql(f"DESCRIBE TABLE hive_metastore.`{db}`.`{table}`").collect()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read partition columns for %s.%s: %s", db, table, exc)
        return []
    cols: list[str] = []
    in_partition_section = False
    for row in rows:
        name = (row.asDict().get("col_name") or "").strip()
        if name.startswith("# Partition Information"):
            in_partition_section = True
            continue
        if name.startswith("# col_name"):
            continue
        if in_partition_section:
            if name == "" or name.startswith("#"):
                break
            cols.append(name)
    return cols


def _staging_ctas_sql(db: str, table: str, staging_path: str, partition_cols: list[str]) -> str:
    """Target-side CTAS that lands a MANAGED table in the target DBFS root.

    Reads the two-hop staging Delta directory and writes a managed table (NO
    LOCATION) in hive_metastore so it lands in the target's own DBFS root.
    Partition columns are preserved via ``PARTITIONED BY``.

    Uses ``CREATE OR REPLACE`` so the worker is re-runnable: a retry (or a
    second migration pass) overwrites the target from staging rather than
    failing on an already-existing table (idempotency — findings #8/#12/#20).
    """
    src = f"{staging_path.rstrip('/')}/{db}/{table}/"
    parts = ""
    if partition_cols:
        cols = ", ".join(f"`{c}`" for c in partition_cols)
        parts = f" PARTITIONED BY ({cols})"
    return (
        f"CREATE OR REPLACE TABLE `hive_metastore`.`{db}`.`{table}` USING DELTA{parts} "
        f"AS SELECT * FROM delta.`{src}`"
    )


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
    """Two-hop staging copy of a Hive DBFS-root managed table into the target's
    own DBFS root (like-for-like: stays managed in hive_metastore)."""
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
    if not config.hive_dbfs_staging_path:
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": "hive_dbfs_staging_path required but not set",
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

    staging_path = f"{config.hive_dbfs_staging_path.rstrip('/')}/{db}/{table}/"

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would stage %s to %s then CTAS into target DBFS root", obj_name, staging_path)
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    # STAGE 1: source-side write of table data to the shared abfss staging path
    # (reachable by both workspaces). Preserve partition layout (finding #4).
    try:
        logger.info("Reading source table %s", obj_name)
        df = spark.read.table(f"hive_metastore.`{db}`.`{table}`")
        source_row_count = df.count()
        partition_cols = _source_partition_columns(spark, db, table)
        writer = df.write.mode("overwrite").format("delta")
        if partition_cols:
            logger.info("Preserving partition columns %s for %s", partition_cols, obj_name)
            writer = writer.partitionBy(*partition_cols)
        logger.info("STAGE 1: writing %d rows to staging %s", source_row_count, staging_path)
        writer.save(staging_path)
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": f"Staging write failed: {exc}",
            "duration_seconds": duration,
        }

    # STAGE 2: target-side CTAS that reads staging and writes a MANAGED table
    # into the target's own DBFS root (no LOCATION), via the TARGET warehouse.
    target_fqn = f"`hive_metastore`.`{db}`.`{table}`"
    ctas_sql = _staging_ctas_sql(db, table, config.hive_dbfs_staging_path, partition_cols)
    logger.info("STAGE 2: creating managed target table %s from staging", target_fqn)
    result = execute_and_poll(auth, wh_id, ctas_sql)
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

    # Validate: compare the source row count to the TARGET managed table count
    # (read through the target warehouse — the target metastore isn't visible
    # to this worker's spark session).
    target_row_count = None
    try:
        target_row_count = warehouse_table_count(auth, wh_id, target_fqn)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read target count for %s: %s", target_fqn, exc)

    if isinstance(target_row_count, int) and target_row_count != source_row_count:
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "validation_failed",
            "error_message": (
                f"Row count mismatch after target CTAS: source {source_row_count}, "
                f"target managed table has {target_row_count}"
            ),
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "duration_seconds": duration,
        }

    return {
        "object_name": obj_name,
        "object_type": "hive_managed_dbfs_root",
        "status": "validated",
        "error_message": None,
        "source_row_count": source_row_count,
        "target_row_count": target_row_count if isinstance(target_row_count, int) else source_row_count,
        "duration_seconds": duration,
    }


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

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
