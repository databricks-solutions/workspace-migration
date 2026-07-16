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
    pass

# COMMAND ----------
# Hive Orchestrator: read discovery_inventory (source_type='hive'), emit per-category batches
# as task values for downstream workers. Also ensures target databases exist in
# hive_metastore so workers don't race to CREATE DATABASE.

import json
import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager
from migrate.batching import MAX_BATCH_BYTES, build_batches
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hive_orchestrator")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------

if _is_notebook():
    config = MigrationConfig.from_workspace_file()

    tracker = TrackingManager(spark, config)  # type: ignore[name-defined] # noqa: F821

    inv_fqn = f"{config.tracking_catalog}.{config.tracking_schema}.discovery_inventory"
    completed_fqn = f"{config.tracking_catalog}.{config.tracking_schema}.migration_status"

    # Subtract already-validated objects from the pending list (idempotent re-runs).
    #
    # Finding #12: discovery records hive tables as object_type='hive_table',
    # but the workers record their migration status under the CLASSIFIED type
    # ('hive_external' / 'hive_managed_nondbfs' / 'hive_managed_dbfs_root').
    # Matching the anti-join on object_type therefore NEVER subtracts an
    # already-migrated hive table, so every re-run re-creates it → the target
    # external table's ADLS path collides (LOCATION_OVERLAP). Both sides share
    # the same object_name (the source hive FQN), and hive object_names are
    # namespace-unique (`hive_metastore`.<db>.<name>), so match on object_name
    # ALONE. (hive_view / hive_function already share the type on both sides;
    # object-name matching is still correct for them.)
    _pending_sql = f"""
        SELECT i.object_name, i.object_type, i.catalog_name, i.schema_name,
               i.data_category, i.table_type, i.provider, i.storage_location
        FROM {inv_fqn} i
        LEFT ANTI JOIN (
          SELECT object_name
          FROM (
            SELECT object_name, status,
              ROW_NUMBER() OVER (PARTITION BY object_name ORDER BY migrated_at DESC) AS rn
            FROM {completed_fqn}
          ) WHERE rn = 1 AND status = 'validated'
        ) c
          ON i.object_name = c.object_name
        WHERE i.source_type = 'hive'
    """
    inventory_rows = spark.sql(_pending_sql).collect()  # noqa: F821

    # Ensure the target DATABASES exist on the TARGET workspace's hive_metastore
    # via its SQL warehouse (not source spark, which would create them in the
    # wrong metastore). Like-for-like: no UC catalog is created.
    auth = AuthManager(config, dbutils)  # type: ignore[name-defined] # noqa: F821
    wh_id = find_warehouse(auth)
    target_schemas = {r.schema_name for r in inventory_rows if r.schema_name}

    for sch in target_schemas:
        db_sql = f"CREATE DATABASE IF NOT EXISTS `hive_metastore`.`{sch}`"
        res = execute_and_poll(auth, wh_id, db_sql)
        if res["state"] != "SUCCEEDED":
            raise RuntimeError(f"Failed to create target database {sch}: {res.get('error')}")

    logger.info(
        "Target hive_metastore ready with %d database(s).",
        len(target_schemas),
    )

    # Partition by category for per-worker routing.
    by_category: dict[str, list[dict]] = {}
    for r in inventory_rows:
        rec = {
            "object_name": r.object_name,
            "object_type": r.object_type,
            "catalog_name": r.catalog_name,
            "schema_name": r.schema_name,
            "data_category": r.data_category,
            "table_type": r.table_type,
            "provider": r.provider,
            "storage_location": r.storage_location,
        }
        by_category.setdefault(r.data_category, []).append(rec)

    # Build batches per category (for_each_task consumes a JSON list).
    # Use the shared ``build_batches`` which enforces BOTH the count
    # ceiling (``batch_size``) AND the Jobs for_each 3000-byte per-
    # parameter size ceiling.
    batch_size = config.batch_size

    # Publish task values.
    _job_run_id = resolve_current_job_run_id(dbutils)  # type: ignore[name-defined] # noqa: F821
    for cat in ("hive_external", "hive_managed_nondbfs", "hive_managed_dbfs_root"):
        key = f"{cat}_batches"
        batches, oversize = build_batches(by_category.get(cat, []), batch_size)
        dbutils.jobs.taskValues.set(key=key, value=json.dumps(batches))  # type: ignore[name-defined] # noqa: F821
        logger.info("%s: %d batch(es) (%d objects)", key, len(batches), len(by_category.get(cat, [])))
        if oversize:
            # H6: see migrate/orchestrator.py for the parallel UC handler.
            tracker.append_migration_status(
                [
                    {
                        "object_name": o["object_name"],
                        "object_type": o["object_type"],
                        "status": "failed_batch_oversize",
                        "error_message": (
                            f"Stripped object JSON exceeds MAX_BATCH_BYTES={MAX_BATCH_BYTES}. "
                            "Trim heavy metadata (e.g. very long create_statement) or split the object."
                        ),
                        "job_run_id": str(_job_run_id),
                        "task_run_id": None,
                        "source_row_count": None,
                        "target_row_count": None,
                        "duration_seconds": None,
                    }
                    for o in oversize
                ]
            )
            logger.error(
                "Skipped %d %s object(s) from batches due to size cap: %s",
                len(oversize),
                cat,
                [o.get("object_name") for o in oversize],
            )

    # Views and functions are lists (not batched; workers handle topological ordering).
    dbutils.jobs.taskValues.set(  # type: ignore[name-defined] # noqa: F821
        key="hive_view_list",
        value=json.dumps(by_category.get("hive_view", []), default=str),
    )
    dbutils.jobs.taskValues.set(  # type: ignore[name-defined] # noqa: F821
        key="hive_function_list",
        value=json.dumps(by_category.get("hive_function", []), default=str),
    )

    logger.info("Hive orchestrator complete.")
