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
# Hive Managed Non-DBFS Worker: promotes Hive MANAGED tables whose storage
# lives on customer ADLS mounts (or other non-DBFS-root paths) to UC external
# tables pointing at the *same* storage path. No data is copied.

import json
import logging
import re
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
logger = logging.getLogger("hive_managed_nondbfs_worker")

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
# DDL helpers


_LOCATION_RE = re.compile(r"LOCATION\s+'[^']*'", re.IGNORECASE)


def _ensure_location_clause(ddl: str, storage_location: str) -> str:
    """Ensure the DDL contains a ``LOCATION '<storage_location>'`` clause.

    Hive's ``SHOW CREATE TABLE`` on a MANAGED table may or may not emit a
    LOCATION clause. Without one, the statement will create a UC MANAGED
    table at the catalog's default root, which is exactly what we DON'T
    want here. If a LOCATION clause already exists we leave it alone.
    """
    if _LOCATION_RE.search(ddl):
        return ddl
    if not storage_location:
        return ddl
    return f"{ddl.rstrip().rstrip(';')}\nLOCATION '{storage_location}'"


# COMMAND ----------
# Migrate a single Hive managed non-DBFS table


def migrate_hive_managed_nondbfs(
    record: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    explorer: CatalogExplorer,
    wh_id: str,
    tracking_fqn: str,
    job_run_id: str | None,
    status_wh_id: str,
) -> dict:
    """Recreate a Hive MANAGED non-DBFS-root table as a UC EXTERNAL table.

    The target points at the same storage path as the source — no data copy.
    Runs on NON-UC (No Isolation) compute for legacy account-key ADLS access to
    the source; status writes and the target row-count go through the SQL
    warehouse (UC-capable), not the worker's spark session.
    """
    source_fqn = record["object_name"]
    storage_location = record.get("storage_location", "")
    provider = (record.get("provider") or "").lower()
    target_fqn = source_fqn  # like-for-like: same FQN in hive_metastore

    append_migration_status_via_warehouse(
        auth,
        status_wh_id,
        tracking_fqn,
        [
            {
                "object_name": source_fqn,
                "object_type": "hive_managed_nondbfs",
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
            "object_type": "hive_managed_nondbfs",
            "status": "failed",
            "error_message": f"Failed to get DDL: {exc}",
            "duration_seconds": duration,
        }

    # Like-for-like: replay as-is into hive_metastore (no namespace rewrite).
    ddl = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")
    # Force a LOCATION so the managed source lands as a located table on target.
    ddl = _ensure_location_clause(ddl, storage_location)

    if not _LOCATION_RE.search(ddl):
        duration = time.time() - start
        return {
            "object_name": source_fqn,
            "object_type": "hive_managed_nondbfs",
            "status": "failed",
            "error_message": (
                "No LOCATION clause available; source storage_location missing. "
                "UC won't allow a MANAGED table at an arbitrary path."
            ),
            "duration_seconds": duration,
        }

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute DDL for %s -> %s", source_fqn, target_fqn)
        return {
            "object_name": source_fqn,
            "object_type": "hive_managed_nondbfs",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Creating UC external table %s at %s", target_fqn, storage_location)
    result = execute_and_poll(auth, wh_id, ddl)
    if result["state"] != "SUCCEEDED":
        duration = time.time() - start
        return {
            "object_name": source_fqn,
            "object_type": "hive_managed_nondbfs",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    # Non-Delta providers need partition metadata rebuilt on the target.
    if provider and provider != "delta":
        repair_sql = f"MSCK REPAIR TABLE {target_fqn}"
        logger.info("Running %s", repair_sql)
        repair_result = execute_and_poll(auth, wh_id, repair_sql)
        if repair_result["state"] != "SUCCEEDED":
            duration = time.time() - start
            return {
                "object_name": source_fqn,
                "object_type": "hive_managed_nondbfs",
                "status": "failed",
                "error_message": f"MSCK REPAIR failed: {repair_result.get('error', repair_result['state'])}",
                "duration_seconds": duration,
            }

    duration = time.time() - start

    # Validate row count — source read on the worker's (NON-UC) spark via the
    # legacy account key; target read through the warehouse (UC-capable).
    try:
        source_count = explorer.get_table_row_count(source_fqn)
        target_count = warehouse_table_count(auth, wh_id, target_fqn)
        match = source_count == target_count
        status = "validated" if match else "validation_failed"
        return {
            "object_name": source_fqn,
            "object_type": "hive_managed_nondbfs",
            "status": status,
            "error_message": None
            if match
            else (f"Row count mismatch: source={source_count}, target={target_count}"),
            "source_row_count": source_count,
            "target_row_count": target_count,
            "duration_seconds": duration,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "object_name": source_fqn,
            "object_type": "hive_managed_nondbfs",
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
    logger.info("Received batch of %d hive managed non-DBFS tables.", len(batch))

    # hive_metastore managed-non-DBFS tables live on ADLS; set the legacy
    # account-key Hadoop conf per source storage account before reading (UC
    # vending doesn't cover hive_metastore LOCATION). Classic compute only
    # (this task runs on hive_adls_classic); no-op/warns on serverless.
    for _rec in batch:
        configure_adls_account_key(spark_session, dbutils, _rec.get("storage_location"))

    wh_id = find_warehouse(auth)
    # Tracking catalog is on the SOURCE metastore — status writes use a
    # source warehouse; target table ops use wh_id (target).
    status_wh_id = find_warehouse(auth, use_source=True)

    # Process batch with thread pool

    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                migrate_hive_managed_nondbfs,
                rec,
                config=config,
                auth=auth,
                explorer=explorer,
                wh_id=wh_id,
                tracking_fqn=tracking_fqn,
                job_run_id=job_run_id,
                status_wh_id=status_wh_id,
            ): rec
            for rec in batch
        }
        for future in as_completed(futures):
            rec_info = futures[future]
            try:
                res = future.result()
            except Exception as exc:  # noqa: BLE001
                res = {
                    "object_name": rec_info["object_name"],
                    "object_type": "hive_managed_nondbfs",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            results.append(res)
            logger.info("Table %s -> %s", res["object_name"], res["status"])

    # Record final statuses (via warehouse — worker runs on NON-UC compute)

    append_migration_status_via_warehouse(auth, status_wh_id, tracking_fqn, results, job_run_id=job_run_id)
    logger.info(
        "Hive managed non-DBFS worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] in ("failed", "validation_failed")),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
