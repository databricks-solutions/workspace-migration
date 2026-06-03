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
from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl
from common.tracking import TrackingManager
from common.validation import Validator
from migrate.hive_common import rewrite_hive_fqn, rewrite_hive_namespace

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
    tracker: TrackingManager,
    explorer: CatalogExplorer,
    validator: Validator,
    wh_id: str,
) -> dict:
    """Recreate a Hive MANAGED non-DBFS-root table as a UC EXTERNAL table.

    The target points at the same storage path as the source — no data copy.
    """
    source_fqn = record["fqn"]
    storage_location = record.get("storage_location", "")
    provider = (record.get("provider") or "").lower()
    target_fqn = rewrite_hive_fqn(source_fqn, config.hive_target_catalog)

    tracker.append_migration_status(
        [
            {
                "object_name": source_fqn,
                "object_type": "hive_managed_nondbfs",
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
            "object_type": "hive_managed_nondbfs",
            "status": "failed",
            "error_message": f"Failed to get DDL: {exc}",
            "duration_seconds": duration,
        }

    # Rewrite hive_metastore.* references -> <hive_target_catalog>.*
    ddl = rewrite_hive_namespace(ddl, config.hive_target_catalog)
    # CREATE TABLE -> CREATE TABLE IF NOT EXISTS
    ddl = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")
    # Force the target to be EXTERNAL by making sure LOCATION is present.
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

    # Validate row count — source under hive_metastore, target under UC catalog.
    try:
        validation = validator.validate_row_count(source_fqn, target_fqn)
        status = "validated" if validation["match"] else "validation_failed"
        return {
            "object_name": source_fqn,
            "object_type": "hive_managed_nondbfs",
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
    tracker = TrackingManager(spark_session, config)
    explorer = CatalogExplorer(spark_session, auth)

    # Build a target explorer for validation (shares the same spark session)
    target_explorer = CatalogExplorer(spark_session, auth)
    validator = Validator(explorer, target_explorer)

    # Parse batch from for_each_task input widget
    dbutils.widgets.text("batch", "[]")
    batch_json = dbutils.widgets.get("batch")
    batch: list[dict] = json.loads(batch_json)
    logger.info("Received batch of %d hive managed non-DBFS tables.", len(batch))

    wh_id = find_warehouse(auth)

    # Process batch with thread pool

    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                migrate_hive_managed_nondbfs,
                rec,
                config=config,
                auth=auth,
                tracker=tracker,
                explorer=explorer,
                validator=validator,
                wh_id=wh_id,
            ): rec
            for rec in batch
        }
        for future in as_completed(futures):
            rec_info = futures[future]
            try:
                res = future.result()
            except Exception as exc:  # noqa: BLE001
                res = {
                    "object_name": rec_info["fqn"],
                    "object_type": "hive_managed_nondbfs",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            results.append(res)
            logger.info("Table %s -> %s", res["object_name"], res["status"])

    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "Hive managed non-DBFS worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] in ("failed", "validation_failed")),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
