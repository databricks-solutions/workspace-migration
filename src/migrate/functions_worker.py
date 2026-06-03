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
# Functions Worker: migrates UDFs from source to target workspace by
# extracting DDL and replaying as CREATE OR REPLACE FUNCTION.

import json
import logging
import time

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("functions_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Migrate a single function


def migrate_function(
    func_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    explorer: CatalogExplorer,
    wh_id: str,
) -> dict:
    """Migrate a single UDF to the target workspace."""
    obj_name = func_info["object_name"]

    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "function",
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
        ddl = explorer.get_function_ddl(obj_name)
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "function",
            "status": "failed",
            "error_message": f"Failed to get DDL: {exc}",
            "duration_seconds": duration,
        }

    # Replace CREATE FUNCTION with CREATE OR REPLACE FUNCTION
    ddl = rewrite_ddl(ddl, r"CREATE\s+FUNCTION\b", "CREATE OR REPLACE FUNCTION")

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute DDL for function %s", obj_name)
        return {
            "object_name": obj_name,
            "object_type": "function",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Executing DDL for function %s", obj_name)
    result = execute_and_poll(auth, wh_id, ddl)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "function",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    return {
        "object_name": obj_name,
        "object_type": "function",
        "status": "validated",
        "error_message": None,
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

    # Parse function list from task values
    function_list_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="function_list")
    functions: list[dict] = json.loads(function_list_json)
    logger.info("Received %d functions to migrate.", len(functions))

    wh_id = find_warehouse(auth)

    # Process functions sequentially

    results: list[dict] = []

    for func in functions:
        try:
            res = migrate_function(func, config=config, auth=auth, tracker=tracker, explorer=explorer, wh_id=wh_id)
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": func["object_name"],
                "object_type": "function",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)
        logger.info("Function %s -> %s", res["object_name"], res["status"])

    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "Functions worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
