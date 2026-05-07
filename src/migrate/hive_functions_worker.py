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
# Hive Functions Worker: migrates Hive UDFs from hive_metastore to the UC target
# catalog by extracting DDL via DESCRIBE FUNCTION EXTENDED, rewriting the
# namespace, and replaying as CREATE OR REPLACE FUNCTION on the target.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl
from common.tracking import TrackingManager
from migrate.hive_common import rewrite_hive_namespace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hive_functions_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# DDL extraction for Hive functions (SQL UDFs and JVM UDFs)


def get_hive_function_ddl(spark, func_fqn: str) -> str:
    """Extract the CREATE FUNCTION DDL for a Hive function from
    ``DESCRIBE FUNCTION EXTENDED``. Handles both SQL UDFs and JVM UDFs
    (registered via ``USING JAR``).
    """
    rows = spark.sql(f"DESCRIBE FUNCTION EXTENDED {func_fqn}").collect()
    # DESCRIBE returns rows with a `function_desc` column. Parse key: value lines.
    info: dict[str, str] = {}
    for row in rows:
        text = row.function_desc
        if ":" in text:
            key, _, value = text.partition(":")
            info[key.strip()] = value.strip()

    # JVM UDF: class-backed function.
    if info.get("Class"):
        jar = info.get("Resource", "")  # may or may not be present
        if jar:
            return f"CREATE FUNCTION {func_fqn} AS '{info['Class']}' USING JAR '{jar}'"
        return f"CREATE FUNCTION {func_fqn} AS '{info['Class']}'"

    # SQL UDF: reconstruct from Body / Returns / Input (preferred) or Usage (fallback).
    body = info.get("Body", "")
    return_type = info.get("Returns", "")
    # `Input:` line carries typed parameters ("x DOUBLE, y STRING"). `Usage:` only
    # has names without types ("triple(x)") — use it only if Input is missing.
    args = info.get("Input", "")
    if not args:
        usage = info.get("Usage", "")
        if "(" in usage and ")" in usage:
            args = usage[usage.index("(") + 1 : usage.rindex(")")]
    if not body:
        raise ValueError(f"Could not extract DDL for {func_fqn}: no Body and no Class in DESCRIBE output")
    return f"CREATE FUNCTION {func_fqn}({args}) RETURNS {return_type} RETURN {body}"


# COMMAND ----------
# Migrate a single Hive function


def migrate_hive_function(
    func_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    spark,
    wh_id: str,
) -> dict:
    """Migrate a single Hive UDF to the UC target catalog."""
    obj_name = func_info["object_name"]

    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "hive_function",
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

    # Extract DDL from source (hive_metastore) via DESCRIBE FUNCTION EXTENDED
    try:
        ddl = get_hive_function_ddl(spark, obj_name)
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "hive_function",
            "status": "failed",
            "error_message": f"Failed to get DDL: {exc}",
            "duration_seconds": duration,
        }

    # Rewrite hive_metastore. -> target_catalog.
    ddl = rewrite_hive_namespace(ddl, config.hive_target_catalog)

    # Replace CREATE FUNCTION with CREATE OR REPLACE FUNCTION
    ddl = rewrite_ddl(ddl, r"CREATE\s+FUNCTION\b", "CREATE OR REPLACE FUNCTION")

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute DDL for hive function %s", obj_name)
        return {
            "object_name": obj_name,
            "object_type": "hive_function",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Executing DDL for hive function %s", obj_name)
    result = execute_and_poll(auth, wh_id, ddl)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "hive_function",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    return {
        "object_name": obj_name,
        "object_type": "hive_function",
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

    # Parse hive function list from task values (produced by hive_orchestrator)
    function_list_json = dbutils.jobs.taskValues.get(taskKey="hive_orchestrator", key="hive_function_list")
    functions: list[dict] = json.loads(function_list_json)
    logger.info("Received %d hive functions to migrate.", len(functions))

    wh_id = find_warehouse(auth)

    # Process functions sequentially

    results: list[dict] = []

    for func in functions:
        try:
            res = migrate_hive_function(
                func,
                config=config,
                auth=auth,
                tracker=tracker,
                spark=spark_session,
                wh_id=wh_id,
            )
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": func["object_name"],
                "object_type": "hive_function",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)
        logger.info("Hive function %s -> %s", res["object_name"], res["status"])

    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "Hive functions worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
