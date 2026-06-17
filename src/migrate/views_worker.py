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
# Views Worker: migrates views from source to target workspace,
# respecting dependency order via topological sort.

import json
import logging
import time

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("views_worker")

# Number of passes views_worker will retry still-failing views. Covers
# the case where resolve_view_dependency_order misses an edge (e.g. a
# view that references a table via dynamic SQL that regex can't parse).
_MAX_RETRY_PASSES = 3


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Migrate a single view


def migrate_view(
    view_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    explorer: CatalogExplorer,
    wh_id: str,
) -> dict:
    """Migrate a single view to the target workspace."""
    obj_name = view_info["object_name"]

    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "view",
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
            "object_type": "view",
            "status": "failed",
            "error_message": f"Failed to get DDL: {exc}",
            "duration_seconds": duration,
        }

    # Replace CREATE VIEW with CREATE OR REPLACE VIEW
    ddl = rewrite_ddl(ddl, r"CREATE\s+VIEW\b", "CREATE OR REPLACE VIEW")

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute DDL for view %s", obj_name)
        return {
            "object_name": obj_name,
            "object_type": "view",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Executing DDL for view %s", obj_name)
    result = execute_and_poll(auth, wh_id, ddl)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "view",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    return {
        "object_name": obj_name,
        "object_type": "view",
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
    tracker.job_run_id = resolve_current_job_run_id(dbutils)
    explorer = CatalogExplorer(spark_session, auth)

    # Parse view list from task values
    view_list_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="view_list")
    views_raw: list[dict] = json.loads(view_list_json)
    logger.info("Received %d views to migrate.", len(views_raw))

    # Build ordered list using dependency resolution
    view_fqns = [v["object_name"] for v in views_raw]
    ordered_fqns = explorer.resolve_view_dependency_order(view_fqns)

    # Build a lookup for view info
    view_lookup: dict[str, dict] = {v["object_name"]: v for v in views_raw}

    logger.info("Dependency order resolved. Processing %d views.", len(ordered_fqns))

    wh_id = find_warehouse(auth)

    # Process views in dependency order. view_table_usage does not exist in
    # UC, so topological sort is best-effort (parsed from view_definition) —
    # any missed dependency edges are caught by the retry loop below: if a
    # view fails with TABLE_OR_VIEW_NOT_FOUND on pass N, its upstream may land
    # on pass N+1. Stop when a full pass produces no additional successes.

    pending_fqns: list[str] = list(ordered_fqns)
    final_by_fqn: dict[str, dict] = {}

    for pass_num in range(1, _MAX_RETRY_PASSES + 1):
        next_pending: list[str] = []
        pass_progress = False
        for fqn in pending_fqns:
            view_info = view_lookup.get(fqn)
            if view_info is None:
                logger.warning("View %s not in input list, skipping.", fqn)
                continue
            try:
                res = migrate_view(
                    view_info,
                    config=config,
                    auth=auth,
                    tracker=tracker,
                    explorer=explorer,
                    wh_id=wh_id,
                )
            except Exception as exc:  # noqa: BLE001
                res = {
                    "object_name": fqn,
                    "object_type": "view",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            if res["status"] == "validated":
                pass_progress = True
                final_by_fqn[fqn] = res
            elif res["status"] == "skipped":
                # dry_run or similar — keep as final, no retry
                final_by_fqn[fqn] = res
            else:
                # failed — possibly missing upstream. Keep the last attempt
                # recorded but try again on next pass.
                final_by_fqn[fqn] = res
                next_pending.append(fqn)
            logger.info(
                "View %s -> %s (pass %d)",
                res["object_name"],
                res["status"],
                pass_num,
            )
        if not next_pending:
            break
        if not pass_progress:
            logger.warning(
                "No view made progress on pass %d; %d still failing, giving up.",
                pass_num,
                len(next_pending),
            )
            break
        pending_fqns = next_pending

    results = list(final_by_fqn.values())

    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "Views worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
