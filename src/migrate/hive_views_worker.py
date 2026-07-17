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
# Hive Views Worker: replays Hive view DDLs into `hive_metastore` unchanged
# (like-for-like migration — no namespace rewrite), processing views in
# dependency order via a topological sort.

import json
import logging
import time
from collections import deque

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hive_views_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Topological sort for Hive views (textual heuristic)


def _sort_views_by_deps(view_ddls: dict[str, str]) -> list[str]:
    """Return view FQNs in topological order (dependencies first).

    ``view_ddls`` maps ``source_fqn`` -> DDL string. Uses a simple text search:
    if view A's DDL mentions view B's FQN (backticked or unquoted), then B
    must migrate before A. Any remaining views (cycles / unresolved) are
    appended in their original order.
    """
    all_fqns = set(view_ddls.keys())
    indeg = {v: 0 for v in all_fqns}
    deps: dict[str, set[str]] = {v: set() for v in all_fqns}

    for v, ddl in view_ddls.items():
        for other in all_fqns:
            if other == v:
                continue
            unquoted = other.strip("`").replace("`.`", ".")
            if other in ddl or unquoted in ddl:
                deps[v].add(other)
                indeg[v] += 1

    # Kahn's algorithm
    q = deque([v for v, d in indeg.items() if d == 0])
    order: list[str] = []
    while q:
        v = q.popleft()
        order.append(v)
        for w in all_fqns:
            if v in deps[w]:
                deps[w].discard(v)
                indeg[w] -= 1
                if indeg[w] == 0:
                    q.append(w)

    # If a cycle remains, append the rest in original order
    remaining = [v for v in view_ddls if v not in order]
    return order + remaining


# COMMAND ----------
# Migrate a single Hive view


def migrate_hive_view(
    view_info: dict,
    ddl: str,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    wh_id: str,
) -> dict:
    """Replay a single Hive view DDL into `hive_metastore` unchanged."""
    obj_name = view_info["object_name"]
    start = time.time()

    rewritten = ddl  # like-for-like: replay view DDL into hive_metastore as-is
    rewritten = rewrite_ddl(rewritten, r"CREATE\s+VIEW\b", "CREATE OR REPLACE VIEW")

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute DDL for hive view %s", obj_name)
        return {
            "object_name": obj_name,
            "object_type": "hive_view",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Executing DDL for hive view %s", obj_name)
    result = execute_and_poll(auth, wh_id, rewritten)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "hive_view",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    return {
        "object_name": obj_name,
        "object_type": "hive_view",
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
    tracker = TrackingManager(spark, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

    view_list_json = dbutils.jobs.taskValues.get(taskKey="hive_orchestrator", key="hive_view_list")
    views_raw: list[dict] = json.loads(view_list_json) if view_list_json else []
    logger.info("Received %d hive views to migrate.", len(views_raw))

    if not views_raw:
        logger.info("No hive views to migrate; exiting.")
        return

    view_lookup: dict[str, dict] = {v["object_name"]: v for v in views_raw}
    wh_id = find_warehouse(auth)

    # Step 1: fetch DDLs for all views.
    # SHOW CREATE TABLE's output for Hive views is not reliably replayable
    # as-is, so we build the DDL ourselves from DESCRIBE EXTENDED's
    # `View Text` field and a fully-qualified `hive_metastore` header.
    def _fetch_view_body(source_fqn: str) -> str:
        rows = spark.sql(f"DESCRIBE EXTENDED {source_fqn}").collect()
        body = None
        for row in rows:
            col = (row.col_name or "").strip().lower()
            data = (row.data_type or "").strip() if row.data_type is not None else ""
            if col in ("view text", "view original text") and data:
                body = data
                # Prefer "View Text" (resolved, fully-qualified) over "View Original Text"
                if col == "view text":
                    break
        if body is None:
            raise ValueError(f"No view body found for {source_fqn}")
        return body

    ddls: dict[str, str] = {}
    results: list[dict] = []
    for v in views_raw:
        source_fqn = v["object_name"]
        try:
            body = _fetch_view_body(source_fqn)
            # Target-side fully-qualified header
            parts = source_fqn.strip("`").split("`.`")
            target_fqn = f"`hive_metastore`.`{parts[1]}`.`{parts[2]}`"
            ddls[source_fqn] = f"CREATE OR REPLACE VIEW {target_fqn} AS {body}"
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not get DDL for %s: %s", source_fqn, exc, exc_info=True)
            results.append(
                {
                    "object_name": source_fqn,
                    "object_type": "hive_view",
                    "status": "failed",
                    "error_message": f"Failed to get DDL: {exc}",
                    "duration_seconds": 0.0,
                }
            )

    # Step 2: topological sort
    ordered = _sort_views_by_deps(ddls)
    logger.info("Dependency order resolved. Processing %d hive views.", len(ordered))

    # Step 3: mark all resolvable views as in_progress up front
    tracker.append_migration_status(
        [
            {
                "object_name": fqn,
                "object_type": "hive_view",
                "status": "in_progress",
                "error_message": None,
                "job_run_id": None,
                "task_run_id": None,
                "source_row_count": None,
                "target_row_count": None,
                "duration_seconds": None,
            }
            for fqn in ordered
        ]
    )

    # Step 4: process in dependency order (sequential)
    for source_fqn in ordered:
        view_info = view_lookup.get(source_fqn)
        if view_info is None:
            logger.warning("View %s from dependency order not in input list, skipping.", source_fqn)
            continue

        try:
            res = migrate_hive_view(
                view_info,
                ddls[source_fqn],
                config=config,
                auth=auth,
                wh_id=wh_id,
            )
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": source_fqn,
                "object_type": "hive_view",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)
        logger.info("Hive view %s -> %s", res["object_name"], res["status"])

    # Record final statuses
    tracker.append_migration_status(results)
    logger.info(
        "Hive views worker complete. %d succeeded, %d failed, %d skipped.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
        sum(1 for r in results if r["status"] == "skipped"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
