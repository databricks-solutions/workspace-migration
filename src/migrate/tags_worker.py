# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

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
# Tags Worker (Phase 3 Task 28).
#
# Applies tags on target securables via ALTER ... SET TAGS. Groups tags by
# (securable_type, securable_fqn, column_name) so one ALTER per object.

import json
import logging
import time
from collections import defaultdict

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tags_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def _tag_clause(pairs: list[tuple[str, str]]) -> str:
    """Render a SET TAGS (...) clause from (name, value) pairs.

    Values are wrapped in single quotes with embedded quotes escaped.
    """
    parts = [f"'{k}' = '{str(v).replace(chr(39), chr(39) * 2)}'" for k, v in pairs]
    return "(" + ", ".join(parts) + ")"


def apply_tag_group(
    group_key: tuple[str, str, str],
    tags: list[dict],
    *,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
) -> dict:
    """Apply all tags in a single (type, fqn, column) group with one ALTER."""
    securable_type, securable_fqn, column_name = group_key

    pairs = [(t["tag_name"], t.get("tag_value") or "") for t in tags]
    clause = _tag_clause(pairs)

    if securable_type == "COLUMN":
        sql = f"ALTER TABLE {securable_fqn} ALTER COLUMN `{column_name}` SET TAGS {clause}"
    else:
        sql = f"ALTER {securable_type} {securable_fqn} SET TAGS {clause}"

    obj_key = f"TAGS_{securable_type}_{securable_fqn}"
    if column_name:
        obj_key += f".{column_name}"

    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] %s", sql)
        return {
            "object_name": obj_key,
            "object_type": "tag",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }

    logger.info("Executing: %s", sql)
    result = execute_and_poll(auth, wh_id, sql)
    duration = time.time() - start
    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_key,
            "object_type": "tag",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }
    return {
        "object_name": obj_key,
        "object_type": "tag",
        "status": "validated",
        "error_message": None,
        "duration_seconds": duration,
    }


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    tags_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="tag_list")
    tag_rows: list[dict] = json.loads(tags_json)
    logger.info("Received %d tag records.", len(tag_rows))

    # metadata_json holds the tag payload; pull per-row
    parsed: list[dict] = []
    for r in tag_rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if isinstance(meta, dict):
            parsed.append(meta)

    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for t in parsed:
        key = (
            t.get("securable_type", ""),
            t.get("securable_fqn", ""),
            t.get("column_name") or "",
        )
        groups[key].append(t)

    wh_id = find_warehouse(auth)
    results: list[dict] = []
    for key, tags in groups.items():
        try:
            res = apply_tag_group(key, tags, auth=auth, wh_id=wh_id, dry_run=config.dry_run)
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": f"TAGS_{key[0]}_{key[1]}",
                "object_type": "tag",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Tags worker complete. %d validated, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
