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
# MV / ST Worker.
#
# Phase 4 (this PR): both materialized views (MV) AND streaming tables
# (ST) are hard-excluded from the core migration tool. The Stateful
# Services Phase — a separate future job — will migrate them properly,
# including stream state (Kafka offsets, Auto Loader checkpoints,
# Delta CDF cursors) for ST and pipeline state for MV. See
# ``docs/stateful_services_phase.md``.
#
# This worker short-circuits all MV / ST rows to
# ``skipped_by_stateful_service_migration`` (terminal) so the discovery-
# inventory row reaches a clean terminal status and downstream
# get_pending_objects stops re-emitting it.
#
# Historical note: an earlier version of this worker had a DDL-replay
# path for SQL-created MVs (distinguished by empty ``spec.libraries`` on
# the backing pipeline). That code is removed in Phase 4 — the Stateful
# Services Phase will rebuild MV migration with proper state handling.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mv_st_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Migrate a single MV or ST — hard-excluded


_SKIP_MESSAGE = (
    "Out of scope for the core migration tool. MV / ST are migrated by the "
    "Stateful Services Phase (separate future job) with proper stream / "
    "pipeline state handling. See docs/stateful_services_phase.md."
)


def migrate_mv_st(
    obj_info: dict,
    *,
    config: MigrationConfig,  # noqa: ARG001 — signature kept for symmetry with other workers
    auth: AuthManager,  # noqa: ARG001
    tracker: TrackingManager,  # noqa: ARG001
    wh_id: str,  # noqa: ARG001
) -> dict:
    """Short-circuit both MV and ST to ``skipped_by_stateful_service_migration``.

    Phase 4 hard-exclusion: same pattern as PR #41 (ST). Returns
    immediately without touching target or source.
    """
    obj_name = obj_info["object_name"]
    obj_type = obj_info["object_type"]  # "mv" or "st"
    start = time.time()
    logger.info(
        "Skipping %s %s — handled by the Stateful Services Phase.",
        obj_type,
        obj_name,
    )
    return {
        "object_name": obj_name,
        "object_type": obj_type,
        "status": "skipped_by_stateful_service_migration",
        "error_message": _SKIP_MESSAGE,
        "duration_seconds": time.time() - start,
    }


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    dbutils.widgets.text("batch", "[]")
    batch: list[dict] = json.loads(dbutils.widgets.get("batch"))
    logger.info("Received batch of %d MV/ST objects.", len(batch))

    results: list[dict] = []
    for obj in batch:
        try:
            res = migrate_mv_st(
                obj,
                config=config,
                auth=auth,
                tracker=tracker,
                wh_id="",  # not used after Phase 4 hard-exclusion
            )
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": obj["object_name"],
                "object_type": obj.get("object_type", "mv"),
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)
        logger.info("%s %s -> %s", res["object_type"], res["object_name"], res["status"])

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "MV/ST worker complete. %d skipped_by_stateful_service_migration, %d failed.",
        sum(1 for r in results if r["status"] == "skipped_by_stateful_service_migration"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
