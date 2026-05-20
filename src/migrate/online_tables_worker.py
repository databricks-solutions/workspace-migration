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
# Online Tables Worker.
#
# Phase 4 (this PR): online tables are hard-excluded from the core
# migration tool. Online-table index state (sync history, freshness
# cursors) is runtime state that cannot be replayed via a REST POST —
# even though the POST itself succeeds, the resulting target index
# starts from zero with no provenance link to source. Migration is
# handled by the Stateful Services Phase (separate future job) with
# proper source-table cutover semantics. See
# ``docs/stateful_services_phase.md``.
#
# This worker short-circuits all online_table rows to
# ``skipped_by_stateful_service_migration`` (terminal) so the discovery-
# inventory row reaches a clean terminal status and get_pending_objects
# stops re-emitting it.
#
# Historical note: an earlier version of this worker called
# ``POST /api/2.0/online-tables`` directly. That code is removed in
# Phase 4.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("online_tables_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


_SKIP_MESSAGE = (
    "Out of scope for the core migration tool. Online tables are migrated "
    "by the Stateful Services Phase (separate future job) with proper "
    "source-table cutover and sync rebuild. See "
    "docs/stateful_services_phase.md."
)


def apply_online_table(
    ot: dict,
    *,
    auth: AuthManager,  # noqa: ARG001 — signature kept for symmetry with other workers
    dry_run: bool,  # noqa: ARG001
) -> dict:
    """Short-circuit every online_table row to
    ``skipped_by_stateful_service_migration``.

    Phase 4 hard-exclusion: same pattern as MV / ST. Returns immediately
    without POSTing to target.
    """
    name = ot.get("online_table_fqn", "")
    obj_key = f"ONLINE_TABLE_{name}"
    start = time.time()
    logger.info("Skipping online_table %s — handled by the Stateful Services Phase.", name)
    return {
        "object_name": obj_key,
        "object_type": "online_table",
        "status": "skipped_by_stateful_service_migration",
        "error_message": _SKIP_MESSAGE,
        "duration_seconds": time.time() - start,
    }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="online_table_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d online_table records.", len(rows))

    results: list[dict] = []
    for r in rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if not isinstance(meta, dict):
            continue
        results.append(apply_online_table(meta, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Online tables worker complete. %d skipped_by_stateful_service_migration, %d failed.",
        sum(1 for r in results if r["status"] == "skipped_by_stateful_service_migration"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
