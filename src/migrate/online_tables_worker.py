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
# Online Tables migration worker. Recreates each online table on the target by
# replaying its spec (pointing at the same-named, already-migrated source Delta
# table), which triggers a fresh re-sync. Sync history/freshness is NOT
# transferred (same accepted trade-off as Vector Search re-embed). Consumes the
# orchestrator's online_table_list task value.
# Spec: docs/superpowers/specs/2026-06-03-online-tables-migration-design.md

import json
import logging
import time

from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.catalog import OnlineTable, OnlineTableSpec

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("online_tables_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def _build_online_table_spec(definition: dict) -> OnlineTableSpec:
    """Reconstruct a create-spec from the discovered online-table definition.

    The discovered ``spec`` is the GET response shape, carrying a response-only
    ``pipeline_id`` not accepted on create — drop it. ``from_dict`` parses the
    sync-mode sub-objects (run_triggered / run_continuously / perform_full_copy)
    plus primary_key_columns / timeseries_key.
    """
    spec_dict = dict(definition.get("spec") or {})
    spec_dict.pop("pipeline_id", None)
    return OnlineTableSpec.from_dict(spec_dict)


def migrate_online_table(target_client, row: dict) -> dict:
    """Migrate one online_table discovery row. Returns a status dict. Fully
    exception-safe: any error for a single online table becomes ``failed`` so
    one bad row never aborts the batch."""
    start = time.time()
    obj_name = row.get("object_name") or ""

    def _result(status: str, error: str | None = None) -> dict:
        return {
            "object_name": obj_name,
            "object_type": "online_table",
            "status": status,
            "error_message": error,
            "duration_seconds": time.time() - start,
        }

    try:
        meta = json.loads(row.get("metadata_json") or "{}")
        definition = meta.get("definition") or {}
        if not definition.get("spec"):
            return _result("failed", "discovery row has no spec in metadata_json — cannot migrate online table.")
        fqn = definition.get("name") or obj_name
        spec = _build_online_table_spec(definition)
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"online table spec rebuild failed: {exc}")

    try:
        target_client.online_tables.create(OnlineTable(name=fqn, spec=spec))
    except AlreadyExists as exc:
        return _result("skipped_target_exists", f"Online table already exists on target: {exc}")
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"online_tables.create failed: {exc}")

    return _result("created_resync_pending", None)


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(  # type: ignore[union-attr]
        taskKey="orchestrator", key="online_table_list", debugValue="[]"
    )
    rows = json.loads(rows_json) if rows_json else []
    logger.info("Received %d online_table record(s).", len(rows))

    results = [migrate_online_table(auth.target_client, row) for row in rows]
    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Online tables worker complete: %d created_resync_pending, %d skipped_target_exists, %d failed.",
        sum(1 for r in results if r["status"] == "created_resync_pending"),
        sum(1 for r in results if r["status"] == "skipped_target_exists"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
