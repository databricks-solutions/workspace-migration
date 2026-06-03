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
# Online Tables migration worker. Legacy online tables are deprecated and can no
# longer be created, so this converts each discovered online table into a
# Lakebase SYNCED TABLE on the target: ensure a shared Lakebase database instance
# exists (create-if-missing), then create_synced_database_table from the online
# table's source. Consumer apps must repoint to the new Postgres endpoint (out of
# scope). Consumes the orchestrator's online_table_list task value.
# Spec: docs/superpowers/specs/2026-06-03-online-tables-to-synced-tables-design.md

import contextlib
import json
import logging
import time

from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.database import (
    DatabaseInstance,
    SyncedDatabaseTable,
    SyncedTableSchedulingPolicy,
    SyncedTableSpec,
)

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


def _scheduling_policy(spec: dict) -> SyncedTableSchedulingPolicy:
    """Map the online table's sync mode to a synced-table scheduling policy."""
    if spec.get("run_continuously") is not None:
        return SyncedTableSchedulingPolicy.CONTINUOUS
    if spec.get("perform_full_copy"):
        return SyncedTableSchedulingPolicy.SNAPSHOT
    return SyncedTableSchedulingPolicy.TRIGGERED  # run_triggered or unspecified


def _build_synced_table_spec(definition: dict) -> SyncedTableSpec:
    """Build a SyncedTableSpec from the discovered online-table definition."""
    spec = definition.get("spec") or {}
    return SyncedTableSpec(
        source_table_full_name=spec.get("source_table_full_name"),
        primary_key_columns=list(spec.get("primary_key_columns") or []),
        timeseries_key=spec.get("timeseries_key"),
        scheduling_policy=_scheduling_policy(spec),
    )


def _instance_ready(inst: object) -> bool:
    return "AVAILABLE" in str(getattr(inst, "state", "")).upper()


def _ensure_lakebase_instance(
    target_client,
    name: str,
    capacity: str,
    *,
    max_attempts: int = 120,
    sleep_seconds: float = 15.0,
    sleep_fn=time.sleep,
) -> bool:
    """Ensure the target Lakebase database instance exists and is AVAILABLE.
    Create-if-missing (VS-endpoint-style), poll up to ~30 min. Returns ready?."""
    try:
        inst = target_client.database.get_database_instance(name)
        if _instance_ready(inst):
            return True
    except Exception:  # noqa: BLE001 — absent or transient; create then poll
        with contextlib.suppress(AlreadyExists):
            target_client.database.create_database_instance(DatabaseInstance(name=name, capacity=capacity))

    for _ in range(max_attempts):
        try:
            inst = target_client.database.get_database_instance(name)
            if _instance_ready(inst):
                return True
        except Exception:  # noqa: BLE001 — keep polling
            pass
        sleep_fn(sleep_seconds)
    return False


def migrate_online_table(
    target_client,
    row: dict,
    config,
    *,
    max_attempts: int = 120,
    sleep_seconds: float = 15.0,
    sleep_fn=time.sleep,
) -> dict:
    """Convert one online_table discovery row into a Lakebase synced table.
    Fully exception-safe (one bad row never aborts the batch)."""
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
        if not (definition.get("spec") or {}).get("source_table_full_name"):
            return _result("failed", "online table has no source_table_full_name — cannot convert to synced table.")
        fqn = definition.get("name") or obj_name
        spec = _build_synced_table_spec(definition)
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"synced-table spec rebuild failed: {exc}")

    try:
        ready = _ensure_lakebase_instance(
            target_client, config.lakebase_instance_name, config.lakebase_capacity,
            max_attempts=max_attempts, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn,
        )
    except Exception as exc:  # noqa: BLE001 — instance setup error (e.g. bad name/quota) fails THIS row only
        return _result("failed", f"Lakebase instance setup failed: {exc}")
    if not ready:
        return _result(
            "skipped_instance_not_ready",
            f"Lakebase instance '{config.lakebase_instance_name}' not AVAILABLE within wait budget; "
            "a re-run will retry this online table.",
        )

    try:
        target_client.database.create_synced_database_table(
            SyncedDatabaseTable(
                name=fqn,
                database_instance_name=config.lakebase_instance_name,
                logical_database_name=config.lakebase_logical_database,
                spec=spec,
            )
        )
    except AlreadyExists as exc:
        return _result("skipped_target_exists", f"Synced table already exists on target: {exc}")
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"create_synced_database_table failed: {exc}")

    return _result("created_resync_pending", None)


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(  # type: ignore[union-attr]
        taskKey="orchestrator", key="online_table_list", debugValue="[]"
    )
    rows = json.loads(rows_json) if rows_json else []
    logger.info("Received %d online_table record(s) to convert to synced tables.", len(rows))

    results = [migrate_online_table(auth.target_client, row, config) for row in rows]
    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Online tables worker complete: %d created_resync_pending, %d skipped_target_exists, "
        "%d skipped_instance_not_ready, %d failed.",
        sum(1 for r in results if r["status"] == "created_resync_pending"),
        sum(1 for r in results if r["status"] == "skipped_target_exists"),
        sum(1 for r in results if r["status"] == "skipped_instance_not_ready"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
