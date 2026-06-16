"""Pure-Python library module for deep-cloning managed Delta/Iceberg tables.

Extracted from managed_table_worker.py (which is a Databricks notebook source
file and therefore cannot be imported at runtime).  This module contains only
the reusable functions; the notebook-level orchestration (run(), bootstrap,
widget handling) remains in managed_table_worker.py.
"""
from __future__ import annotations

import logging
import threading as _threading
import time

from databricks.sdk.errors import NotFound

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, rewrite_ddl
from common.tracking import TrackingManager
from common.validation import Validator
from migrate.reconciliation import maybe_kill

logger = logging.getLogger("clone_lib")

# ---------------------------------------------------------------------------
# X.1 kill-injection counter (test only). Module-level so the ThreadPool
# workers share it; safe to reset between test cases via _reset_kill_counter.
# ---------------------------------------------------------------------------

_kill_lock = _threading.Lock()
_kill_counter = 0


def _bump_kill_counter() -> int:
    """Atomically increment and return the post-increment counter value."""
    global _kill_counter
    with _kill_lock:
        _kill_counter += 1
        return _kill_counter


def _reset_kill_counter() -> None:
    """Reset the counter (test helper)."""
    global _kill_counter
    with _kill_lock:
        _kill_counter = 0


def _target_table_exists(auth: AuthManager, target_fqn: str) -> bool:
    """Whether ``target_fqn`` exists on the TARGET workspace.

    Critically uses ``auth.target_client`` (the target metastore), NOT the
    source spark session — source and target share the same FQN, so a source
    spark ``DESCRIBE TABLE`` would always find the source table and wrongly
    report the target as present (review finding #2 live regression). On any
    non-NotFound error we return False so an existence-check hiccup never
    blocks the migration (CREATE OR REPLACE is correct for a fresh target).
    """
    dotted = target_fqn.replace("`", "")
    try:
        auth.target_client.tables.get(full_name=dotted)
        return True
    except NotFound:
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not determine target existence for %s: %s; proceeding to clone.", dotted, exc)
        return False


def clone_table(
    table_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    validator: Validator,
    wh_id: str,
    share_name: str,
    target_fqn: str | None = None,
    object_type: str = "managed_table",
) -> dict:
    """Deep clone a single managed table from delta share to target.

    Path A staging_copy: tables with RLS/CM are CTAS'd into a staging
    schema by ``setup_sharing`` and the staging FQN is added to the
    share. ``tracker.get_staging_for_original`` returns the staging FQN
    so this worker can DEEP CLONE the staging copy directly. Tables
    without staging entries DEEP CLONE the original consumer-side path.
    Row-count validation is identical for both paths.
    """
    obj_name = table_info["object_name"]
    # X.1 kill-injection: simulated mid-batch crash for integration tests.
    # No-op in production (test_kill_after=None). See reconciliation.maybe_kill.
    _n = _bump_kill_counter()
    maybe_kill(config, _n, "managed_table_worker")
    parts = obj_name.strip("`").split("`.`")
    if len(parts) != 3:
        return {
            "object_name": obj_name,
            "object_type": object_type,
            "status": "failed",
            "error_message": f"Malformed FQN: {obj_name}",
            "duration_seconds": 0.0,
        }

    _catalog, schema, table = parts
    target_fqn = target_fqn or obj_name  # default: same FQN on target
    consumer_catalog = f"{share_name}_consumer"
    fmt = (table_info.get("format") or "delta").lower()

    # Record in-progress
    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": object_type,
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

    # --- Iceberg (UC-managed native Iceberg) — Option A: DDL replay + re-ingest ---
    if fmt == "iceberg":
        if config.iceberg_strategy != "ddl_replay":
            duration = time.time() - start
            logger.warning(
                "Skipping Iceberg table %s — set config.iceberg_strategy='ddl_replay' to opt in.",
                obj_name,
            )
            # Use ``skipped_by_config`` rather than plain ``skipped`` so that a
            # subsequent run with iceberg_strategy='ddl_replay' picks these
            # tables back up — ``get_pending_objects`` excludes 'validated'
            # and 'skipped' but not status suffixes.
            return {
                "object_name": obj_name,
                "object_type": object_type,
                "status": "skipped_by_config",
                "error_message": (
                    "Iceberg migration not enabled. Set iceberg_strategy='ddl_replay' "
                    "in config to opt into Option A (loses snapshot history, time travel, "
                    "branches/tags — target gets a fresh compacted state)."
                ),
                "duration_seconds": duration,
            }

        # create_statement is stripped from for_each batch payloads to stay
        # under Jobs' 3000-byte limit; re-hydrate from discovery_inventory.
        create_stmt = table_info.get("create_statement") or ""
        if not create_stmt:
            full_row = tracker.get_row("managed_table", obj_name)
            create_stmt = (full_row or {}).get("create_statement") or ""
        if not create_stmt:
            return {
                "object_name": obj_name,
                "object_type": object_type,
                "status": "failed",
                "error_message": "Iceberg DDL replay requires create_statement in discovery row",
                "duration_seconds": time.time() - start,
            }

        # Idempotency (review finding #3): make CREATE non-destructive-on-retry
        # (IF NOT EXISTS) and the load replace-not-append (INSERT OVERWRITE), so
        # a retry after a successful first attempt can't double the rows.
        create_stmt = rewrite_ddl(create_stmt, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")
        insert_sql = f"INSERT OVERWRITE {target_fqn} SELECT * FROM `{consumer_catalog}`.`{schema}`.`{table}`"

        if config.dry_run:
            duration = time.time() - start
            logger.info("[DRY RUN] Would execute: %s", create_stmt)
            logger.info("[DRY RUN] Would execute: %s", insert_sql)
            return {
                "object_name": obj_name,
                "object_type": object_type,
                "status": "skipped",
                "error_message": "dry_run",
                "duration_seconds": duration,
            }

        logger.info("Creating Iceberg table on target via DDL: %s", obj_name)
        res = execute_and_poll(auth, wh_id, create_stmt)
        if res["state"] != "SUCCEEDED":
            return {
                "object_name": obj_name,
                "object_type": object_type,
                "status": "failed",
                "error_message": f"Iceberg CREATE failed: {res.get('error', res['state'])}",
                "duration_seconds": time.time() - start,
            }

        logger.info("Re-ingesting Iceberg table data via share: %s", obj_name)
        res = execute_and_poll(auth, wh_id, insert_sql)
        if res["state"] != "SUCCEEDED":
            return {
                "object_name": obj_name,
                "object_type": object_type,
                "status": "failed",
                "error_message": f"Iceberg INSERT failed: {res.get('error', res['state'])}",
                "duration_seconds": time.time() - start,
            }

        duration = time.time() - start
        # Validate row count for Iceberg too
        try:
            validation = validator.validate_row_count(obj_name, target_fqn)
            status = "validated" if validation["match"] else "validation_failed"
            err = (
                None
                if validation["match"]
                else (f"Row count mismatch: source={validation['source_count']}, target={validation['target_count']}")
            )
            return {
                "object_name": obj_name,
                "object_type": object_type,
                "status": status,
                "error_message": err,
                "source_row_count": validation["source_count"],
                "target_row_count": validation["target_count"],
                "duration_seconds": duration,
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "object_name": obj_name,
                "object_type": object_type,
                "status": "validation_failed",
                "error_message": f"Iceberg validation error: {exc}",
                "duration_seconds": duration,
            }

    # --- Delta (default) — DEEP CLONE from staging consumer path if staging
    #     exists (Path A staging_copy), else from the original consumer path.
    consumer_fqn = f"`{consumer_catalog}`.`{schema}`.`{table}`"
    staging_fqn = tracker.get_staging_for_original(obj_name) if tracker is not None else None
    if staging_fqn:
        # Path A: the staging table was CTAS'd from the source into the source
        # workspace's `cp_migration_staging` schema, so it's a regular Delta
        # table with full schema and properties preserved — DEEP CLONE works
        # without a CTAS fallback. Build the consumer-side path:
        #   `<consumer>.cp_migration_staging.<staging_table_name>`.
        # `staging_fqn` looks like `tcat`.`cp_migration_staging`.`stg_abc...`;
        # extract just the trailing identifier.
        staging_table_name = staging_fqn.rstrip("`").split("`")[-1]
        staging_consumer_fqn = (
            f"`{consumer_catalog}`.`cp_migration_staging`.`{staging_table_name}`"
        )
        sql = f"CREATE OR REPLACE TABLE {target_fqn} DEEP CLONE {staging_consumer_fqn}"
        logger.info(
            "Executing DEEP CLONE from staging for %s (staging=%s)", obj_name, staging_fqn
        )
    else:
        sql = f"CREATE OR REPLACE TABLE {target_fqn} DEEP CLONE {consumer_fqn}"
        logger.info("Executing DEEP CLONE for %s", obj_name)

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute: %s", sql)
        return {
            "object_name": obj_name,
            "object_type": object_type,
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    # Existence gate (review finding #2): CREATE OR REPLACE … DEEP CLONE is
    # unconditionally destructive. If the target already exists and the
    # operator hasn't opted into overwrite, do NOT re-clone — validate the
    # existing target instead. This closes the orphan-replay window where a
    # crash between clone and status-write leaves an in_progress row that
    # reconciliation resets to pending, re-feeding the worker. Foreign
    # pre-existing targets are caught earlier by collision pre-check.
    overwrite = getattr(config, "overwrite_existing", False)
    if not overwrite and _target_table_exists(auth, target_fqn):
        logger.info(
            "Target %s already exists; validating without re-clone "
            "(set overwrite_existing=true to force CREATE OR REPLACE).",
            target_fqn,
        )
        duration = time.time() - start
    else:
        result = execute_and_poll(auth, wh_id, sql)
        duration = time.time() - start
        if result["state"] != "SUCCEEDED":
            return {
                "object_name": obj_name,
                "object_type": object_type,
                "status": "failed",
                "error_message": result.get("error", result["state"]),
                "duration_seconds": duration,
            }

    # Validate row count
    try:
        validation = validator.validate_row_count(obj_name, target_fqn)
        status = "validated" if validation["match"] else "validation_failed"
        return {
            "object_name": obj_name,
            "object_type": object_type,
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
            "object_name": obj_name,
            "object_type": object_type,
            "status": "validation_failed",
            "error_message": f"Validation error: {exc}",
            "duration_seconds": duration,
        }
