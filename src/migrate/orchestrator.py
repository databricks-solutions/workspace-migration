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
# UC Orchestrator: reads discovery inventory (source_type='uc'), builds
# batches per object type, and publishes them as task values for downstream
# workers.

import json
import logging

from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator")


# COMMAND ----------
# Batching helpers live in ``migrate.batching`` (plain module — notebooks
# can't import other notebooks). Re-exported here for back-compat so
# existing ``from migrate.orchestrator import build_batches`` callers
# (unit tests, downstream code) keep working.

from migrate.batching import (  # noqa: E402, F401
    MAX_BATCH_BYTES,
    _strip_heavy_fields,
    build_batches,
)
from migrate.reconciliation import (  # noqa: E402
    reconcile_stale_runs,
    resolve_current_job_run_id,
)


def check_collision_gate(spark, config) -> None:
    """Raise when the latest pre_check_results has any target_collision FAILs.

    X.4 fail-fast gate. Looks only at the latest row per ``check_name`` in
    ``pre_check_results`` so a prior run's FAIL that has since been
    resolved (operator dropped the colliding target object and re-ran
    pre_check) doesn't block this migrate.

    No-op when:
        - pre_check_results doesn't exist (fresh install, should be
          impossible because tracking.init_tracking_tables() creates it,
          but defensive: raise a clearer error than a metastore 404).
        - No target_collision row exists (pre_check hasn't been run with
          discovery, or discovery inventory was empty).
        - Latest target_collision row is PASS or WARN (skip policy).

    Raises:
        RuntimeError: when the latest ``check_target_collisions`` pre_check
            row is FAIL, instructing the operator to rerun pre_check after
            resolving or flipping ``on_target_collision`` to ``skip``.
    """
    fqn = f"{config.tracking_catalog}.{config.tracking_schema}.pre_check_results"
    try:
        rows = spark.sql(
            f"""
            SELECT status, message
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY check_name ORDER BY checked_at DESC
                ) AS rn
                FROM {fqn}
            )
            WHERE rn = 1 AND check_name = 'check_target_collisions'
            """
        ).collect()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read pre_check_results for collision gate: %s", exc)
        return
    for r in rows:
        if (r.status or "").upper() == "FAIL":
            msg = (
                "Migrate refused to start: latest pre_check recorded a "
                "target_collision FAIL. Resolve the colliding target "
                "object(s), or flip on_target_collision to 'skip', then "
                "rerun pre_check. Details: "
                + (r.message or "(no message)")
            )
            raise RuntimeError(msg)


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Notebook execution

if _is_notebook():
    config = MigrationConfig.from_workspace_file()  # type: ignore[name-defined] # noqa: F821
    # X.4: fail-fast when the latest pre_check flagged a target_collision.
    # Hive-target collisions are covered by detect_collisions too, so the
    # gate runs unconditionally here.
    check_collision_gate(spark, config)  # type: ignore[name-defined] # noqa: F821

    # X.1: reconcile orphaned in_progress rows from a prior crashed run
    # before dispatching workers. Runs ONCE here; workers themselves do
    # not re-run reconciliation. Called after the collision gate so
    # target-pre-existing-state failures fail-fast without triggering
    # cleanup hooks. Reconciliation operates on UC + governance workers;
    # the Hive chain has its own idempotency guarantees (audit:
    # TestHive*Idempotency).
    from common.auth import AuthManager  # noqa: E402 local import for notebook speed

    _job_run_id = resolve_current_job_run_id(dbutils)  # type: ignore[name-defined] # noqa: F821
    _auth = AuthManager(config, dbutils)  # type: ignore[name-defined] # noqa: F821
    _tracker = TrackingManager(spark, config)  # type: ignore[name-defined] # noqa: F821
    try:
        _recon = reconcile_stale_runs(
            spark=spark,  # type: ignore[name-defined] # noqa: F821
            config=config,
            tracker=_tracker,
            auth=_auth,
            current_job_run_id=_job_run_id,
        )
        logger.info(
            "Reconciliation complete: reset=%d validation_failed=%d cleanup_hooks=%d",
            _recon["reset_count"],
            _recon["validation_failed_count"],
            _recon["cleanup_count"],
        )
    except Exception as exc:  # noqa: BLE001
        # Reconciliation failure must not block a migrate: the worst case is
        # that workers re-pickup orphaned rows and their X.2 idempotency
        # tolerates "already exists" on retry. Surface loudly and continue.
        logger.warning("Reconciliation pass raised, continuing with migrate: %s", exc)

    spark_session = spark  # type: ignore[name-defined] # noqa: F821
    tracker = TrackingManager(spark_session, config)

    # Read discovery inventory and collect pending objects per type
    BATCHED_TYPES = ("managed_table", "external_table", "volume", "mv", "st")
    LIST_TYPES = (
        "function",
        "view",
        # Phase 3 governance object types — published even when counts
        # are zero so downstream worker tasks always have a valid JSON
        # payload to consume.
        "tag",
        "row_filter",
        "column_mask",
        "policy",
        "comment",
        "monitor",
        "registered_model",
        "connection",
        "foreign_catalog",
        "share",
        "recipient",
        "provider",
        "online_table",
    )

    batch_output: dict[str, list[str]] = {}
    list_output: dict[str, str] = {}

    for obj_type in BATCHED_TYPES:
        pending = tracker.get_pending_objects(obj_type)
        logger.info("Pending %s: %d objects", obj_type, len(pending))
        batches = build_batches(pending, config.batch_size)
        batch_output[f"{obj_type}_batches"] = batches

    for obj_type in LIST_TYPES:
        pending = tracker.get_pending_objects(obj_type)
        logger.info("Pending %s: %d objects", obj_type, len(pending))
        # Strip heavy fields for the same reason as batched types — the
        # aggregated list is also subject to the 3000-byte task-value limit.
        list_output[f"{obj_type}_list"] = json.dumps(_strip_heavy_fields(pending), default=str)

    # Publish task values for downstream workers
    for key, batches in batch_output.items():
        dbutils.jobs.taskValues.set(key=key, value=json.dumps(batches))  # type: ignore[name-defined] # noqa: F821
        logger.info("Published %s: %d batches", key, len(batches))

    for key, value in list_output.items():
        dbutils.jobs.taskValues.set(key=key, value=value)  # type: ignore[name-defined] # noqa: F821
        logger.info("Published %s", key)

    logger.info("Orchestrator complete.")
