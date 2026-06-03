"""Reconciliation / retry-resumability pass (X.1).

Runs ONCE at the start of every ``migrate`` workflow (via
:mod:`migrate.orchestrator`). Scans ``migration_status`` for non-terminal
rows from **prior** runs and decides whether to reset them to
``pending`` so the workers re-pick them up on this run.

Contract (see ``docs/retry_resumability.md`` for the full decision
table):

================================  ==============  =========================
Latest status (prior run)                    Reconciler      Notes
===========================================  ==============  ==================
``validated``                                no-op           Terminal filtered by
                                                             ``get_pending_objects``.
``skipped_by_pipeline_migration``            no-op           Terminal ditto.
``skipped_target_exists`` (X.4)              no-op           Terminal ditto.
``skipped_by_stateful_service_migration``   no-op           Terminal ditto — STs are
                                                             handled by the future
                                                             Stateful Services Phase.
``in_progress`` (orphaned)                   RESET -> pending Worker died mid-flight.
                                                  Calls per-worker cleanup
                                                  hook; appends new
                                                  ``pending`` row.
``failed``                        no-op           Worker already re-picks
                                                  up via
                                                  ``get_pending_objects``
                                                  (non-terminal).
``validation_failed``             surface only    Operator intervention.
                                                  Left alone; appears in
                                                  ``summary``.
other ``skipped_*`` non-terminal  no-op           Config-flag skips
                                                  (iceberg / rls_cm /
                                                  dry_run) re-pickup is
                                                  correct; no cleanup
                                                  needed.
================================  ==============  =========================

Plain module (no ``# Databricks notebook source`` header) so notebooks
can import it Databricks refuses notebook-to-notebook imports at
runtime.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from common.auth import AuthManager
    from common.config import MigrationConfig
    from common.tracking import TrackingManager

logger = logging.getLogger(__name__)


# Statuses the reconciler treats as "orphaned when from a prior run".
# Everything else (terminal or transient-but-safe) is left untouched so the
# reconciler stays narrow.
_ORPHAN_CANDIDATE_STATUSES = ("in_progress",)


# Per-worker cleanup hooks. Each hook signature:
#     cleanup(object_name: str, *, auth: AuthManager, spark, config) -> None
#
# Hooks MUST be best-effort: swallow errors and log. The reset row is
# appended regardless retry is safe because X.2 idempotency gives
# workers "already-exists" tolerance; cleanup is only for cases where
# partial target state would wedge the retry (volume file bytes, share
# object adds).
#
# Populated in ``_resolve_cleanup_hooks`` (deferred import to avoid a
# module-load-time cycle volume_worker / sharing_worker both already
# import ``common.tracking`` which this module will too once the
# orchestrator wires things up).
def _resolve_cleanup_hooks() -> dict[str, Any]:
    hooks: dict[str, Any] = {}
    try:
        from migrate.volume_worker import cleanup_partial_target as _volume_cleanup

        hooks["volume"] = _volume_cleanup
    except Exception:  # noqa: BLE001 worker import failures should not crash reconcile
        logger.debug("volume_worker.cleanup_partial_target not available; skipping")

    try:
        from migrate.sharing_worker import cleanup_partial_share as _share_cleanup

        hooks["share"] = _share_cleanup
    except Exception:  # noqa: BLE001
        logger.debug("sharing_worker.cleanup_partial_share not available; skipping")

    try:
        from migrate.models_worker import cleanup_partial_target as _model_cleanup

        # C5: multi-version model copy that crashes mid-way leaves partial
        # state on target (shell + some versions + half-copied artifacts).
        # Dropping the model lets the retry start clean.
        hooks["registered_model"] = _model_cleanup
    except Exception:  # noqa: BLE001
        logger.debug("models_worker.cleanup_partial_target not available; skipping")

    return hooks


def _latest_status_rows(spark, config: MigrationConfig) -> list[dict]:
    """Return (object_name, object_type, status, job_run_id, migrated_at)
    for the most-recent row per (object_name, object_type). Empty list if
    the table does not exist yet (fresh install reconciliation no-ops)."""
    fqn = f"{config.tracking_catalog}.{config.tracking_schema}.migration_status"
    try:
        rows = spark.sql(
            f"""
            SELECT object_name, object_type, status, job_run_id, migrated_at
            FROM (
                SELECT object_name, object_type, status, job_run_id, migrated_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY object_name, object_type
                           ORDER BY migrated_at DESC
                       ) AS rn
                FROM {fqn}
            )
            WHERE rn = 1
            """
        ).collect()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Reconciliation could not read migration_status: %s", exc)
        return []
    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "object_name": r.object_name,
                "object_type": r.object_type,
                "status": r.status,
                "job_run_id": r.job_run_id,
                "migrated_at": r.migrated_at,
            }
        )
    return out


def _run_cleanup(
    *,
    object_type: str,
    object_name: str,
    hooks: dict[str, Any],
    auth: AuthManager | None,
    spark,
    config: MigrationConfig,
) -> str | None:
    """Invoke the per-worker cleanup hook if one is registered. Return the
    hook name (for logging / result accounting) or None if no hook ran.

    Errors are swallowed and logged the reset row is appended regardless
    because X.2 idempotency makes the worker retry safe. Cleanup only
    matters for workers that leave partial target state (volume file
    bytes, shares with half-added objects)."""
    hook = hooks.get(object_type)
    if hook is None:
        return None
    if auth is None:
        logger.info(
            "Skipping cleanup hook for %s %r: auth not provided (test mode).",
            object_type,
            object_name,
        )
        return None
    try:
        hook(object_name, auth=auth, spark=spark, config=config)
        logger.info("Cleanup hook %s succeeded for %s", object_type, object_name)
        return object_type
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Cleanup hook for %s %r failed (retry will still proceed): %s",
            object_type,
            object_name,
            exc,
        )
        return object_type


def reconcile_stale_runs(
    *,
    spark,
    config: MigrationConfig,
    tracker: TrackingManager,
    auth: AuthManager | None = None,
    current_job_run_id: str | None = None,
) -> dict[str, Any]:
    """Reset orphaned ``in_progress`` rows from prior runs to ``pending``.

    Emits per-object decisions as new ``migration_status`` rows (append-
    only: the latest-per-object SQL returns the new ``pending`` on the
    next ``get_pending_objects`` call). Calls per-worker cleanup hooks
    where registered.

    Arguments:
        spark: SparkSession (required for status table reads).
        config: MigrationConfig for tracking catalog/schema + dry_run.
        tracker: TrackingManager for the tracking table. Used to append
            the reset rows via ``append_migration_status``.
        auth: AuthManager. Optional omitted in unit tests that do not
            need per-worker cleanup.
        current_job_run_id: the current workflow's ``job_run_id`` (as
            resolved from ``dbutils.notebook ... getContext()``). Rows
            whose ``job_run_id`` equals this are this run's own and
            are left alone (rare: reconciliation runs before workers
            emit, but a re-run of the notebook within one job run
            must not self-reset).

    Returns a summary dict:

        {
            "reset_count": int,
            "validation_failed_count": int,
            "cleanup_count": int,
            "decisions": [...],
        }
    """
    rows = _latest_status_rows(spark, config)
    if not rows:
        logger.info("Reconciliation: no prior migration_status rows.")
        return {
            "reset_count": 0,
            "validation_failed_count": 0,
            "cleanup_count": 0,
            "decisions": [],
        }

    hooks = _resolve_cleanup_hooks()
    reset_rows: list[dict] = []
    decisions: list[dict[str, Any]] = []
    cleanup_count = 0
    validation_failed_count = 0

    for r in rows:
        status = r["status"]
        obj_name = r["object_name"]
        obj_type = r["object_type"]
        prior_run_id = r.get("job_run_id")

        if status == "validation_failed":
            validation_failed_count += 1
            decisions.append(
                {
                    "object_type": obj_type,
                    "object_name": obj_name,
                    "prior_status": status,
                    "action": "surface",
                    "cleanup": None,
                }
            )
            continue

        if status not in _ORPHAN_CANDIDATE_STATUSES:
            # failed / skipped_by_config / other non-terminal and retry-safe,
            # but the reconciler does not rewrite the row; workers'
            # get_pending query already re-picks-up on the next run.
            continue

        # Status is 'in_progress'. If it belongs to the current job run, the
        # worker is actively processing leave alone. In practice
        # reconciliation runs BEFORE any worker emits 'in_progress' this run,
        # but guarding here makes the helper safe to call from other callers
        # and from tests that seed current-run rows.
        if current_job_run_id and prior_run_id == current_job_run_id:
            continue

        cleanup = _run_cleanup(
            object_type=obj_type,
            object_name=obj_name,
            hooks=hooks,
            auth=auth,
            spark=spark,
            config=config,
        )
        if cleanup:
            cleanup_count += 1

        prior_label = prior_run_id or "unknown"
        error_msg = (
            f"Reconciled orphaned in_progress from prior run "
            f"(job_run_id={prior_label}); retrying."
        )
        reset_rows.append(
            {
                "object_name": obj_name,
                "object_type": obj_type,
                "status": "pending",
                "error_message": error_msg,
                "job_run_id": current_job_run_id,
                "task_run_id": None,
                "source_row_count": None,
                "target_row_count": None,
                "duration_seconds": None,
            }
        )
        decisions.append(
            {
                "object_type": obj_type,
                "object_name": obj_name,
                "prior_status": status,
                "action": "reset",
                "cleanup": cleanup,
            }
        )

    if reset_rows:
        if config.dry_run:
            logger.info(
                "[DRY RUN] Would reconcile %d orphaned in_progress row(s); skipping append.",
                len(reset_rows),
            )
        else:
            tracker.append_migration_status(reset_rows)

    summary = {
        "reset_count": len(reset_rows),
        "validation_failed_count": validation_failed_count,
        "cleanup_count": cleanup_count,
        "decisions": decisions,
    }
    logger.info(
        "Reconciliation: reset=%d validation_failed=%d cleanup_hooks_ran=%d",
        summary["reset_count"],
        summary["validation_failed_count"],
        summary["cleanup_count"],
    )
    return summary


# ---------------------------------------------------------------------------
# Kill-injection (test-only)
#
# ``config.test_kill_after`` makes a worker raise SystemExit after the Nth
# object in the current batch. Used by the retry/resumability integration
# fixture to simulate a crash mid-batch; the second ``migrate`` run then
# reconciles the orphaned ``in_progress`` rows.
#
# SAFETY: the field is refused at runtime unless the environment signals
# a test profile. See ``_is_test_mode`` below. Never call this helper
# from a code path a real customer can trigger.
# ---------------------------------------------------------------------------


def _is_test_mode() -> bool:
    """Return True when the environment looks like a test profile.

    Matches:
      - ``WSM_TEST_MODE=1`` env var (integration fixtures set this).
      - ``DATABRICKS_ENVIRONMENT`` starts with ``test`` (ring/staging).

    Both signals must come from outside the config file so an operator
    cannot flip kill-injection on in config.yaml and fire it in prod.
    """
    import os

    if os.environ.get("WSM_TEST_MODE") == "1":
        return True
    env = os.environ.get("DATABRICKS_ENVIRONMENT", "").lower()
    return bool(env.startswith("test"))


def maybe_kill(config: MigrationConfig, counter: int, worker_name: str) -> None:
    """Kill the current process with ``SystemExit`` when the counter
    reaches ``config.test_kill_after``. No-op when the flag is unset.

    Raises:
        RuntimeError: when the flag is set but the environment is not a
            test profile. This is the runtime guard that keeps the flag
            from leaking into prod deployments.
    """
    kill_after_raw = getattr(config, "test_kill_after", None)
    # Only honour plain ``int`` (not bool, not MagicMock, not string). Unit
    # tests pass MagicMock() as config and would otherwise trip the
    # test-profile guard on every call; prod configs set the field via
    # dataclass which enforces int | None.
    if not isinstance(kill_after_raw, int) or isinstance(kill_after_raw, bool):
        return
    if kill_after_raw <= 0:
        return
    kill_after = kill_after_raw
    if not _is_test_mode():
        msg = (
            "config.test_kill_after is set but the environment is not a "
            "test profile (WSM_TEST_MODE=1 / DATABRICKS_ENVIRONMENT=test*). "
            "This flag is test-only unset it before running against "
            "real data."
        )
        raise RuntimeError(msg)
    if counter >= kill_after:
        logger.error(
            "[TEST KILL-INJECTION] worker=%s counter=%d >= test_kill_after=%d "
            "raising SystemExit to simulate a mid-batch crash.",
            worker_name,
            counter,
            kill_after,
        )
        raise SystemExit(
            f"test_kill_after tripped in worker={worker_name} at counter={counter}"
        )


# ---------------------------------------------------------------------------
# Job-run context helpers
# ---------------------------------------------------------------------------


def resolve_current_job_run_id(dbutils: Any) -> str | None:
    """Extract the current Databricks job run ID from dbutils context.

    Returns None when not running in a Jobs context (interactive cluster
    / pytest). Never raises returning None is the right "unknown"
    signal; the reconciler treats unknown ``job_run_id`` as orphaned,
    which is the safe direction.
    """
    if dbutils is None:
        return None
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:  # noqa: BLE001
        return None
    # The getContext() JSON / dict-like value varies by runtime. Probe a
    # few shapes and return None if none match.
    for attr in ("currentRunId", "jobRunId"):
        getter = getattr(ctx, attr, None)
        if getter is None:
            continue
        try:
            val = getter()
        except Exception:  # noqa: BLE001
            continue
        if val is None:
            continue
        # Some SDK variants wrap in an Option-like; try .get() then str.
        try:
            got = val.get()
        except Exception:  # noqa: BLE001
            got = val
        if got is not None:
            return str(got)
    # Fall through: try tags().get("jobRunId")
    try:
        tags_getter = getattr(ctx, "tags", None)
        if tags_getter is not None:
            tags = tags_getter()
            for key in ("jobRunId", "runId"):
                try:
                    v = tags.get(key)
                except Exception:  # noqa: BLE001
                    v = None
                if v is None:
                    continue
                try:
                    got = v.get()
                except Exception:  # noqa: BLE001
                    got = v
                if got:
                    return str(got)
    except Exception:  # noqa: BLE001
        pass
    return None
