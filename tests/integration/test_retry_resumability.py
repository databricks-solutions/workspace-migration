# Databricks notebook source

# COMMAND ----------

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

# X.1 retry / resumability integration fixture.
#
# Simulates a mid-flight crash of the managed_table_worker, then asserts
# that a second migrate run reconciles the orphaned ``in_progress`` rows
# and drives every object to ``validated``.
#
# This notebook is designed to be chained by the uc_integration_test
# workflow AFTER seed_uc_test_data + migrate runs once to completion. It:
#
#   1. Reads ``scenario`` widget to decide what to assert:
#      - ``seed_kill``   : verify migration_status is in a mid-flight state
#                          (some ``validated``, some ``in_progress``, some
#                          ``pending``) consistent with a worker crash.
#      - ``resume``      : after a second migrate run, verify every object
#                          is ``validated`` and no ``in_progress`` leftovers
#                          survive.
#
# The kill itself is injected by ``config.test_kill_after`` — set by the
# outer workflow on the FIRST migrate run, unset on the SECOND.
#
# NOTE: this file is code-only. Executing it requires:
#   - A seeded source catalog with at least 5 managed tables (standard UC
#     seed satisfies this).
#   - A workflow task chain that runs migrate twice back-to-back with
#     ``test_kill_after: 2`` on the first run and no override on the
#     second.
#   - ``WSM_TEST_MODE=1`` set in the job env (honoured by
#     ``migrate.reconciliation.maybe_kill``).

from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.reconciliation import reconcile_stale_runs

# COMMAND ----------

dbutils.widgets.text("scenario", "resume")  # noqa: F821
scenario = dbutils.widgets.get("scenario").strip()  # noqa: F821
assert scenario in ("seed_kill", "resume"), f"Unknown scenario {scenario!r}"

config = MigrationConfig.from_workspace_file()
tracker = TrackingManager(spark, config)  # noqa: F821

errors: list[str] = []

# COMMAND ----------

status_df = tracker.get_latest_migration_status()
uc_types = ("managed_table", "external_table", "view", "function", "volume", "mv", "st")
status_df = status_df.filter(status_df.object_type.isin(list(uc_types)))

counts = {
    row["status"]: row["n"]
    for row in status_df.groupBy("status").count().withColumnRenamed("count", "n").collect()
}
print(f"[x1-assert] scenario={scenario!r} status breakdown: {counts}")

# COMMAND ----------

# --- Scenario: seed_kill ---
# First migrate run was invoked with config.test_kill_after=2. Expect to
# see some ``validated`` rows, at least one ``in_progress`` row (the one
# that caused SystemExit), and the rest either absent or ``pending``.
if scenario == "seed_kill":
    in_progress = counts.get("in_progress", 0)
    validated = counts.get("validated", 0)
    if in_progress == 0:
        errors.append(
            "Expected at least one in_progress row after kill-injection. "
            "Did the kill actually fire? Check logs for SystemExit / test_kill_after."
        )
    if validated == 0:
        errors.append(
            "Expected at least one validated row from the partially-run batch; got 0."
        )
    # Sanity: the sum of validated + in_progress + failed should be < total
    # discovered managed_table count — else there was no interrupt.
    total_processed = validated + in_progress + counts.get("failed", 0)
    discovered = spark.sql(  # noqa: F821
        f"""
        SELECT COUNT(*) AS n
        FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory
        WHERE object_type IN ("""
        + ", ".join(f"'{t}'" for t in uc_types)
        + """)
        """
    ).collect()[0]["n"]
    if total_processed >= discovered:
        errors.append(
            f"Expected migrate to have been interrupted before all {discovered} "
            f"objects landed; saw {total_processed} processed. Kill-injection "
            f"may not have fired."
        )
    else:
        print(
            f"[x1-assert] seed_kill OK: {validated} validated, {in_progress} in_progress, "
            f"{discovered - total_processed} untouched of {discovered} discovered."
        )

# COMMAND ----------

# --- Scenario: resume ---
# Second migrate run finished without the kill flag. Every row must now
# be terminal (validated or a legit skipped_*). No in_progress rows may
# survive — if any do, reconciliation failed to reset them OR a worker
# crashed AGAIN without a subsequent run.
if scenario == "resume":
    in_progress = counts.get("in_progress", 0)
    if in_progress > 0:
        # Surface the specific objects for debugging
        stuck = spark.sql(  # noqa: F821
            f"""
            SELECT object_name, object_type, job_run_id, migrated_at
            FROM {config.tracking_catalog}.{config.tracking_schema}.migration_status
            WHERE status = "in_progress"
            """
        ).collect()
        sample = [(r.object_type, r.object_name) for r in stuck[:5]]
        errors.append(
            f"Expected zero in_progress rows after reconciled retry; got {in_progress}. "
            f"Sample stuck objects: {sample}"
        )

    failed = counts.get("failed", 0)
    val_failed = counts.get("validation_failed", 0)
    if failed > 0 or val_failed > 0:
        errors.append(
            f"Expected migrate to recover cleanly; saw {failed} failed + "
            f"{val_failed} validation_failed. The retry path did not heal."
        )

    validated = counts.get("validated", 0)
    # Plus legitimate terminal skips (pipeline_migration / target_exists /
    # stateful_service_migration — streaming tables hard-excluded from
    # the core tool; migrated by the future Stateful Services Phase).
    terminal_skips = (
        counts.get("skipped_by_pipeline_migration", 0)
        + counts.get("skipped_target_exists", 0)
        + counts.get("skipped_by_stateful_service_migration", 0)
    )
    discovered = spark.sql(  # noqa: F821
        f"""
        SELECT COUNT(*) AS n
        FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory
        WHERE object_type IN ("""
        + ", ".join(f"'{t}'" for t in uc_types)
        + """)
        """
    ).collect()[0]["n"]
    if validated + terminal_skips < discovered:
        errors.append(
            f"Expected validated + terminal_skips == discovered ({discovered}); "
            f"got validated={validated}, terminal_skips={terminal_skips}. Resume "
            f"did not cover every discovered object."
        )
    else:
        print(
            f"[x1-assert] resume OK: {validated} validated, "
            f"{terminal_skips} terminal-skipped, 0 in_progress."
        )

# COMMAND ----------

# --- Reconciliation visibility check ---
# Regardless of scenario, calling reconcile_stale_runs again on the
# post-run state must be a no-op — the end-of-run state should have no
# orphaned in_progress rows owned by a DIFFERENT job_run_id than the one
# we resolve now. Current run has already ended; reconcile run here will
# see the whole table as "prior" — but since nothing is in_progress
# except (in seed_kill) the orphan we care about, the reset must be
# deterministic.
current_run_id = None  # we are running post-migrate in a fresh task
summary = reconcile_stale_runs(
    spark=spark,  # noqa: F821
    config=config,
    tracker=tracker,
    auth=None,  # no cleanup hooks exercised in this assertion pass
    current_job_run_id=current_run_id,
)
print(f"[x1-assert] post-run reconciliation summary: {summary}")

if scenario == "resume" and summary["reset_count"] != 0:
    errors.append(
        f"Post-resume reconciliation should have found no orphans to reset; "
        f"got reset_count={summary['reset_count']}."
    )

# COMMAND ----------

if errors:
    msg = "\n".join(f"  - {e}" for e in errors)
    raise AssertionError(f"X.1 retry/resumability integration assertions failed:\n{msg}")

print("[x1-assert] All X.1 retry/resumability integration assertions passed.")
