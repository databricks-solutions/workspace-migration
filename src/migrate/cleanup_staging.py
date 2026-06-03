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
# Path A post-migrate cleanup task.
#
# Runs at the end of the migrate workflow when rls_cm_strategy='staging_copy'.
# For each row in rls_cm_staging_manifest with dropped_at IS NULL:
#   1. ALTER SHARE cp_migration_share REMOVE TABLE <staging_fqn>
#      (UC rejects DROP on a table still in an active Delta Share with
#      INVALID_STATE; "not in share" is swallowed for re-run idempotency.)
#   2. DROP TABLE IF EXISTS <staging_fqn>
#   3. UPDATE rls_cm_staging_manifest SET dropped_at = current_timestamp()
#
# Per-table continue-on-failure: a single bad drop doesn't block the rest;
# failures stamp drop_failed_at + drop_error for operator follow-up. Final
# RuntimeError raised if any drops failed so the operator sees the workflow
# task fail.

import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

SHARE_NAME = "cp_migration_share"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cleanup_staging")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def run(dbutils, spark) -> None:  # noqa: ARG001
    config = MigrationConfig.from_workspace_file()
    if (config.rls_cm_strategy or "").strip().lower() != "staging_copy":
        logger.info("rls_cm_strategy is not 'staging_copy'; nothing to clean up.")
        return

    auth = AuthManager(config, dbutils)  # noqa: F841 — kept for symmetry
    tracker = TrackingManager(spark, config)

    stagings = tracker.get_active_stagings()
    if not stagings:
        logger.info("No active staging tables — nothing to clean up.")
        return
    logger.info("Cleaning up %d staging table(s).", len(stagings))

    dropped = 0
    failed: list[tuple[str, str]] = []
    for row in stagings:
        staging_fqn = row["staging_fqn"]
        try:
            # Step 1: remove the staging table from cp_migration_share. UC
            # rejects DROP on a table still in an active Delta Share with
            # INVALID_STATE. "Not in share" / "not exist" swallowed so a
            # re-run after a partial cleanup still makes progress.
            try:
                spark.sql(f"ALTER SHARE {SHARE_NAME} REMOVE TABLE {staging_fqn}")
            except Exception as share_exc:  # noqa: BLE001
                msg = str(share_exc).lower()
                if not ("not" in msg and ("shared" in msg or "in share" in msg or "exist" in msg)):
                    raise
                logger.info("Staging %s already out of share; continuing.", staging_fqn)
            # Step 2: drop the staging table.
            spark.sql(f"DROP TABLE IF EXISTS {staging_fqn}")
            tracker.mark_staging_dropped(staging_fqn)
            dropped += 1
            logger.info("Dropped staging table %s.", staging_fqn)
        except Exception as exc:  # noqa: BLE001
            err_text = str(exc)
            tracker.mark_staging_drop_failed(staging_fqn, err_text)
            failed.append((staging_fqn, err_text))
            logger.error("Drop failed for %s: %s", staging_fqn, exc, exc_info=True)

    logger.info("cleanup_staging done. %d dropped, %d failed.", dropped, len(failed))
    if failed:
        lines = "\n".join(f"  {fqn}: {err[:200]}" for fqn, err in failed)
        raise RuntimeError(
            f"{len(failed)} staging table(s) failed to drop. Re-run this task "
            f"after addressing the underlying error. Tables:\n{lines}"
        )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
