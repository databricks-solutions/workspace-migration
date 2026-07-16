# Databricks notebook source

# COMMAND ----------

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
# Setup Delta Sharing: create share, recipient, add tables, grant access,
# and ensure target catalogs/schemas exist.

import logging

from databricks.sdk.service.sharing import (
    PermissionsChange,
    Privilege,
)

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id
from migrate.sharing_lib import (  # noqa: F401,F403  (re-export; runtime can't import this notebook)
    SHARE_NAME,
    _add_rls_cm_from_tables_api,
    _validate_rls_cm_strategy,  # noqa: F401 — re-export for tests; setup_sharing no longer calls it (flag deprecated)
    add_tables_to_share,
    ensure_share_consumer_catalog,
    ensure_target_catalogs_and_schemas,
    get_or_create_recipient,
    get_or_create_share,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("setup_sharing")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:  # noqa: ARG001
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    spark_session = spark
    tracker = TrackingManager(spark_session, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

    # 1. Create or get the delta share on source
    share = get_or_create_share(auth, SHARE_NAME, dry_run=config.dry_run)

    # 2. Get target metastore global_metastore_id (format: <cloud>:<region>:<uuid>)
    target_metastore = auth.target_client.metastores.summary()
    target_metastore_id = target_metastore.global_metastore_id
    logger.info("Target global metastore ID: %s", target_metastore_id)

    # 3. Create or get recipient for target
    recipient_name = get_or_create_recipient(auth, target_metastore_id, dry_run=config.dry_run)

    # 4. Read pending managed tables from tracker
    pending_tables = tracker.get_pending_objects("managed_table")
    logger.info("Found %d pending managed tables to share.", len(pending_tables))

    # 4a. Exclude policy-protected tables (row filter / column mask / ABAC)
    #     from the share. Delta Sharing refuses them, and copying would read
    #     THROUGH the policy → silent data loss (findings #21/#16). Discovery
    #     flags these as ``policy_protected_table`` (legacy RLS/CM bound to a
    #     table + ABAC-policy-resolved tables). A live UC Tables API probe is
    #     kept as belt-and-braces for legacy RLS/CM discovery might miss.
    #     Affected tables are recorded ``skipped_policy_protected`` (terminal)
    #     and NOT migrated — surfaced in the dashboard for manual handling.
    protected_fqns: set[str] = {p["object_name"] for p in tracker.get_policy_protected_tables()}
    _add_rls_cm_from_tables_api(auth, pending_tables, protected_fqns)
    logger.info("Policy-protected tables excluded from share: %s", sorted(protected_fqns))

    tables_to_share = [t for t in pending_tables if t["object_name"] not in protected_fqns]
    skipped = [t for t in pending_tables if t["object_name"] in protected_fqns]

    if skipped:
        logger.warning(
            "Excluding %d policy-protected managed table(s) (row filter / column "
            "mask / ABAC) from migration — see the dashboard's Policy-Protected "
            "Tables panel. Migrate manually (remove policy on source → migrate → "
            "re-apply on target).",
            len(skipped),
        )
        for t in skipped:
            logger.warning("  - %s", t["object_name"])
        tracker.append_migration_status(
            [
                {
                    "object_name": t["object_name"],
                    "object_type": "managed_table",
                    "status": "skipped_policy_protected",
                    "error_message": (
                        "Protected by a row filter, column mask, or ABAC policy; "
                        "not migrated (copying reads through the policy). Migrate "
                        "manually: remove policy on source, migrate, re-apply on target."
                    ),
                    "job_run_id": None,
                    "task_run_id": None,
                    "source_row_count": None,
                    "target_row_count": None,
                    "duration_seconds": 0.0,
                }
                for t in skipped
            ]
        )

    # 5. Add tables to share (policy-protected tables excluded above)
    add_tables_to_share(auth, SHARE_NAME, tables_to_share, dry_run=config.dry_run)

    # 6. Grant SELECT on share to recipient
    if config.dry_run:
        logger.info("[DRY RUN] Would grant SELECT on '%s' to '%s'.", share, recipient_name)
    else:
        auth.source_client.shares.update_permissions(
            name=SHARE_NAME,
            changes=[
                PermissionsChange(
                    principal=recipient_name,
                    add=[Privilege.SELECT.value],
                )
            ],
        )
        logger.info("Granted SELECT on '%s' to '%s'.", share, recipient_name)

    # 7. Ensure target catalogs and schemas exist
    ensure_target_catalogs_and_schemas(auth, pending_tables, dry_run=config.dry_run)

    # 8. Create share-consumer catalog on target (reads from the share)
    ensure_share_consumer_catalog(auth, SHARE_NAME, config.dry_run)

    logger.info("Delta sharing setup complete.")


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
