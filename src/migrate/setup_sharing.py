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
from migrate.rls_cm import capture_rls_cm, has_rls_cm, make_staging_table_fqn
from migrate.sharing_lib import (  # noqa: F401,F403  (re-export; runtime can't import this notebook)
    SHARE_NAME,
    _add_rls_cm_from_tables_api,
    _validate_rls_cm_strategy,
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
    # Validate config-gated flags BEFORE any side effects so operator errors
    # (bad rls_cm_strategy value) don't leave orphan shares / recipients.
    strategy = _validate_rls_cm_strategy(config)
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

    # 4a. Filter out tables with row filter / column mask. Delta Sharing
    #     refuses to share tables with legacy RLS/CM
    #     (``InvalidParameterValue: Table has row level security or column
    #     masks, which is not supported by Delta Sharing``). Strategy was
    #     already validated at the top of ``run()``; only "" reaches here
    #     today.
    #
    # Two sources feed the skip set, belt-and-braces:
    #   1. ``tracker.get_tables_with_rls_cm()`` — reads discovery_inventory.
    #   2. Live UC Tables API probe per pending managed table — catches
    #      cases where discovery's ``list_row_filters`` /
    #      ``list_column_masks`` silently suppressed an exception.
    rls_cm_fqns: set[str] = set(tracker.get_tables_with_rls_cm())
    _add_rls_cm_from_tables_api(auth, pending_tables, rls_cm_fqns)
    logger.info("RLS/CM set after live probe: %s", sorted(rls_cm_fqns))
    tables_to_share: list[dict] = []
    skipped_rls_cm: list[dict] = []
    staged_rls_cm: list[dict] = []  # Path A staging_copy counter
    run_id = getattr(config, "current_run_id", "") or "unknown"
    for t in pending_tables:
        if t["object_name"] not in rls_cm_fqns:
            tables_to_share.append(t)
            continue
        if strategy == "staging_copy":
            # Path A: copy the table into cp_migration_staging via CTAS,
            # then add the STAGING fqn to the share. Source RLS/CM is
            # never touched. Migration SPN must be a workspace admin so
            # the filter function's is_account_group_member('admins')
            # bypass returns true and the CTAS reads unfiltered rows.
            try:
                captured = capture_rls_cm(auth, t["object_name"])
                if not has_rls_cm(captured):
                    # Live probe flagged it but the policy is already gone
                    # (discovery caught a race). Safe to share as-is.
                    tables_to_share.append(t)
                    continue
                staging_fqn = make_staging_table_fqn(
                    t["object_name"], run_id, config.tracking_catalog
                )
                if not config.dry_run:
                    spark_session.sql(
                        f"CREATE OR REPLACE TABLE {staging_fqn} AS "
                        f"SELECT * FROM {t['object_name']}"
                    )
                    tracker.record_staging_created(
                        original_fqn=t["object_name"],
                        staging_fqn=staging_fqn,
                        run_id=run_id,
                    )
                # Share the staging FQN, not the original.
                staging_share_entry = dict(t)
                staging_share_entry["object_name"] = staging_fqn
                tables_to_share.append(staging_share_entry)
                staged_rls_cm.append(t)
                continue
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Failed to create staging copy for %s; table will NOT be shared. "
                    "Source state unchanged. Error: %s",
                    t["object_name"],
                    exc,
                    exc_info=True,
                )
                skipped_rls_cm.append(t)
                continue
        # strategy == "" (skip): record the table as skipped_by_rls_cm_policy
        # and don't add to the share. Source untouched.
        skipped_rls_cm.append(t)

    if staged_rls_cm:
        logger.info(
            "staging_copy: copied %d table(s) with RLS/CM into "
            "%s.cp_migration_staging. Source RLS/CM untouched. "
            "cleanup_staging will drop staging tables after migrate completes.",
            len(staged_rls_cm),
            config.tracking_catalog,
        )
        for t in staged_rls_cm:
            logger.info("  staged: %s", t["object_name"])

    if skipped_rls_cm:
        logger.warning(
            "Skipping %d managed table(s) with row filter / column mask — "
            "Delta Sharing does not support sharing these. See README.md "
            "section 'Row filter / column mask on managed tables'.",
            len(skipped_rls_cm),
        )
        for t in skipped_rls_cm:
            logger.warning("  - %s", t["object_name"])
        tracker.append_migration_status(
            [
                {
                    "object_name": t["object_name"],
                    "object_type": "managed_table",
                    "status": "skipped_by_rls_cm_policy",
                    "error_message": (
                        "Table has row filter or column mask; Delta Sharing "
                        "refuses to share it. Data was not migrated to target. "
                        "See README.md for options (migrate to ABAC, or set "
                        "rls_cm_strategy='staging_copy')."
                    ),
                    "job_run_id": None,
                    "task_run_id": None,
                    "source_row_count": None,
                    "target_row_count": None,
                    "duration_seconds": 0.0,
                }
                for t in skipped_rls_cm
            ]
        )

    # 5. Add tables to share (RLS/CM-affected tables excluded above)
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
