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
    AuthenticationType,
    PermissionsChange,
    Privilege,
    SharedDataObject,
    SharedDataObjectUpdate,
    SharedDataObjectUpdateAction,
)

try:
    from databricks.sdk.service.sharing import SharedDataObjectDataObjectType as _DataObjectType  # type: ignore

    _TABLE_TYPE: object = _DataObjectType.TABLE
except ImportError:
    _TABLE_TYPE = "TABLE"

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.rls_cm import capture_rls_cm, has_rls_cm, make_staging_table_fqn, restore_rls_cm, strip_rls_cm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("setup_sharing")

SHARE_NAME = "cp_migration_share"


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# 1. Create or get the delta share on source


def get_or_create_share(auth_mgr: AuthManager, share_name: str, *, dry_run: bool = False) -> str:
    """Create or retrieve a delta share on the source workspace. Returns share name."""
    source = auth_mgr.source_client
    try:
        share = source.shares.get(share_name)
        logger.info("Share '%s' already exists.", share.name)
        return share.name  # type: ignore[return-value]
    except Exception:  # noqa: BLE001
        logger.info("Share '%s' not found, creating...", share_name)

    if dry_run:
        logger.info("[DRY RUN] Would create share '%s'.", share_name)
        return share_name

    share = source.shares.create(name=share_name)
    logger.info("Created share '%s'.", share.name)
    return share.name  # type: ignore[return-value]


# COMMAND ----------
# 3. Create or get recipient for target


def get_or_create_recipient(auth_mgr: AuthManager, metastore_id: str, *, dry_run: bool = False) -> str:
    """Create or retrieve a sharing recipient for the target metastore."""
    source = auth_mgr.source_client
    recipient_name = f"cp_migration_recipient_{metastore_id}"
    try:
        recipient = source.recipients.get(recipient_name)
        logger.info("Recipient '%s' already exists.", recipient.name)
        return recipient.name  # type: ignore[return-value]
    except Exception:  # noqa: BLE001
        logger.info("Recipient '%s' not found, creating...", recipient_name)

    if dry_run:
        logger.info("[DRY RUN] Would create recipient '%s'.", recipient_name)
        return recipient_name

    recipient = source.recipients.create(
        name=recipient_name,
        authentication_type=AuthenticationType.DATABRICKS,
        data_recipient_global_metastore_id=metastore_id,
    )
    logger.info("Created recipient '%s'.", recipient.name)
    return recipient.name  # type: ignore[return-value]


# COMMAND ----------
# 5. Add tables to share in batches of 100


def add_tables_to_share(
    auth_mgr: AuthManager,
    share_name: str,
    tables: list[dict],
    *,
    dry_run: bool = False,
) -> None:
    """Add tables to a delta share in batches of 100 (removes stale entries first)."""
    source = auth_mgr.source_client
    batch_size = 100

    # Remove any existing objects first so re-runs start from a clean slate and
    # don't conflict with entries that used the old shared_as format.
    try:
        existing_share = source.shares.get(name=share_name, include_shared_data=True)
        removals = [
            SharedDataObjectUpdate(
                action=SharedDataObjectUpdateAction.REMOVE,
                data_object=SharedDataObject(name=o.name, data_object_type=o.data_object_type),
            )
            for o in (existing_share.objects or [])
        ]
        if removals and not dry_run:
            source.shares.update(name=share_name, updates=removals)
            logger.info("Removed %d stale object(s) from share '%s'.", len(removals), share_name)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not pre-clean share: %s", exc, exc_info=True)
    existing_names: set[str] = set()

    for i in range(0, len(tables), batch_size):
        batch = tables[i : i + batch_size]
        updates = []
        for tbl in batch:
            obj_name = tbl["object_name"]
            # object_name is expected to be FQN like `catalog`.`schema`.`table`
            parts = obj_name.strip("`").split("`.`")
            if len(parts) != 3:
                logger.warning("Skipping malformed FQN: %s", obj_name)
                continue
            clean_name = ".".join(parts)  # catalog.schema.table without backticks
            if clean_name in existing_names:
                continue
            # NOTE: don't set shared_as — let Databricks expose the original catalog.schema.table
            # structure in the consumer catalog. With shared_as as "schema.table", the UC
            # share-consumer catalog returned an internal schema UUID error on DEEP CLONE.
            updates.append(
                SharedDataObjectUpdate(
                    action=SharedDataObjectUpdateAction.ADD,
                    data_object=SharedDataObject(
                        name=clean_name,
                        data_object_type=_TABLE_TYPE,
                    ),
                )
            )

        if not updates:
            continue

        if dry_run:
            logger.info(
                "[DRY RUN] Would add %d tables to share '%s' (batch %d).",
                len(updates),
                share_name,
                i // batch_size + 1,
            )
            continue

        source.shares.update(name=share_name, updates=updates)
        logger.info(
            "Added %d tables to share '%s' (batch %d).",
            len(updates),
            share_name,
            i // batch_size + 1,
        )


# COMMAND ----------
# 7. Create target catalogs/schemas if missing


def ensure_target_catalogs_and_schemas(
    auth_mgr: AuthManager,
    tables: list[dict],
    *,
    dry_run: bool = False,
) -> None:
    """Ensure all required catalogs and schemas exist on the target workspace."""
    target = auth_mgr.target_client
    seen_catalogs: set[str] = set()
    seen_schemas: set[str] = set()

    for tbl in tables:
        catalog_name = tbl.get("catalog_name", "")
        schema_name = tbl.get("schema_name", "")
        if not catalog_name or not schema_name:
            continue

        if catalog_name not in seen_catalogs:
            seen_catalogs.add(catalog_name)
            if dry_run:
                logger.info("[DRY RUN] Would create catalog '%s' on target.", catalog_name)
            else:
                try:
                    target.catalogs.get(catalog_name)
                    logger.info("Target catalog '%s' already exists.", catalog_name)
                except Exception:  # noqa: BLE001
                    target.catalogs.create(name=catalog_name)
                    logger.info("Created target catalog '%s'.", catalog_name)

        schema_fqn = f"{catalog_name}.{schema_name}"
        if schema_fqn not in seen_schemas:
            seen_schemas.add(schema_fqn)
            if dry_run:
                logger.info("[DRY RUN] Would create schema '%s' on target.", schema_fqn)
            else:
                try:
                    target.schemas.get(f"{catalog_name}.{schema_name}")
                    logger.info("Target schema '%s' already exists.", schema_fqn)
                except Exception:  # noqa: BLE001
                    target.schemas.create(
                        name=schema_name,
                        catalog_name=catalog_name,
                    )
                    logger.info("Created target schema '%s'.", schema_fqn)


# COMMAND ----------
# Notebook execution


def _add_rls_cm_from_tables_api(auth_mgr: AuthManager, pending_tables: list[dict], rls_cm_fqns: set[str]) -> None:
    """Populate ``rls_cm_fqns`` with any pending managed table that carries
    row filter / column mask according to the UC Tables API.

    Backup path for ``tracker.get_tables_with_rls_cm()`` — that helper reads
    discovery_inventory, which can miss tables if discovery's
    ``list_row_filters`` / ``list_column_masks`` silently suppressed an
    exception or the information_schema columns don't surface the
    filter/mask on a given runtime.

    ``source_client.tables.get(full_name)`` returns a ``TableInfo`` whose
    ``row_filter`` field is populated iff ``ALTER TABLE ... SET ROW FILTER``
    has been applied, and whose per-column ``mask`` field is populated iff
    ``ALTER COLUMN ... SET MASK`` is applied. Authoritative and bypasses any
    caching.

    Best-effort: any exception for one table logs a warning but doesn't
    abort the other tables' checks — the migrate will still fail loud at
    shares.update if a table slips through.
    """
    source = auth_mgr.source_client
    for t in pending_tables:
        fqn = t["object_name"]
        full_name = fqn.strip("`").replace("`.`", ".")
        try:
            info = source.tables.get(full_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("tables.get(%s) failed: %s", full_name, exc, exc_info=True)
            continue
        if getattr(info, "row_filter", None) is not None:
            rls_cm_fqns.add(fqn)
            continue
        columns = getattr(info, "columns", None) or []
        for col in columns:
            if getattr(col, "mask", None) is not None:
                rls_cm_fqns.add(fqn)
                break


def _recover_unrestored_rls_cm(auth: AuthManager, tracker: TrackingManager, spark) -> None:
    """Crash-recovery: re-apply RLS/CM policies for any manifest row that's
    still ``restored_at IS NULL`` from a previous (crashed) run.

    Runs BEFORE any new strip happens so a crashed run's tables heal on
    the very next invocation. Continue-on-failure: a table whose restore
    fails gets ``restore_failed_at`` set, and the loop continues. The
    operator can then diagnose and re-run.
    """
    pending = tracker.get_unrestored_rls_cm_manifest()
    if not pending:
        return
    logger.warning(
        "Crash recovery: found %d RLS/CM manifest row(s) still unrestored "
        "from a prior run. Re-applying policies on source before proceeding.",
        len(pending),
    )
    for row in pending:
        table_fqn = row["table_fqn"]
        captured = {
            "filter_fn_fqn": row.get("filter_fn_fqn"),
            "filter_columns": row.get("filter_columns") or [],
            "masks": row.get("masks") or [],
        }
        try:
            # Remove from share first — UC refuses SET ROW FILTER / SET
            # MASK while the table is in an active Delta Share. "Not in
            # share" swallowed so a second recovery attempt still works.
            remove_sql = f"ALTER SHARE {SHARE_NAME} REMOVE TABLE {table_fqn}"
            try:
                spark.sql(remove_sql)
            except Exception as share_exc:  # noqa: BLE001
                msg = str(share_exc).lower()
                if not ("not" in msg and ("shared" in msg or "in share" in msg or "exist" in msg)):
                    raise
            restore_rls_cm(spark, table_fqn, captured)
            tracker.mark_rls_cm_restored(table_fqn)
            logger.warning("Recovered RLS/CM on %s.", table_fqn)
        except Exception as exc:  # noqa: BLE001
            tracker.mark_rls_cm_restore_failed(table_fqn, str(exc))
            logger.error(
                "Recovery failed for %s: %s. Manual intervention required — "
                "see rls_cm_manifest.restore_error.",
                table_fqn,
                exc,
                exc_info=True,
            )


def _validate_rls_cm_strategy(config: MigrationConfig) -> str:
    """Validate ``config.rls_cm_strategy`` and return the normalized value.

    Runs BEFORE any side-effecting setup (share creation, API calls) so
    misconfiguration fails loud without leaving orphan state on source.

    Supported: ``""`` (skip affected tables), ``"drop_and_restore"``, or
    ``"staging_copy"``. The ``drop_and_restore`` path requires
    ``rls_cm_maintenance_window_confirmed = true`` — a deliberate
    informed-consent gate because during each table's DEEP CLONE the
    source is briefly unprotected. ``staging_copy`` does NOT need the
    consent gate: source RLS/CM is never touched (CTAS into a staging
    schema while the migration SPN bypasses the filter as a workspace
    admin).
    """
    strategy = (config.rls_cm_strategy or "").strip().lower()
    if strategy not in ("", "drop_and_restore", "staging_copy"):
        msg = (
            f"Unknown rls_cm_strategy {config.rls_cm_strategy!r}. "
            f"Supported values: '' (skip), 'drop_and_restore', or 'staging_copy'."
        )
        raise ValueError(msg)
    if strategy == "drop_and_restore" and not config.rls_cm_maintenance_window_confirmed:
        msg = (
            "rls_cm_strategy='drop_and_restore' requires "
            "rls_cm_maintenance_window_confirmed=true. This path temporarily "
            "DROPs row filter / column mask on the source table during "
            "each table's DEEP CLONE, briefly exposing unfiltered, "
            "unmasked data to any concurrent reader on source. Only "
            "appropriate for maintenance-window migrations. Set the flag "
            "to true in config.yaml to confirm you've scheduled a "
            "maintenance window, then re-run."
        )
        raise ValueError(msg)
    return strategy


def run(dbutils, spark) -> None:  # noqa: ARG001
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    if not config.include_uc:
        logger.info("Skipping setup_sharing: scope.include_uc=false.")
        return
    # Validate config-gated flags BEFORE any side effects so operator errors
    # (bad rls_cm_strategy value) don't leave orphan shares / recipients.
    strategy = _validate_rls_cm_strategy(config)
    auth = AuthManager(config, dbutils)
    spark_session = spark
    tracker = TrackingManager(spark_session, config)

    # 0. Crash recovery: if a previous drop_and_restore run left any
    #    tables stripped without re-applying, heal them BEFORE we strip
    #    anything new. Always safe to run — if the manifest is empty
    #    (normal case) this is a no-op.
    if strategy == "drop_and_restore":
        _recover_unrestored_rls_cm(auth, tracker, spark_session)

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
    stripped_rls_cm: list[dict] = []
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
        if strategy != "drop_and_restore":
            skipped_rls_cm.append(t)
            continue
        # drop_and_restore: capture + record manifest + strip on source,
        # THEN add the table to the share like any normal table. The
        # post-migrate restore_rls_cm task re-applies the policy after
        # managed_table_worker finishes the DEEP CLONE.
        try:
            captured = capture_rls_cm(auth, t["object_name"])
            if not has_rls_cm(captured):
                # Live probe flagged it but the policy is already gone
                # (discovery caught a race). Safe to share as-is.
                tables_to_share.append(t)
                continue
            if not config.dry_run:
                tracker.record_rls_cm_strip(
                    table_fqn=t["object_name"],
                    filter_fn_fqn=captured["filter_fn_fqn"],
                    filter_columns=captured["filter_columns"],
                    masks=captured["masks"],
                    run_id=run_id,
                )
                strip_rls_cm(spark_session, t["object_name"], captured)
            stripped_rls_cm.append(t)
            tables_to_share.append(t)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to strip RLS/CM from %s; table will NOT be shared. "
                "Source state unchanged. Error: %s",
                t["object_name"],
                exc,
                exc_info=True,
            )
            skipped_rls_cm.append(t)

    if stripped_rls_cm:
        logger.warning(
            "drop_and_restore: stripped row filter / column mask from %d "
            "table(s). Source is unprotected until the post-migrate "
            "restore_rls_cm task re-applies the policies.",
            len(stripped_rls_cm),
        )
        for t in stripped_rls_cm:
            logger.warning("  stripped: %s", t["object_name"])

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
                        "See README.md for options (migrate to ABAC, or use "
                        "rls_cm_strategy='drop_and_restore' in a maintenance window)."
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


def ensure_share_consumer_catalog(auth_mgr: AuthManager, share_name: str, dry_run: bool) -> None:
    """On target workspace, create a catalog that reads from the source share.

    The catalog name is ``<share_name>_consumer``. It's required for workers to
    DEEP CLONE shared tables on the target side.
    """
    consumer_catalog = f"{share_name}_consumer"
    target = auth_mgr.target_client
    source_metastore = auth_mgr.source_client.metastores.summary()
    source_metastore_id = source_metastore.global_metastore_id

    # Find the provider on target that matches the source metastore
    providers = list(target.providers.list())
    matching = [p for p in providers if getattr(p, "data_provider_global_metastore_id", None) == source_metastore_id]
    if not matching:
        names = [p.name for p in providers]
        raise RuntimeError(
            f"No target-side provider found for source metastore {source_metastore_id}. Available providers: {names}"
        )
    provider_name = matching[0].name
    logger.info("Matched target provider '%s' for source metastore.", provider_name)

    if dry_run:
        logger.info("[DRY RUN] Would CREATE CATALOG %s USING SHARE %s.%s", consumer_catalog, provider_name, share_name)
        return

    # Recreate on every run to pick up any shared_as changes.
    try:
        target.catalogs.delete(consumer_catalog, force=True)
        logger.info("Dropped existing share consumer catalog '%s'.", consumer_catalog)
    except Exception:  # noqa: BLE001
        pass

    target.catalogs.create(
        name=consumer_catalog,
        provider_name=provider_name,
        share_name=share_name,
    )
    logger.info("Created share consumer catalog '%s' from %s.%s", consumer_catalog, provider_name, share_name)


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
