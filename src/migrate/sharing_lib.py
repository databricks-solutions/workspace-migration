"""Pure-Python library module for Delta Sharing setup helpers.

Extracted from setup_sharing.py (which is a Databricks notebook source file
and therefore cannot be imported at runtime).  This module contains only the
reusable helper functions and module constants; the notebook-level
orchestration (run(), bootstrap) remains in setup_sharing.py.
"""
from __future__ import annotations

import logging

from databricks.sdk.errors import NotFound
from databricks.sdk.service.sharing import (
    AuthenticationType,
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

logger = logging.getLogger("sharing_lib")

SHARE_NAME = "cp_migration_share"


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


def add_tables_to_share(
    auth_mgr: AuthManager,
    share_name: str,
    tables: list[dict],
    *,
    dry_run: bool = False,
) -> list[str]:
    """Add tables to a delta share in batches of 100 (removes stale entries first).

    Returns the list of source object names that were SKIPPED because they no
    longer exist on source (review finding #14). A stale ``discovery_inventory``
    row (e.g. a table torn down by a prior test) must not abort the whole
    migration: if a batch add fails with NotFound, we retry that batch
    one-by-one and skip+warn the vanished objects.
    """
    source = auth_mgr.source_client
    batch_size = 100

    # Pre-clean only objects that exist in the share but are NOT in the
    # desired list. The previous unconditional drop created a brief
    # empty-share window where consumers saw zero objects between the
    # REMOVE and the subsequent ADD; skipping the pre-clean when desired
    # is a superset of current preserves share availability for re-adds.
    desired_names: set[str] = set()
    for tbl in tables:
        obj_name = tbl["object_name"]
        parts = obj_name.strip("`").split("`.`")
        if len(parts) == 3:
            desired_names.add(".".join(parts))
    try:
        existing_share = source.shares.get(name=share_name, include_shared_data=True)
        existing_objects = existing_share.objects or []
        removals = [
            SharedDataObjectUpdate(
                action=SharedDataObjectUpdateAction.REMOVE,
                data_object=SharedDataObject(name=o.name, data_object_type=o.data_object_type),
            )
            for o in existing_objects
            if o.name not in desired_names
        ]
        if removals and not dry_run:
            source.shares.update(name=share_name, updates=removals)
            logger.info(
                "Removed %d stale object(s) from share '%s' (existed but no longer desired).",
                len(removals),
                share_name,
            )
        elif not removals and existing_objects:
            logger.info(
                "Share '%s' pre-clean skipped: existing objects are a subset of desired.",
                share_name,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not pre-clean share: %s", exc, exc_info=True)
    existing_names: set[str] = set()
    skipped_vanished: list[str] = []

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

        try:
            source.shares.update(name=share_name, updates=updates)
            logger.info(
                "Added %d tables to share '%s' (batch %d).",
                len(updates),
                share_name,
                i // batch_size + 1,
            )
        except NotFound as exc:
            # A member of this batch vanished since discovery (stale inventory).
            # Retry one-by-one so one missing object doesn't abort the migration.
            logger.warning(
                "Batch add to share '%s' hit NotFound (%s); retrying individually "
                "to skip vanished source object(s).",
                share_name,
                exc,
            )
            for u in updates:
                name = u.data_object.name
                try:
                    source.shares.update(name=share_name, updates=[u])
                except NotFound as exc2:
                    logger.warning(
                        "Skipping vanished source object '%s' — not added to share "
                        "(stale discovery_inventory row?): %s",
                        name,
                        exc2,
                    )
                    skipped_vanished.append(name)

    if skipped_vanished:
        logger.warning(
            "Share setup skipped %d vanished source object(s): %s",
            len(skipped_vanished),
            skipped_vanished,
        )
    return skipped_vanished


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
