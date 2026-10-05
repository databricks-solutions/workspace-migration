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
# Grants Worker: replays catalog and schema grants from source to target, and
# transfers ownership (OWN -> ALTER ... OWNER TO original owner) after the
# other grants. Gated on config.transfer_ownership.

import logging
import time

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("grants_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Dependency-skip helpers (finding #19 — UC counterpart of Hive finding #9)


def _grant_target_not_migrated(object_name: str, not_migrated_names: set[str]) -> bool:
    """True when a per-object grant's target was not migrated (finding #19)."""
    return object_name in not_migrated_names


def _skipped_dependency_grant_row(object_name: str) -> dict:
    """migration_status row for a grant skipped because its target object was
    not migrated (finding #19) — recorded skipped, not failed."""
    return {
        "object_name": f"GRANT_SKIPPED_{object_name}",
        "object_type": "grant",
        "status": "skipped_dependency_not_migrated",
        "error_message": f"target object {object_name} was not migrated; grants skipped",
        "duration_seconds": 0.0,
    }


# COMMAND ----------
# Inherited-grant filter (finding #33)


def _normalize_securable_key(key: str) -> str:
    return (key or "").replace("`", "").strip().lower()


def _direct_grants(grants: list[dict], securable_fqn: str) -> list[dict]:
    """Keep only grants set directly on *securable_fqn*.

    ``SHOW GRANTS ON <child>`` also returns grants inherited from the parent
    schema/catalog, keyed (``ObjectKey``) by that parent. Replaying those on
    the child would turn inherited access into explicit child grants on target,
    which a later parent-level REVOKE no longer removes. Matched on the key, not
    ``ObjectType`` — a direct VIEW grant reports ``ObjectType=TABLE``."""
    target = _normalize_securable_key(securable_fqn)
    return [g for g in grants if _normalize_securable_key(g.get("securable_fqn", "")) == target]


# COMMAND ----------
# Grant scope (finding #31). Foreign catalogs are created by migrate_governance,
# which runs AFTER migrate_uc — so their grants can't be replayed in migrate_uc.
# migrate_uc runs this worker with scope "uc" (foreign catalogs excluded);
# migrate_governance runs it again with scope "foreign_catalogs" right after
# migrate_foreign_catalogs.

GRANT_SCOPE_UC = "uc"
GRANT_SCOPE_FOREIGN_CATALOGS = "foreign_catalogs"


def _resolve_grant_scope(dbutils) -> str:
    """``grant_scope`` notebook parameter; defaults to ``uc`` when unset."""
    try:
        scope = dbutils.widgets.get("grant_scope")
    except Exception:  # noqa: BLE001 — widget not defined → default scope
        scope = ""
    scope = (scope.strip() if isinstance(scope, str) else "") or GRANT_SCOPE_UC
    if scope not in (GRANT_SCOPE_UC, GRANT_SCOPE_FOREIGN_CATALOGS):
        raise ValueError(f"Unknown grant_scope {scope!r}")
    return scope


def _target_catalog_exists(auth: AuthManager, name: str) -> bool:
    """True when catalog *name* exists on target. Checked directly (not via
    migration_status) so a foreign catalog's status key prefix doesn't matter."""
    try:
        auth.target_client.catalogs.get(name)
        return True
    except Exception:  # noqa: BLE001 — NotFound / not visible → treat as absent
        return False


# COMMAND ----------
# Replay grants


def replay_grants(
    securable_type: str,
    securable_fqn: str,
    grants: list[dict],
    *,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool = False,
    transfer_ownership: bool = True,
) -> list[dict]:
    """Replay a list of grants on the target.

    Non-OWN grants are replayed as ``GRANT``. OWN grants transfer ownership to
    the original owner via ``ALTER <securable> OWNER TO`` (review finding #5) —
    applied AFTER the non-OWN grants so the migration SPN keeps MANAGE while it
    is still granting. Set ``transfer_ownership=False`` to leave ownership with
    the SPN (the old skip behaviour).
    """
    results: list[dict] = []
    own_grants: list[dict] = []
    for grant in grants:
        principal = grant["principal"]
        action_type = grant["action_type"]

        # Defer OWN to an ownership transfer applied after the other grants.
        if action_type.upper() == "OWN":
            own_grants.append(grant)
            continue

        sql = f"GRANT {action_type} ON {securable_type} {securable_fqn} TO `{principal}`"
        obj_key = f"GRANT_{action_type}_{securable_type}_{securable_fqn}_{principal}"

        if dry_run:
            logger.info("[DRY RUN] Would execute: %s", sql)
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "skipped",
                    "error_message": "dry_run",
                    "duration_seconds": 0.0,
                }
            )
            continue

        start = time.time()
        logger.info("Executing: %s", sql)
        result = execute_and_poll(auth, wh_id, sql)
        duration = time.time() - start

        if result["state"] == "SUCCEEDED":
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "validated",
                    "error_message": None,
                    "duration_seconds": duration,
                }
            )
        else:
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "failed",
                    "error_message": result.get("error", result["state"]),
                    "duration_seconds": duration,
                }
            )

    # Ownership transfer last (review finding #5).
    if not transfer_ownership:
        for grant in own_grants:
            logger.info(
                "transfer_ownership=False: leaving %s %s owned by the migration SPN "
                "(original owner %s not restored).",
                securable_type, securable_fqn, grant["principal"],
            )
        return results

    for grant in own_grants:
        principal = grant["principal"]
        sql = f"ALTER {securable_type} {securable_fqn} OWNER TO `{principal}`"
        obj_key = f"OWNER_{securable_type}_{securable_fqn}_{principal}"

        if dry_run:
            logger.info("[DRY RUN] Would execute: %s", sql)
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "skipped",
                    "error_message": "dry_run",
                    "duration_seconds": 0.0,
                }
            )
            continue

        start = time.time()
        logger.info("Transferring ownership: %s", sql)
        result = execute_and_poll(auth, wh_id, sql)
        duration = time.time() - start

        if result["state"] == "SUCCEEDED":
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "validated",
                    "error_message": None,
                    "duration_seconds": duration,
                }
            )
        else:
            # Fail loud per-row (e.g. original owner missing on target) — do
            # not crash the batch, do not silently skip.
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "failed",
                    "error_message": (
                        f"Ownership transfer to '{principal}' failed "
                        f"(does the principal exist on target?): "
                        f"{result.get('error', result['state'])}"
                    ),
                    "duration_seconds": duration,
                }
            )

    return results


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    spark_session = spark
    tracker = TrackingManager(spark_session, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)
    explorer = CatalogExplorer(spark_session, auth)

    # Read discovery inventory for catalogs, schemas, and granular objects.
    #
    # Object-type → UC securable-type mapping for SHOW GRANTS ON <type>:
    #   managed_table, external_table, mv, st  -> TABLE
    #   view                                    -> VIEW
    #   volume                                  -> VOLUME
    #   function                                -> FUNCTION
    # (Function grants at securable level are a GA feature; keep them in
    # scope so migrations don't silently lose EXECUTE permissions on UDFs.)
    inventory_df = spark_session.sql(
        f"SELECT DISTINCT catalog_name, schema_name, object_name, object_type "
        f"FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE source_type = 'uc'"
    )
    inventory_rows = inventory_df.collect()

    grant_scope = _resolve_grant_scope(dbutils)

    catalogs: set[str] = set()
    foreign_catalogs: set[str] = set()
    schemas: set[str] = set()  # set of "catalog.schema" FQNs
    tables: set[str] = set()  # managed_table, external_table, mv, st
    views: set[str] = set()
    volumes: set[str] = set()
    functions: set[str] = set()

    for row in inventory_rows:
        cat = row.catalog_name
        sch = row.schema_name
        obj_name = row.object_name
        obj_type = row.object_type
        if obj_type == "foreign_catalog":
            foreign_catalogs.add(cat or obj_name)
            continue
        if cat:
            catalogs.add(cat)
        if cat and sch:
            schemas.add(f"`{cat}`.`{sch}`")
        if obj_name:
            if obj_type in ("managed_table", "external_table", "mv", "st"):
                tables.add(obj_name)
            elif obj_type == "view":
                views.add(obj_name)
            elif obj_type == "volume":
                volumes.add(obj_name)
            elif obj_type == "function":
                functions.add(obj_name)

    # Other inventory rows (e.g. a comment on a foreign catalog) can carry a
    # foreign catalog's name — keep foreign catalogs out of the UC scope.
    catalogs -= foreign_catalogs
    schemas = {s for s in schemas if s.split("`.`")[0].strip("`") not in foreign_catalogs}

    logger.info(
        "Grants scope (%s): %d catalog(s), %d foreign catalog(s), %d schema(s), %d table(s), %d view(s), "
        "%d volume(s), %d function(s)",
        grant_scope,
        len(catalogs),
        len(foreign_catalogs),
        len(schemas),
        len(tables),
        len(views),
        len(volumes),
        len(functions),
    )

    wh_id = find_warehouse(auth)

    # Object names whose latest migration_status is not 'validated'. A
    # per-object grant on any of these (a failed/skipped table) is
    # cascade-skipped rather than hard-failing (finding #19). Foreign
    # catalogs are handled by the foreign_catalogs scope instead (#31).
    not_migrated_names = tracker.not_validated_object_names(source_type="uc")

    all_results: list[dict] = []

    def _process(securable_type: str, fqn: str, *, object_level: bool = False) -> None:
        """Enumerate + replay grants for a single securable. Records
        a ``failed`` row on enumeration error so the gap surfaces in
        migration_status rather than being silently swallowed.

        For object-level securables (``object_level=True``) whose target
        was not migrated, records ``skipped_dependency_not_migrated`` and
        skips enumeration (finding #19). Catalog/schema grants are never
        cascade-skipped — they aren't per-migrated-object.

        Only grants set directly on the securable are replayed; inherited ones
        are replayed once, on the parent they belong to (finding #33)."""
        if object_level and _grant_target_not_migrated(fqn, not_migrated_names):
            logger.info("Skipping grants for not-migrated object %s.", fqn)
            all_results.append(_skipped_dependency_grant_row(fqn))
            return
        try:
            grants = _direct_grants(explorer.list_grants(securable_type, fqn), fqn)
            grant_results = replay_grants(
                securable_type,
                fqn,
                grants,
                auth=auth,
                wh_id=wh_id,
                dry_run=config.dry_run,
                transfer_ownership=config.transfer_ownership,
            )
            all_results.extend(grant_results)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to process grants for %s %s: %s",
                securable_type,
                fqn,
                exc,
                exc_info=True,
            )
            all_results.append(
                {
                    "object_name": f"{securable_type}_GRANTS_{fqn}",
                    "object_type": "grant",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            )

    if grant_scope == GRANT_SCOPE_FOREIGN_CATALOGS:
        # Catalog + schema grants on each foreign catalog (#31). Schemas come
        # from the source foreign catalog itself — discovery records only the
        # catalog row, so its schema grants were never replayed before.
        for fc in sorted(foreign_catalogs):
            if not _target_catalog_exists(auth, fc):
                logger.info("Skipping grants for foreign catalog %s — not on target.", fc)
                all_results.append(_skipped_dependency_grant_row(f"`{fc}`"))
                continue
            logger.info("Processing grants for FOREIGN CATALOG %s", fc)
            _process("CATALOG", f"`{fc}`")
            try:
                fc_schemas = explorer.list_schemas(fc)
            except Exception as exc:  # noqa: BLE001 — e.g. connection unreachable
                logger.error("Failed to list schemas of foreign catalog %s: %s", fc, exc, exc_info=True)
                all_results.append(
                    {
                        "object_name": f"SCHEMA_GRANTS_`{fc}`",
                        "object_type": "grant",
                        "status": "failed",
                        "error_message": f"could not list schemas of foreign catalog: {exc}",
                        "duration_seconds": 0.0,
                    }
                )
                fc_schemas = []
            for sch in sorted(fc_schemas):
                _process("SCHEMA", f"`{fc}`.`{sch}`")
    else:
        # Process in CATALOG → SCHEMA → object order. UC evaluates grants
        # at the object level first, falling back to schema then catalog,
        # so the replay order doesn't affect correctness — this ordering
        # just keeps logs easy to scan.
        for catalog_name in sorted(catalogs):
            logger.info("Processing grants for CATALOG %s", catalog_name)
            _process("CATALOG", f"`{catalog_name}`")

        for schema_fqn in sorted(schemas):
            logger.info("Processing grants for SCHEMA %s", schema_fqn)
            _process("SCHEMA", schema_fqn)

        for fqn in sorted(tables):
            _process("TABLE", fqn, object_level=True)

        for fqn in sorted(views):
            _process("VIEW", fqn, object_level=True)

        for fqn in sorted(volumes):
            _process("VOLUME", fqn, object_level=True)

        for fqn in sorted(functions):
            _process("FUNCTION", fqn, object_level=True)

    # Record final statuses

    if all_results:
        tracker.append_migration_status(all_results)

    logger.info(
        "Grants worker complete. %d succeeded, %d failed.",
        sum(1 for r in all_results if r["status"] == "validated"),
        sum(1 for r in all_results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
