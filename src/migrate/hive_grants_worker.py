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
# Hive Grants Worker: replays legacy hive_metastore ACLs as UC grants on the
# target catalog. Runs AFTER hive table/view/function workers so that all
# target securables exist.

import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager
from migrate.hive_common import HIVE_TO_UC_PRIVILEGES, rewrite_hive_fqn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hive_grants_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Helpers


_OBJECT_TYPE_TO_SECURABLE = {
    # Hive discovery classifies managed/external tables by storage location
    # (dbfs-root vs non-dbfs vs external); for grants they're all TABLE.
    "hive_table": "TABLE",
    "hive_external": "TABLE",
    "hive_managed_dbfs_root": "TABLE",
    "hive_managed_nondbfs": "TABLE",
    "hive_view": "VIEW",
    "hive_function": "FUNCTION",
}


def _skip_principal(principal: str) -> bool:
    """Skip system / internal principals (e.g. ``__`` prefix)."""
    return principal is None or principal.startswith("__")


def _emit_grant(
    *,
    action_type: str,
    securable_keyword: str,
    target_fqn: str,
    principal: str,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
) -> dict:
    """Build and emit a GRANT statement. Returns the migration_status record."""
    # Map Hive action to UC privilege.
    uc_priv = HIVE_TO_UC_PRIVILEGES.get(action_type.upper())
    obj_key = f"GRANT_{action_type}_{securable_keyword}_{target_fqn}_{principal}"

    if action_type.upper() == "OWN":
        logger.info(
            "Skipping OWN grant for %s on %s %s (ownership transfer not supported).",
            principal,
            securable_keyword,
            target_fqn,
        )
        return {
            "object_name": obj_key,
            "object_type": "hive_grant",
            "status": "skipped",
            "error_message": "ownership transfer not supported",
            "duration_seconds": 0.0,
        }

    if uc_priv is None:
        logger.warning(
            "Unmapped Hive privilege %s for %s on %s %s; skipping.",
            action_type,
            principal,
            securable_keyword,
            target_fqn,
        )
        return {
            "object_name": obj_key,
            "object_type": "hive_grant",
            "status": "skipped",
            "error_message": "unmapped privilege",
            "duration_seconds": 0.0,
        }

    sql = f"GRANT {uc_priv} ON {securable_keyword} {target_fqn} TO `{principal}`"

    if dry_run:
        logger.info("[DRY RUN] Would grant: %s", sql)
        return {
            "object_name": obj_key,
            "object_type": "hive_grant",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": 0.0,
        }

    start = time.time()
    logger.info("Executing: %s", sql)
    result = execute_and_poll(auth, wh_id, sql)
    duration = time.time() - start

    if result["state"] == "SUCCEEDED":
        return {
            "object_name": obj_key,
            "object_type": "hive_grant",
            "status": "validated",
            "error_message": None,
            "duration_seconds": duration,
        }
    return {
        "object_name": obj_key,
        "object_type": "hive_grant",
        "status": "failed",
        "error_message": result.get("error", result["state"]),
        "duration_seconds": duration,
    }


def _process_show_grants_rows(
    rows,
    *,
    securable_keyword: str,
    target_fqn: str,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
) -> list[dict]:
    """Iterate SHOW GRANTS output rows and emit UC grants."""
    out: list[dict] = []
    for row in rows:
        principal = row["Principal"] if "Principal" in row.asDict() else row[0]
        action_type = row["ActionType"] if "ActionType" in row.asDict() else row[1]
        if _skip_principal(principal):
            logger.info("Skipping system principal %s.", principal)
            continue
        out.append(
            _emit_grant(
                action_type=action_type,
                securable_keyword=securable_keyword,
                target_fqn=target_fqn,
                principal=principal,
                auth=auth,
                wh_id=wh_id,
                dry_run=dry_run,
            )
        )
    return out


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    wh_id = find_warehouse(auth)

    target_catalog = config.hive_target_catalog
    dry_run = config.dry_run

    results: list[dict] = []

    # 1. Catalog-level grants: hive_metastore -> target_catalog
    logger.info("Reading catalog-level grants on hive_metastore.")
    try:
        rows = spark.sql("SHOW GRANTS ON CATALOG hive_metastore").collect()
        target_fqn = f"`{target_catalog}`"
        results.extend(
            _process_show_grants_rows(
                rows,
                securable_keyword="CATALOG",
                target_fqn=target_fqn,
                auth=auth,
                wh_id=wh_id,
                dry_run=dry_run,
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read catalog grants on hive_metastore: %s", exc)

    # 2. Per-object grants - iterate hive discovery inventory
    inv = spark.sql(
        f"SELECT object_name, object_type, catalog_name, schema_name "
        f"FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE source_type = 'hive'"
    ).collect()

    logger.info("Processing grants for %d hive inventory records.", len(inv))

    schemas_seen: set[str] = set()
    for rec in inv:
        schema_name = rec.schema_name
        object_name = rec.object_name
        object_type = rec.object_type

        # Schema-level grants: once per schema
        if schema_name and schema_name not in schemas_seen:
            schemas_seen.add(schema_name)
            try:
                rows = spark.sql(f"SHOW GRANTS ON SCHEMA hive_metastore.`{schema_name}`").collect()
                target_schema_fqn = f"`{target_catalog}`.`{schema_name}`"
                results.extend(
                    _process_show_grants_rows(
                        rows,
                        securable_keyword="SCHEMA",
                        target_fqn=target_schema_fqn,
                        auth=auth,
                        wh_id=wh_id,
                        dry_run=dry_run,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to read schema grants on hive_metastore.%s: %s",
                    schema_name,
                    exc,
                )

        # Object-level grants
        securable = _OBJECT_TYPE_TO_SECURABLE.get(object_type)
        if not securable:
            logger.info("Skipping unknown object_type %s for %s.", object_type, object_name)
            continue

        try:
            rows = spark.sql(f"SHOW GRANTS ON {securable} {object_name}").collect()
            target_obj_fqn = rewrite_hive_fqn(object_name, target_catalog)
            results.extend(
                _process_show_grants_rows(
                    rows,
                    securable_keyword=securable,
                    target_fqn=target_obj_fqn,
                    auth=auth,
                    wh_id=wh_id,
                    dry_run=dry_run,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to read grants on %s %s: %s", securable, object_name, exc)

    # 3. Record final statuses
    if results:
        tracker.append_migration_status(results)

    logger.info(
        "Hive grants worker complete. %d validated, %d skipped, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "skipped"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
