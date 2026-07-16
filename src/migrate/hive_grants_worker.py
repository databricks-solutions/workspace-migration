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
# Hive Grants Worker: replays hive_metastore ACLs + ownership onto the target
# workspace's own hive_metastore, like-for-like (target FQN == source FQN).
# Runs AFTER hive table/view/function workers so all target securables exist.
# Ownership transfer uses grant-before-transfer (GRANT USAGE, CREATE to the SPN
# before ALTER SCHEMA OWNER) so the SPN keeps write access on re-runs, and
# skips the transfer when the target already owns the securable (finding #13).

import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager
from migrate.hive_common import HIVE_TO_UC_PRIVILEGES
from migrate.reconciliation import resolve_current_job_run_id

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


def _should_skip_owner_transfer(current_owner: str | None, target_principal: str) -> bool:
    """Skip the ALTER … OWNER TO when the target already owns the securable —
    makes re-runs idempotent (finding #13: no re-transfer failure)."""
    if not current_owner:
        return False
    return current_owner.strip().lower() == target_principal.strip().lower()


_OWNER_DESCRIBE = {
    "CATALOG": "DESCRIBE CATALOG EXTENDED {fqn}",
    "SCHEMA": "DESCRIBE SCHEMA EXTENDED {fqn}",
    "TABLE": "DESCRIBE TABLE EXTENDED {fqn}",
    "VIEW": "DESCRIBE TABLE EXTENDED {fqn}",
    "FUNCTION": "DESCRIBE FUNCTION EXTENDED {fqn}",
}


def _current_owner(auth: AuthManager, wh_id: str, securable_keyword: str, target_fqn: str) -> str | None:
    """Read the current owner of a securable via the target warehouse.

    Parses the ``Owner`` row from ``DESCRIBE … EXTENDED``. Best-effort: any
    failure returns None (caller then proceeds with the transfer).
    """
    tmpl = _OWNER_DESCRIBE.get(securable_keyword)
    if not tmpl:
        return None
    res = execute_and_poll(auth, wh_id, tmpl.format(fqn=target_fqn))
    if res.get("state") != "SUCCEEDED":
        return None
    for row in res.get("rows", []) or []:
        cells = [str(c) if c is not None else "" for c in row]
        for i, c in enumerate(cells):
            if c.strip().lower() in ("owner", "table owner") and i + 1 < len(cells):
                return cells[i + 1].strip() or None
    return None


def _emit_grant(
    *,
    action_type: str,
    securable_keyword: str,
    target_fqn: str,
    principal: str,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
    transfer_ownership: bool = True,
    spn_client_id: str = "",
) -> dict:
    """Build and emit a GRANT statement. Returns the migration_status record."""
    # Map Hive action to UC privilege.
    uc_priv = HIVE_TO_UC_PRIVILEGES.get(action_type.upper())
    obj_key = f"GRANT_{action_type}_{securable_keyword}_{target_fqn}_{principal}"

    if action_type.upper() == "OWN":
        if not transfer_ownership:
            logger.info(
                "transfer_ownership=False: leaving %s %s owned by the migration SPN.",
                securable_keyword, target_fqn,
            )
            return {
                "object_name": obj_key, "object_type": "hive_grant",
                "status": "skipped", "error_message": "transfer_ownership disabled",
                "duration_seconds": 0.0,
            }
        owner_obj_key = f"OWNER_{securable_keyword}_{target_fqn}_{principal}"
        # #14 moot: never transfer ownership of the built-in hive_metastore catalog.
        if securable_keyword == "CATALOG":
            return {
                "object_name": owner_obj_key, "object_type": "hive_grant",
                "status": "skipped",
                "error_message": "hive_metastore catalog ownership not transferred (built-in)",
                "duration_seconds": 0.0,
            }
        owner_sql = f"ALTER {securable_keyword} {target_fqn} OWNER TO `{principal}`"
        grant_sql = None
        if securable_keyword == "SCHEMA" and spn_client_id:
            grant_sql = f"GRANT USAGE, CREATE ON SCHEMA {target_fqn} TO `{spn_client_id}`"
        if dry_run:
            if grant_sql:
                logger.info("[DRY RUN] Would grant-before-transfer: %s", grant_sql)
            logger.info("[DRY RUN] Would transfer ownership: %s", owner_sql)
            return {
                "object_name": owner_obj_key, "object_type": "hive_grant",
                "status": "skipped", "error_message": "dry_run", "duration_seconds": 0.0,
            }
        start = time.time()
        # skip-if-already-owned (finding #13): idempotent re-runs.
        current = _current_owner(auth, wh_id, securable_keyword, target_fqn)
        if _should_skip_owner_transfer(current, principal):
            return {
                "object_name": owner_obj_key, "object_type": "hive_grant",
                "status": "skipped",
                "error_message": f"already owned by target principal {principal!r}",
                "duration_seconds": time.time() - start,
            }
        # grant-before-transfer: keep SPN CREATE on the schema after handing
        # ownership back to the original owner (finding #13 lockout).
        if grant_sql:
            logger.info("Grant-before-transfer: %s", grant_sql)
            g_res = execute_and_poll(auth, wh_id, grant_sql)
            if g_res["state"] != "SUCCEEDED":
                return {
                    "object_name": owner_obj_key, "object_type": "hive_grant",
                    "status": "failed",
                    "error_message": f"grant-before-transfer failed: {g_res.get('error', g_res['state'])}",
                    "duration_seconds": time.time() - start,
                }
        logger.info("Transferring ownership: %s", owner_sql)
        result = execute_and_poll(auth, wh_id, owner_sql)
        duration = time.time() - start
        if result["state"] == "SUCCEEDED":
            return {
                "object_name": owner_obj_key, "object_type": "hive_grant",
                "status": "validated", "error_message": None, "duration_seconds": duration,
            }
        return {
            "object_name": owner_obj_key, "object_type": "hive_grant",
            "status": "failed",
            "error_message": (
                f"Ownership transfer to '{principal}' failed "
                f"(does the principal exist on target?): {result.get('error', result['state'])}"
            ),
            "duration_seconds": duration,
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
    transfer_ownership: bool = True,
    spn_client_id: str = "",
) -> list[dict]:
    """Iterate SHOW GRANTS output rows and emit UC grants.

    Non-OWN grants are emitted first, then OWN rows (ownership transfer) last,
    so the migration SPN keeps MANAGE while granting (review finding #5).
    """
    out: list[dict] = []
    deferred_own: list[tuple[str, str]] = []
    for row in rows:
        principal = row["Principal"] if "Principal" in row.asDict() else row[0]
        action_type = row["ActionType"] if "ActionType" in row.asDict() else row[1]
        if _skip_principal(principal):
            logger.info("Skipping system principal %s.", principal)
            continue
        if action_type.upper() == "OWN":
            deferred_own.append((action_type, principal))
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
                transfer_ownership=transfer_ownership,
                spn_client_id=spn_client_id,
            )
        )
    for action_type, principal in deferred_own:
        out.append(
            _emit_grant(
                action_type=action_type,
                securable_keyword=securable_keyword,
                target_fqn=target_fqn,
                principal=principal,
                auth=auth,
                wh_id=wh_id,
                dry_run=dry_run,
                transfer_ownership=transfer_ownership,
                spn_client_id=spn_client_id,
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
    tracker.job_run_id = resolve_current_job_run_id(dbutils)
    wh_id = find_warehouse(auth)

    dry_run = config.dry_run
    transfer_ownership = config.transfer_ownership
    spn_client_id = config.spn_client_id

    results: list[dict] = []

    # 1. Catalog-level grants: replayed on the target's built-in hive_metastore
    # (like-for-like — target FQN == source FQN). The OWN row is skipped inside
    # _emit_grant for CATALOG (built-in hive_metastore ownership, #14 moot).
    logger.info("Reading catalog-level grants on hive_metastore.")
    try:
        rows = spark.sql("SHOW GRANTS ON CATALOG hive_metastore").collect()
        target_fqn = "`hive_metastore`"
        results.extend(
            _process_show_grants_rows(
                rows,
                securable_keyword="CATALOG",
                target_fqn=target_fqn,
                auth=auth,
                wh_id=wh_id,
                dry_run=dry_run,
                transfer_ownership=transfer_ownership,
                spn_client_id=spn_client_id,
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
                target_schema_fqn = f"`hive_metastore`.`{schema_name}`"
                results.extend(
                    _process_show_grants_rows(
                        rows,
                        securable_keyword="SCHEMA",
                        target_fqn=target_schema_fqn,
                        auth=auth,
                        wh_id=wh_id,
                        dry_run=dry_run,
                        transfer_ownership=transfer_ownership,
                        spn_client_id=spn_client_id,
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
            target_obj_fqn = object_name  # like-for-like: target FQN == source FQN
            results.extend(
                _process_show_grants_rows(
                    rows,
                    securable_keyword=securable,
                    target_fqn=target_obj_fqn,
                    auth=auth,
                    wh_id=wh_id,
                    dry_run=dry_run,
                    transfer_ownership=transfer_ownership,
                    spn_client_id=spn_client_id,
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
