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
# Delta Sharing Objects Worker (Phase 3 Task 37).
#
# Recreates the customer's own Delta Sharing configuration (shares,
# recipients, providers) on target. Excludes the migration's internal
# cp_migration_share (already handled by discovery's exclude).
#
# Extensions over the original Phase 3 plan (added 2026-04-19):
# shares can contain views, volumes, schemas, and catalogs — not just
# tables. The ADD logic dispatches on data_object_type.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sharing_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


# Map SharedDataObjectDataObjectType enum string -> SQL keyword for ALTER SHARE ADD
_SHARE_OBJECT_SQL = {
    "TABLE": "TABLE",
    "VIEW": "VIEW",
    "VOLUME": "VOLUME",
    "SCHEMA": "SCHEMA",
    "CATALOG": "CATALOG",
    "MODEL": "MODEL",
}


def cleanup_partial_share(
    object_name: str, *, auth, spark=None, config=None
) -> None:
    """Cleanup hook (X.1 reconciliation) for an in-progress share.

    ``apply_share`` iterates ``ALTER SHARE ADD`` per object. If the worker
    dies after adding a subset, the next run's idempotency path (X.2)
    treats the already-added objects as ``already_present`` and succeeds,
    so no destructive cleanup is strictly required. But the share may
    still carry objects that the *next* run has since dropped from the
    source spec (rare, but possible when source was edited between runs).
    For safety we drop the share entirely; ``apply_share`` will recreate
    it on retry from the live spec.

    Best-effort: swallow NOT_FOUND / "does not exist" errors. The
    ``object_name`` here is the ``SHARE_<name>`` key stamped by
    ``apply_share`` strip the prefix to get the real share name.
    """
    name = object_name
    if name.startswith("SHARE_"):
        name = name[len("SHARE_"):]
    try:
        auth.target_client.shares.delete(name=name)
        logger.info("Reconciliation cleanup: dropped partial share %s", name)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).lower()
        if "not" in msg and ("exist" in msg or "found" in msg):
            logger.info(
                "Reconciliation cleanup: share %s already absent; nothing to drop.",
                name,
            )
            return
        raise


def apply_share(
    share: dict,
    *,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
) -> dict:
    share_name = share["share_name"]
    comment = share.get("comment")
    objects = share.get("objects") or []

    obj_key = f"SHARE_{share_name}"
    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] Would create share %s with %d object(s)", share_name, len(objects))
        return {
            "object_name": obj_key,
            "object_type": "share",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }

    # Create share shell (tolerate already-exists)
    try:
        auth.target_client.shares.create(name=share_name, comment=comment)
    except Exception as exc:  # noqa: BLE001
        if "already" not in str(exc).lower() or "exists" not in str(exc).lower():
            return {
                "object_name": obj_key,
                "object_type": "share",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": time.time() - start,
            }

    # Add objects via SQL ALTER SHARE (covers table/view/volume/schema/catalog)
    failures: list[str] = []
    added = 0
    already_present = 0
    for o in objects:
        dot_raw = o.get("data_object_type", "")
        # Enum stringified: "SharedDataObjectDataObjectType.TABLE" → "TABLE"
        dot = dot_raw.split(".")[-1].upper() if dot_raw else "TABLE"
        keyword = _SHARE_OBJECT_SQL.get(dot, "TABLE")
        fqn = o.get("name", "")
        if not fqn:
            continue
        # FQN in SharedDataObject is unquoted dot-form; backtick-quote parts
        parts = fqn.split(".")
        quoted = ".".join(f"`{p}`" for p in parts)
        sql = f"ALTER SHARE `{share_name}` ADD {keyword} {quoted}"
        try:
            res = execute_and_poll(auth, wh_id, sql)
            if res["state"] == "SUCCEEDED":
                added += 1
            else:
                err_text = str(res.get("error", res["state"])).lower()
                # Idempotency: ALTER SHARE ADD errors if the object is already
                # in the share. Treat as no-op on retry rather than a failure.
                if "already" in err_text:
                    already_present += 1
                else:
                    failures.append(f"{keyword} {fqn}: {res.get('error', res['state'])}")
        except Exception as exc:  # noqa: BLE001
            if "already" in str(exc).lower():
                already_present += 1
            else:
                failures.append(f"{keyword} {fqn}: {exc}")

    duration = time.time() - start
    status_msg = f"Added {added} object(s)"
    if already_present:
        status_msg += f"; {already_present} already present"
    if failures:
        status_msg += f"; {len(failures)} failed: " + "; ".join(failures[:3])
    return {
        "object_name": obj_key,
        "object_type": "share",
        "status": "validated" if not failures else "validation_failed",
        "error_message": status_msg if (failures or (added == 0 and not already_present)) else None,
        "duration_seconds": duration,
    }


def apply_recipient(rc: dict, *, auth: AuthManager, dry_run: bool) -> dict:
    from databricks.sdk.service.sharing import AuthenticationType

    name = rc["recipient_name"]
    auth_type_raw = rc.get("authentication_type", "DATABRICKS")
    # Strip any enum prefix ("AuthenticationType.DATABRICKS")
    auth_type = auth_type_raw.split(".")[-1]
    try:
        auth_type_enum = AuthenticationType[auth_type]
    except Exception:  # noqa: BLE001
        auth_type_enum = AuthenticationType.DATABRICKS

    obj_key = f"RECIPIENT_{name}"
    start = time.time()
    if dry_run:
        return {
            "object_name": obj_key,
            "object_type": "recipient",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }
    try:
        kwargs = {"name": name, "authentication_type": auth_type_enum}
        if rc.get("global_metastore_id"):
            kwargs["data_recipient_global_metastore_id"] = rc["global_metastore_id"]
        if rc.get("comment"):
            kwargs["comment"] = rc["comment"]
        auth.target_client.recipients.create(**kwargs)
        note = None
        if str(auth_type_enum).endswith("TOKEN"):
            note = "New activation token generated on target — redistribute to consumer."
        return {
            "object_name": obj_key,
            "object_type": "recipient",
            "status": "validated",
            "error_message": note,
            "duration_seconds": time.time() - start,
        }
    except Exception as exc:  # noqa: BLE001
        if "already" in str(exc).lower() and "exists" in str(exc).lower():
            return {
                "object_name": obj_key,
                "object_type": "recipient",
                "status": "validated",
                "error_message": "already existed",
                "duration_seconds": time.time() - start,
            }
        return {
            "object_name": obj_key,
            "object_type": "recipient",
            "status": "failed",
            "error_message": str(exc),
            "duration_seconds": time.time() - start,
        }


def apply_provider(prov: dict, *, auth: AuthManager, dry_run: bool) -> dict:
    from databricks.sdk.service.sharing import AuthenticationType

    name = prov["provider_name"]
    auth_type_raw = prov.get("authentication_type", "TOKEN")
    auth_type = auth_type_raw.split(".")[-1]
    try:
        auth_type_enum = AuthenticationType[auth_type]
    except Exception:  # noqa: BLE001
        auth_type_enum = AuthenticationType.TOKEN

    obj_key = f"PROVIDER_{name}"
    start = time.time()
    if dry_run:
        return {
            "object_name": obj_key,
            "object_type": "provider",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }
    try:
        auth.target_client.providers.create(
            name=name,
            authentication_type=auth_type_enum,
            comment=prov.get("comment"),
        )
        return {
            "object_name": obj_key,
            "object_type": "provider",
            "status": "validated",
            "error_message": (
                "Provider shell created — recipient_profile_str must be re-uploaded by customer on target."
            ),
            "duration_seconds": time.time() - start,
        }
    except Exception as exc:  # noqa: BLE001
        if "already" in str(exc).lower() and "exists" in str(exc).lower():
            return {
                "object_name": obj_key,
                "object_type": "provider",
                "status": "validated",
                "error_message": "already existed",
                "duration_seconds": time.time() - start,
            }
        return {
            "object_name": obj_key,
            "object_type": "provider",
            "status": "failed",
            "error_message": str(exc),
            "duration_seconds": time.time() - start,
        }


def _parse(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if isinstance(meta, dict):
            out.append(meta)
    return out


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    wh_id = find_warehouse(auth)

    share_rows = _parse(json.loads(dbutils.jobs.taskValues.get(taskKey="orchestrator", key="share_list")))
    recipient_rows = _parse(json.loads(dbutils.jobs.taskValues.get(taskKey="orchestrator", key="recipient_list")))
    provider_rows = _parse(json.loads(dbutils.jobs.taskValues.get(taskKey="orchestrator", key="provider_list")))

    logger.info(
        "Received %d share, %d recipient, %d provider records.",
        len(share_rows),
        len(recipient_rows),
        len(provider_rows),
    )

    results: list[dict] = []
    # Recipients first so shares can grant SELECT to them; providers last.
    for r in recipient_rows:
        results.append(apply_recipient(r, auth=auth, dry_run=config.dry_run))
    for s in share_rows:
        results.append(apply_share(s, auth=auth, wh_id=wh_id, dry_run=config.dry_run))
    for p in provider_rows:
        results.append(apply_provider(p, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Sharing worker complete. %d validated, %d validation_failed, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "validation_failed"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
