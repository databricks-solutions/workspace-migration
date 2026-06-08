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
# Connections Worker (Phase 3 Task 35).
#
# Creates UC connections on target. Secret fields in options (password,
# client_secret, etc.) are NOT returned by source's GET API — the worker
# creates the connection shell and records status='validation_failed' with
# a list of credentials the customer must re-enter via the Databricks UI
# or SQL ALTER CONNECTION.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("connections_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


# Keys in connection.options that typically need to be re-entered by the
# customer after migration — never returned by the GET API.
_SECRET_KEYS = frozenset(
    {
        "password",
        "pwd",
        "client_secret",
        "access_token",
        "api_key",
        "private_key",
        "private_key_password",
        "secret",
        "auth_token",
    }
)


def _credential_gaps(options: dict) -> list[str]:
    return sorted(k for k in options if k.lower() in _SECRET_KEYS or options[k] in ("", None, "REDACTED"))


def apply_connection(conn: dict, *, auth: AuthManager, dry_run: bool) -> dict:
    from databricks.sdk.service.catalog import ConnectionType

    name = conn["connection_name"]
    raw_type = conn.get("connection_type", "")
    options = dict(conn.get("options", {}))
    comment = conn.get("comment")

    obj_key = f"CONNECTION_{name}"
    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] Would create connection %s (%s)", name, raw_type)
        return {
            "object_name": obj_key,
            "object_type": "connection",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }

    # Coerce to a ConnectionType enum. raw_type may be "SQLSERVER",
    # "sqlserver", or the enum repr "ConnectionType.SQLSERVER" (discovery
    # stores str(connection_type)). The SDK's connections.create() calls
    # .value on connection_type, so a passed-through string raises
    # "'str' object has no attribute 'value'" — NEVER fall back to a string.
    _type_str = str(raw_type).split(".")[-1].upper()
    try:
        conn_type_enum = ConnectionType(_type_str)
    except ValueError:
        try:
            conn_type_enum = ConnectionType[_type_str]
        except KeyError:
            return {
                "object_name": obj_key,
                "object_type": "connection",
                "status": "failed",
                "error_message": (
                    f"Unsupported connection_type {raw_type!r} (normalized to {_type_str!r}); "
                    f"not a known ConnectionType."
                ),
                "duration_seconds": time.time() - start,
            }

    # Idempotency / credential-gap pre-check. UC's GET never returns secret
    # options (e.g. user+password for SQLSERVER), so the worker cannot
    # recreate such a connection from the discovered options alone. If the
    # connection already exists on target — the operator recreated it with
    # credentials per the migration runbook, or a prior run made it — treat
    # as validated WITHOUT attempting create. Fail-closed: only short-circuit
    # on a definite hit (mirrors managed_table's _target_table_exists).
    try:
        existing = auth.target_client.connections.get(name=name)
    except Exception:  # noqa: BLE001 — NotFound / not checkable → proceed to create
        existing = None
    if existing is not None:
        return {
            "object_name": obj_key,
            "object_type": "connection",
            "status": "validated",
            "error_message": "already existed on target",
            "duration_seconds": time.time() - start,
        }

    try:
        auth.target_client.connections.create(
            name=name,
            connection_type=conn_type_enum,  # type: ignore[arg-type]
            options=options,
            comment=comment,
        )
    except Exception as exc:  # noqa: BLE001
        err_text = str(exc).lower()
        if "must include" in err_text and "option" in err_text:
            # Mandatory credential options (user/password) are not exportable
            # from source — this is the known secret limitation, not an
            # unexpected error. Surface validation_failed (manual recreation
            # with credentials required), never a hard failure.
            return {
                "object_name": obj_key,
                "object_type": "connection",
                "status": "validation_failed",
                "error_message": (
                    f"Connection requires credentials not exportable from source "
                    f"({exc}). Recreate on target with credentials."
                ),
                "duration_seconds": time.time() - start,
            }
        if not ("already" in err_text and "exists" in err_text):
            return {
                "object_name": obj_key,
                "object_type": "connection",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": time.time() - start,
            }

    gaps = _credential_gaps(options)
    if gaps:
        return {
            "object_name": obj_key,
            "object_type": "connection",
            "status": "validation_failed",
            "error_message": (f"Connection created but credentials must be re-entered: {', '.join(gaps)}"),
            "duration_seconds": time.time() - start,
        }
    return {
        "object_name": obj_key,
        "object_type": "connection",
        "status": "validated",
        "error_message": None,
        "duration_seconds": time.time() - start,
    }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="connection_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d connection records.", len(rows))

    results: list[dict] = []
    for r in rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if not isinstance(meta, dict):
            continue
        results.append(apply_connection(meta, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Connections worker complete. %d validated, %d validation_failed, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "validation_failed"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
