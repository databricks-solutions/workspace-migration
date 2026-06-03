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

    # Best-effort ConnectionType coercion; fall back to raw string.
    try:
        conn_type_enum = ConnectionType(raw_type.upper())  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001
        conn_type_enum = raw_type  # SDK accepts strings too on newer versions

    try:
        auth.target_client.connections.create(
            name=name,
            connection_type=conn_type_enum,  # type: ignore[arg-type]
            options=options,
            comment=comment,
        )
    except Exception as exc:  # noqa: BLE001
        err_text = str(exc).lower()
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
