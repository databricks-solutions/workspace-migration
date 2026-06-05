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
# ABAC Policies Worker (Phase 3 Task 31).
#
# Recreates ABAC policies on target via POST /api/2.1/unity-catalog/policies
# with the captured policy JSON. Preview feature — if target doesn't expose
# the endpoint, failures surface as worker errors rather than silent skips
# (unlike discovery, which swallows the GET to avoid failing runs).

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("policies_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def apply_policy(definition: dict, *, auth: AuthManager, dry_run: bool) -> dict:
    name = definition.get("name") or "<unnamed>"
    obj_key = f"POLICY_{name}"

    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] Would POST policy %s", name)
        return {
            "object_name": obj_key,
            "object_type": "policy",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }

    # Strip server-generated read-only fields — the GET response carries them
    # but POST /policies rejects/ignores them. Keep the policy spec
    # (name, on_securable_*, to_principals, policy_type, row_filter/
    # column_mask, for_securable_type, comment, when_condition, match_columns,
    # use_session_identity, except_principals).
    _readonly = {"id", "created_at", "created_by", "updated_at", "updated_by"}
    body = {k: v for k, v in definition.items() if k not in _readonly}

    try:
        auth.target_client.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/policies",
            body=body,
        )
        return {
            "object_name": obj_key,
            "object_type": "policy",
            "status": "validated",
            "error_message": None,
            "duration_seconds": time.time() - start,
        }
    except Exception as exc:  # noqa: BLE001
        # Idempotency: POST /policies has no PUT-or-create variant. On retry
        # the policy may already exist — treat "already exists" as validated.
        err_text = str(exc).lower()
        if "already" in err_text and "exists" in err_text:
            return {
                "object_name": obj_key,
                "object_type": "policy",
                "status": "validated",
                "error_message": "already existed on target",
                "duration_seconds": time.time() - start,
            }
        return {
            "object_name": obj_key,
            "object_type": "policy",
            "status": "failed",
            "error_message": str(exc),
            "duration_seconds": time.time() - start,
        }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="policy_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d policy records.", len(rows))

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
        definition = meta.get("definition") or {}
        res = apply_policy(definition, auth=auth, dry_run=config.dry_run)
        results.append(res)

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Policies worker complete. %d validated, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
