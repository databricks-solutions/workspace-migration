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
import json
import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.lfc_utils import classify_pipeline, extract_table_configs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pre_check_lfc")

# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def find_blockers(target_client, rows: list[dict], *, target_connection_name: str) -> list[str]:
    """Hard blockers for query-based pipelines: target connection + each
    destination catalog.schema must exist. Non-query-based rows are skipped."""
    blockers: list[str] = []
    checked_conn = False
    for row in rows:
        definition = (json.loads(row.get("metadata_json") or "{}") or {}).get("definition") or {}
        kind, _ = classify_pipeline(definition)
        if kind != "query_based":
            continue
        if not checked_conn:
            try:
                target_client.connections.get(target_connection_name)
            except Exception:  # noqa: BLE001
                blockers.append(f"Target UC connection '{target_connection_name}' not found.")
            checked_conn = True
        for tc in extract_table_configs(definition):
            fqn = f"{tc['destination_catalog']}.{tc['destination_schema']}"
            try:
                target_client.schemas.get(fqn)
            except Exception:  # noqa: BLE001
                blockers.append(f"Destination schema '{fqn}' not found on target.")
    return blockers


# COMMAND ----------
def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    rows = tracker.get_pending_objects("lfc_pipeline")
    conn = config.lfc_target_connection_name
    blockers = find_blockers(auth.target_client, rows, target_connection_name=conn)
    status = "PASS" if not blockers else "FAIL"
    tracker.append_pre_check_results([{
        "check_name": "lfc_query_based_prereqs",
        "status": status,
        "message": "" if not blockers else "; ".join(sorted(set(blockers))),
        "action_required": "" if not blockers else "Run migrate_uc + migrate connection first, then re-run.",
    }])
    if blockers:
        raise RuntimeError(f"migrate_lfc pre-check FAILED: {sorted(set(blockers))}")
    logger.info("[lfc] pre-check PASS — %d pipeline row(s).", len(rows))


# COMMAND ----------
if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
