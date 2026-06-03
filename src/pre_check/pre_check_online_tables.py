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
# Online Tables pre-check: an online table can only be recreated if its source
# Delta table already exists on target. Fail the job up-front if any are missing.

import json
import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pre_check_online_tables")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def find_missing_source_tables(target_client, rows: list[dict]) -> list[str]:
    """Return source-table FQNs absent on target for the given online_table rows."""
    missing: list[str] = []
    for row in rows:
        definition = (json.loads(row.get("metadata_json") or "{}") or {}).get("definition") or {}
        src = (definition.get("spec") or {}).get("source_table_full_name")
        if not src:
            continue
        try:
            target_client.tables.get(src)
        except Exception as exc:  # noqa: BLE001 — any failure (absent / transient / permission) blocks the gate
            logger.warning("tables.get(%r) failed — treating source table as absent: %s", src, exc)
            missing.append(src)
    return missing


# COMMAND ----------


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows = tracker.get_pending_objects("online_table")
    missing = find_missing_source_tables(auth.target_client, rows)

    status = "PASS" if not missing else "FAIL"
    message = "" if not missing else f"Missing source tables on target: {sorted(set(missing))}"
    action = "" if not missing else "Run migrate_uc first so the source tables exist, then re-run."
    tracker.append_pre_check_results(
        [
            {
                "check_name": "online_table_source_tables",
                "status": status,
                "message": message,
                "action_required": action,
            }
        ]
    )

    if missing:
        raise RuntimeError(
            "migrate_online_tables pre-check FAILED — source Delta tables absent on "
            f"target for {len(set(missing))} online table(s): {sorted(set(missing))}. "
            "Run migrate_uc first so the source tables exist, then re-run."
        )
    logger.info("[online_tables] pre-check PASS — %d online table row(s), all source tables present.", len(rows))


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
