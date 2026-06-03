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
# Vector Search pre-check: a Delta Sync index can only be recreated if its
# source Delta table already exists on target. Fail the job up-front if any are
# missing (Direct Access indexes have no source table and are excluded).

import json
import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pre_check_vector_search")

# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def find_missing_source_tables(target_client, rows: list[dict]) -> list[str]:
    """Return source-table FQNs that are absent on target, for Delta Sync rows only."""
    missing: list[str] = []
    for row in rows:
        definition = (json.loads(row.get("metadata_json") or "{}") or {}).get("definition") or {}
        if not str(definition.get("index_type", "")).upper().endswith("DELTA_SYNC"):
            continue  # Direct Access — no source table
        src = (definition.get("delta_sync_index_spec") or {}).get("source_table")
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

    rows = tracker.get_pending_objects("vector_search_index")
    missing = find_missing_source_tables(auth.target_client, rows)

    status = "PASS" if not missing else "FAIL"
    message = "" if not missing else f"Missing source tables on target: {sorted(set(missing))}"
    action = "" if not missing else "Run migrate_uc first so the source tables exist, then re-run."
    tracker.append_pre_check_results(
        [
            {
                "check_name": "vector_search_source_tables",
                "status": status,
                "message": message,
                "action_required": action,
            }
        ]
    )

    if missing:
        raise RuntimeError(
            "migrate_vector_search pre-check FAILED — source Delta tables absent on "
            f"target for {len(set(missing))} index(es): {sorted(set(missing))}. "
            "Run migrate_uc first so the source tables exist, then re-run."
        )
    logger.info(
        "[vector_search] pre-check PASS — %d index row(s), all source tables present.",
        len(rows),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
