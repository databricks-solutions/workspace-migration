"""Shared SQL execution utilities for migration workers."""

from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from common.auth import AuthManager

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 10
DEFAULT_POLL_TIMEOUT_SECONDS = 3600


def _state_name(state: object) -> str:
    """Return a warehouse/run state as a string, handling SDK versions that
    return either an Enum (with ``.value``) or a raw string."""
    if state is None:
        return ""
    return getattr(state, "value", None) or str(state)


def _wh_client(auth_mgr: AuthManager, use_source: bool):
    """Pick the source or target workspace client. Tracking-catalog
    (discovery_inventory / migration_status) writes must use the SOURCE
    workspace — the tracking catalog lives on the source metastore — while
    target-table operations use the target workspace."""
    return auth_mgr.source_client if use_source else auth_mgr.target_client


def find_warehouse(auth_mgr: AuthManager, *, use_source: bool = False) -> str:
    """Find the first available SQL warehouse on the target (or source) workspace."""
    client = _wh_client(auth_mgr, use_source)
    where = "source" if use_source else "target"
    warehouses = list(client.warehouses.list())
    for wh in warehouses:
        if _state_name(wh.state) in ("RUNNING", "STARTING"):
            logger.info("Using %s warehouse '%s' (%s).", where, wh.name, wh.id)
            return wh.id  # type: ignore[return-value]
    if warehouses:
        logger.info("No running %s warehouse; using '%s' (%s).", where, warehouses[0].name, warehouses[0].id)
        return warehouses[0].id  # type: ignore[return-value]
    msg = f"No SQL warehouse found on the {where} workspace."
    raise RuntimeError(msg)


def execute_and_poll(
    auth_mgr: AuthManager,
    warehouse_id: str,
    sql: str,
    poll_timeout: int = DEFAULT_POLL_TIMEOUT_SECONDS,
    *,
    use_source: bool = False,
) -> dict:
    """Execute SQL via statement execution API and poll until done.

    use_source=True runs on the SOURCE workspace warehouse (for tracking-catalog
    writes); default runs on the target workspace (for target-table ops).
    """
    from databricks.sdk.service.sql import StatementState

    client = _wh_client(auth_mgr, use_source)
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="0s",
    )
    statement_id = resp.statement_id

    elapsed = 0
    while elapsed < poll_timeout:
        status_resp = client.statement_execution.get_statement(statement_id)  # type: ignore[arg-type]
        state = status_resp.status.state if status_resp.status else None
        if state in (StatementState.SUCCEEDED,):
            return {"state": "SUCCEEDED", "statement_id": statement_id}
        if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            error_msg = ""
            if status_resp.status and status_resp.status.error:
                error_msg = status_resp.status.error.message or ""
            return {"state": str(state), "error": error_msg, "statement_id": statement_id}
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    return {"state": "TIMEOUT", "error": "Poll timeout exceeded", "statement_id": statement_id}


def execute_and_fetch(
    auth_mgr: AuthManager,
    warehouse_id: str,
    sql: str,
    poll_timeout: int = DEFAULT_POLL_TIMEOUT_SECONDS,
    *,
    use_source: bool = False,
) -> dict:
    """Like execute_and_poll but returns result rows on success.

    Used by workers running on NON-UC (No Isolation) compute, which can't read
    UC catalogs via their own spark session — they route UC reads through the
    (UC-capable) SQL warehouse instead. use_source picks the source workspace
    warehouse (tracking catalog) vs target (target tables). Returns
    ``{"state", "rows": list[list], "error"?, "statement_id"}``.
    """
    from databricks.sdk.service.sql import StatementState

    client = _wh_client(auth_mgr, use_source)
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="0s",
    )
    statement_id = resp.statement_id

    elapsed = 0
    while elapsed < poll_timeout:
        status_resp = client.statement_execution.get_statement(statement_id)  # type: ignore[arg-type]
        state = status_resp.status.state if status_resp.status else None
        if state in (StatementState.SUCCEEDED,):
            rows = []
            if status_resp.result and status_resp.result.data_array:
                rows = status_resp.result.data_array
            return {"state": "SUCCEEDED", "rows": rows, "statement_id": statement_id}
        if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            error_msg = ""
            if status_resp.status and status_resp.status.error:
                error_msg = status_resp.status.error.message or ""
            return {"state": str(state), "error": error_msg, "statement_id": statement_id}
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    return {"state": "TIMEOUT", "error": "Poll timeout exceeded", "statement_id": statement_id}


def warehouse_table_count(auth_mgr: AuthManager, warehouse_id: str, table_fqn: str) -> int:
    """SELECT COUNT(*) on *table_fqn* via the SQL warehouse (UC-capable).

    For target validation from workers on NON-UC compute, which can't read UC
    tables on their own spark session. Raises on a failed/empty result.
    """
    res = execute_and_fetch(auth_mgr, warehouse_id, f"SELECT COUNT(*) AS cnt FROM {table_fqn}")
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"count query failed for {table_fqn}: {res.get('error', res['state'])}")
    rows = res.get("rows") or []
    if not rows or not rows[0]:
        raise RuntimeError(f"count query returned no rows for {table_fqn}")
    return int(rows[0][0])


def _sql_literal(value: object) -> str:
    """Render a Python value as a SQL literal for an INSERT ... VALUES row."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


# Column order for migration_status INSERTs — mirrors
# TrackingManager.append_migration_status (migrated_at appended as
# current_timestamp()).
_MIGRATION_STATUS_COLUMNS = (
    "object_name",
    "object_type",
    "status",
    "error_message",
    "job_run_id",
    "task_run_id",
    "source_row_count",
    "target_row_count",
    "duration_seconds",
)


def append_migration_status_via_warehouse(
    auth_mgr: AuthManager,
    warehouse_id: str,
    tracking_fqn: str,
    records: list[dict],
    *,
    job_run_id: str | None = None,
) -> None:
    """Append migration_status rows through the SQL warehouse (UC-capable).

    Lets workers on NON-UC (No Isolation) compute record status without a UC
    spark write. Mirrors TrackingManager.append_migration_status: same columns,
    job_run_id stamped from *job_run_id* where a record left it None, and
    migrated_at = current_timestamp(). No-op for an empty list.
    """
    if not records:
        return
    value_rows = []
    for r in records:
        jr = r.get("job_run_id")
        if jr is None:
            jr = job_run_id
        cells = []
        for col in _MIGRATION_STATUS_COLUMNS:
            cells.append(_sql_literal(jr if col == "job_run_id" else r.get(col)))
        value_rows.append("(" + ", ".join(cells) + ", current_timestamp())")
    col_list = ", ".join(_MIGRATION_STATUS_COLUMNS) + ", migrated_at"
    sql = f"INSERT INTO {tracking_fqn}.migration_status ({col_list}) VALUES " + ", ".join(value_rows)
    res = execute_and_poll(auth_mgr, warehouse_id, sql, use_source=True)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"migration_status insert failed: {res.get('error', res['state'])}")


# discovery_inventory columns (discovered_at appended as current_timestamp()).
# Mirrors tracking.discovery_schema() / discovery_row().
_DISCOVERY_INVENTORY_COLUMNS = (
    "object_name",
    "object_type",
    "source_type",
    "catalog_name",
    "schema_name",
    "row_count",
    "size_bytes",
    "is_dlt_managed",
    "pipeline_id",
    "create_statement",
    "data_category",
    "table_type",
    "provider",
    "storage_location",
    "format",
    "metadata_json",
)


def write_discovery_inventory_via_warehouse(
    auth_mgr: AuthManager,
    warehouse_id: str,
    tracking_fqn: str,
    rows: list[dict],
    *,
    source_type: str,
) -> None:
    """Write discovery_inventory rows for *source_type* through the SQL warehouse.

    Lets discovery of sources that require NON-UC (No Isolation) compute — e.g.
    hive_metastore tables on ADLS, whose Delta ``_delta_log`` can't be read on
    serverless/UC compute — record their inventory without a UC ``spark`` write.
    Idempotent replace: DELETEs this source_type's existing rows then INSERTs the
    new set (this source_type is owned solely by this writer, so no MERGE needed).
    discovered_at is set to current_timestamp().
    """
    del_sql = f"DELETE FROM {tracking_fqn}.discovery_inventory WHERE source_type = {_sql_literal(source_type)}"
    res = execute_and_poll(auth_mgr, warehouse_id, del_sql, use_source=True)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"discovery_inventory delete failed: {res.get('error', res['state'])}")
    if not rows:
        return
    value_rows = []
    for r in rows:
        cells = [_sql_literal(r.get(col)) for col in _DISCOVERY_INVENTORY_COLUMNS]
        value_rows.append("(" + ", ".join(cells) + ", current_timestamp())")
    col_list = ", ".join(_DISCOVERY_INVENTORY_COLUMNS) + ", discovered_at"
    sql = f"INSERT INTO {tracking_fqn}.discovery_inventory ({col_list}) VALUES " + ", ".join(value_rows)
    res = execute_and_poll(auth_mgr, warehouse_id, sql, use_source=True)
    if res["state"] != "SUCCEEDED":
        raise RuntimeError(f"discovery_inventory insert failed: {res.get('error', res['state'])}")


def rewrite_ddl(ddl: str, from_pattern: str, to_replacement: str) -> str:
    """Rewrite DDL statement using regex substitution.

    Examples:
        rewrite_ddl(ddl, r"CREATE\\s+TABLE\\b", "CREATE TABLE IF NOT EXISTS")
        rewrite_ddl(ddl, r"CREATE\\s+VIEW\\b", "CREATE OR REPLACE VIEW")
        rewrite_ddl(ddl, r"CREATE\\s+FUNCTION\\b", "CREATE OR REPLACE FUNCTION")
    """
    return re.sub(from_pattern, to_replacement, ddl, count=1, flags=re.IGNORECASE)
