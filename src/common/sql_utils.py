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


def find_warehouse(auth_mgr: AuthManager) -> str:
    """Find the first available SQL warehouse on the target workspace."""
    warehouses = list(auth_mgr.target_client.warehouses.list())
    for wh in warehouses:
        if _state_name(wh.state) in ("RUNNING", "STARTING"):
            logger.info("Using warehouse '%s' (%s).", wh.name, wh.id)
            return wh.id  # type: ignore[return-value]
    if warehouses:
        logger.info("No running warehouse found; using '%s' (%s).", warehouses[0].name, warehouses[0].id)
        return warehouses[0].id  # type: ignore[return-value]
    msg = "No SQL warehouse found on the target workspace."
    raise RuntimeError(msg)


def execute_and_poll(
    auth_mgr: AuthManager,
    warehouse_id: str,
    sql: str,
    poll_timeout: int = DEFAULT_POLL_TIMEOUT_SECONDS,
) -> dict:
    """Execute SQL on target via statement execution API and poll until done."""
    from databricks.sdk.service.sql import StatementState

    resp = auth_mgr.target_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="0s",
    )
    statement_id = resp.statement_id

    elapsed = 0
    while elapsed < poll_timeout:
        status_resp = auth_mgr.target_client.statement_execution.get_statement(statement_id)  # type: ignore[arg-type]
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


def rewrite_ddl(ddl: str, from_pattern: str, to_replacement: str) -> str:
    """Rewrite DDL statement using regex substitution.

    Examples:
        rewrite_ddl(ddl, r"CREATE\\s+TABLE\\b", "CREATE TABLE IF NOT EXISTS")
        rewrite_ddl(ddl, r"CREATE\\s+VIEW\\b", "CREATE OR REPLACE VIEW")
        rewrite_ddl(ddl, r"CREATE\\s+FUNCTION\\b", "CREATE OR REPLACE FUNCTION")
    """
    return re.sub(from_pattern, to_replacement, ddl, count=1, flags=re.IGNORECASE)
