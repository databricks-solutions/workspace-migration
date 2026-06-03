"""Unit tests for common.sql_utils module."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl

# ---------------------------------------------------------------------------
# find_warehouse
# ---------------------------------------------------------------------------


def _make_warehouse(name: str, wh_id: str, state_value: str) -> MagicMock:
    wh = MagicMock()
    wh.name = name
    wh.id = wh_id
    wh.state.value = state_value
    return wh


class TestFindWarehouse:
    def test_find_warehouse_running(self):
        auth_mgr = MagicMock()
        auth_mgr.target_client.warehouses.list.return_value = [
            _make_warehouse("stopped-wh", "wh-001", "TERMINATED"),
            _make_warehouse("active-wh", "wh-002", "RUNNING"),
        ]

        result = find_warehouse(auth_mgr)

        assert result == "wh-002"

    def test_find_warehouse_starting(self):
        auth_mgr = MagicMock()
        auth_mgr.target_client.warehouses.list.return_value = [
            _make_warehouse("starting-wh", "wh-010", "STARTING"),
        ]

        result = find_warehouse(auth_mgr)

        assert result == "wh-010"

    def test_find_warehouse_no_running_uses_first(self):
        auth_mgr = MagicMock()
        auth_mgr.target_client.warehouses.list.return_value = [
            _make_warehouse("first-wh", "wh-100", "TERMINATED"),
            _make_warehouse("second-wh", "wh-200", "TERMINATED"),
        ]

        result = find_warehouse(auth_mgr)

        assert result == "wh-100"

    def test_find_warehouse_empty_raises(self):
        auth_mgr = MagicMock()
        auth_mgr.target_client.warehouses.list.return_value = []

        with pytest.raises(RuntimeError, match="No SQL warehouse found"):
            find_warehouse(auth_mgr)

    def test_find_warehouse_state_as_raw_string(self):
        """Some SDK versions return wh.state as a raw string instead of an
        Enum with .value. find_warehouse must tolerate both shapes."""
        wh = MagicMock()
        wh.name = "string-state-wh"
        wh.id = "wh-999"
        wh.state = "RUNNING"  # raw string, no .value attribute
        auth_mgr = MagicMock()
        auth_mgr.target_client.warehouses.list.return_value = [wh]

        result = find_warehouse(auth_mgr)

        assert result == "wh-999"


# ---------------------------------------------------------------------------
# execute_and_poll  — StatementState is imported inside the function body,
# so we inject a mock into the databricks.sdk.service.sql module namespace.
# ---------------------------------------------------------------------------


def _ensure_mock_statement_state():
    """Ensure databricks.sdk.service.sql.StatementState is a usable mock enum.

    The Databricks SDK may or may not be installed in the test environment.
    We inject a lightweight mock module so that the ``from databricks.sdk
    .service.sql import StatementState`` inside *execute_and_poll* resolves
    to our mock.
    """
    state = type(
        "StatementState",
        (),
        {
            "SUCCEEDED": "SUCCEEDED",
            "FAILED": "FAILED",
            "CANCELED": "CANCELED",
            "CLOSED": "CLOSED",
        },
    )()

    # Build the module hierarchy if it doesn't exist yet.
    for mod_name in [
        "databricks",
        "databricks.sdk",
        "databricks.sdk.service",
        "databricks.sdk.service.sql",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = ModuleType(mod_name)

    sys.modules["databricks.sdk.service.sql"].StatementState = state  # type: ignore[attr-defined]
    return state


class TestExecuteAndPoll:
    def setup_method(self):
        self.mock_state = _ensure_mock_statement_state()

    @patch("common.sql_utils.time.sleep", return_value=None)
    def test_execute_and_poll_success(self, _mock_sleep):
        auth_mgr = MagicMock()
        auth_mgr.target_client.statement_execution.execute_statement.return_value = MagicMock(
            statement_id="stmt-123",
        )

        status_resp = MagicMock()
        status_resp.status.state = self.mock_state.SUCCEEDED
        auth_mgr.target_client.statement_execution.get_statement.return_value = status_resp

        result = execute_and_poll(auth_mgr, "wh-1", "SELECT 1")

        assert result["state"] == "SUCCEEDED"
        assert result["statement_id"] == "stmt-123"

    @patch("common.sql_utils.time.sleep", return_value=None)
    def test_execute_and_poll_failure(self, _mock_sleep):
        auth_mgr = MagicMock()
        auth_mgr.target_client.statement_execution.execute_statement.return_value = MagicMock(
            statement_id="stmt-456",
        )

        status_resp = MagicMock()
        status_resp.status.state = self.mock_state.FAILED
        status_resp.status.error.message = "SYNTAX_ERROR near 'SELET'"
        auth_mgr.target_client.statement_execution.get_statement.return_value = status_resp

        result = execute_and_poll(auth_mgr, "wh-1", "SELET 1")

        assert result["state"] == str(self.mock_state.FAILED)
        assert "SYNTAX_ERROR" in result["error"]
        assert result["statement_id"] == "stmt-456"

    @patch("common.sql_utils.time.sleep", return_value=None)
    def test_execute_and_poll_timeout(self, _mock_sleep):
        auth_mgr = MagicMock()
        auth_mgr.target_client.statement_execution.execute_statement.return_value = MagicMock(
            statement_id="stmt-789",
        )

        status_resp = MagicMock()
        # Return a state that is not terminal (PENDING)
        status_resp.status.state = "PENDING"
        auth_mgr.target_client.statement_execution.get_statement.return_value = status_resp

        result = execute_and_poll(auth_mgr, "wh-1", "SELECT 1", poll_timeout=1)

        assert result["state"] == "TIMEOUT"
        assert result["statement_id"] == "stmt-789"


# ---------------------------------------------------------------------------
# rewrite_ddl
# ---------------------------------------------------------------------------


class TestRewriteDdl:
    def test_rewrite_create_table(self):
        ddl = "CREATE TABLE `cat`.`sch`.`tbl` (id INT, name STRING)"
        result = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")

        assert result.startswith("CREATE TABLE IF NOT EXISTS")
        assert "`cat`.`sch`.`tbl`" in result

    def test_rewrite_create_view(self):
        ddl = "CREATE VIEW `cat`.`sch`.`v1` AS SELECT * FROM t"
        result = rewrite_ddl(ddl, r"CREATE\s+VIEW\b", "CREATE OR REPLACE VIEW")

        assert result.startswith("CREATE OR REPLACE VIEW")

    def test_rewrite_create_function(self):
        ddl = "CREATE FUNCTION `cat`.`sch`.`fn` (x INT) RETURNS INT RETURN x + 1"
        result = rewrite_ddl(ddl, r"CREATE\s+FUNCTION\b", "CREATE OR REPLACE FUNCTION")

        assert result.startswith("CREATE OR REPLACE FUNCTION")

    def test_rewrite_case_insensitive(self):
        ddl = "create table foo (id INT)"
        result = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")

        assert result == "CREATE TABLE IF NOT EXISTS foo (id INT)"

    def test_rewrite_only_first_occurrence(self):
        ddl = "CREATE TABLE t1 (col STRING COMMENT 'see CREATE TABLE docs')"
        result = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")

        assert result.startswith("CREATE TABLE IF NOT EXISTS")
        # The second "CREATE TABLE" in the comment must remain unchanged.
        assert "see CREATE TABLE docs" in result
