from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMigrateFunction:
    """Tests for the functions_worker.migrate_function function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        explorer = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "explorer": explorer,
            "wh_id": "wh-fn-1",
        }

    @patch("migrate.functions_worker.time")
    @patch("migrate.functions_worker.rewrite_ddl")
    @patch("migrate.functions_worker.execute_and_poll")
    def test_migrate_success(self, mock_execute, mock_rewrite, mock_time):
        from migrate.functions_worker import migrate_function

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_rewrite.return_value = "CREATE OR REPLACE FUNCTION `cat`.`sch`.`my_udf`(x INT) RETURNS INT RETURN x + 1"
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        deps[
            "explorer"
        ].get_function_ddl.return_value = "CREATE FUNCTION `cat`.`sch`.`my_udf`(x INT) RETURNS INT RETURN x + 1"

        func_info = {"object_name": "`cat`.`sch`.`my_udf`"}
        result = migrate_function(func_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "function"
        assert result["error_message"] is None
        deps["tracker"].append_migration_status.assert_called_once()
        mock_execute.assert_called_once()

    @patch("migrate.functions_worker.time")
    @patch("migrate.functions_worker.rewrite_ddl")
    @patch("migrate.functions_worker.execute_and_poll")
    def test_migrate_dry_run(self, mock_execute, mock_rewrite, mock_time):
        from migrate.functions_worker import migrate_function

        mock_time.time.side_effect = [100.0, 100.1]
        mock_rewrite.return_value = "CREATE OR REPLACE FUNCTION `cat`.`sch`.`fn1`() RETURNS INT RETURN 1"

        deps = self._make_deps(dry_run=True)
        deps["explorer"].get_function_ddl.return_value = "CREATE FUNCTION `cat`.`sch`.`fn1`() RETURNS INT RETURN 1"

        func_info = {"object_name": "`cat`.`sch`.`fn1`"}
        result = migrate_function(func_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()


# test_migrate_ddl_rewrite removed: the previous version stubbed
# explorer.get_function_ddl with a pre-formed DDL string and then asserted that
# string was passed through, which taught us nothing about the actual DDL
# construction. The real bug (body-only output) lived in
# CatalogExplorer.get_function_ddl and is now covered in test_catalog_utils.py.


class TestMigrateFunctionFailures:
    """``migrate_function`` must turn downstream DDL failures and DDL-fetch
    errors into status='failed' tracking rows rather than raising."""

    def _deps(self):
        cfg = MagicMock()
        cfg.dry_run = False
        return {
            "config": cfg,
            "auth": MagicMock(),
            "tracker": MagicMock(),
            "explorer": MagicMock(),
            "wh_id": "wh-fn-1",
        }

    @patch("migrate.functions_worker.time")
    @patch("migrate.functions_worker.rewrite_ddl")
    @patch("migrate.functions_worker.execute_and_poll")
    def test_failed_sql_marks_function_failed(
        self,
        mock_execute,
        mock_rewrite,
        mock_time,
    ):
        from migrate.functions_worker import migrate_function

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_rewrite.return_value = "CREATE OR REPLACE FUNCTION `cat`.`sch`.`fn1`() RETURNS INT RETURN 1"
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PARSE_SYNTAX_ERROR",
            "statement_id": "s-1",
        }

        deps = self._deps()
        deps["explorer"].get_function_ddl.return_value = "CREATE FUNCTION ..."

        result = migrate_function({"object_name": "`cat`.`sch`.`fn1`"}, **deps)
        assert result["status"] == "failed"
        assert "PARSE_SYNTAX_ERROR" in result["error_message"]

    @patch("migrate.functions_worker.time")
    def test_ddl_fetch_failure_marks_function_failed(self, mock_time):
        """If explorer.get_function_ddl raises (e.g. routine was dropped
        between discovery and migrate), we want a status=failed row with
        a useful error, not a raised exception."""
        from migrate.functions_worker import migrate_function

        mock_time.time.side_effect = [100.0, 100.1]
        deps = self._deps()
        deps["explorer"].get_function_ddl.side_effect = Exception("ROUTINE_NOT_FOUND")

        result = migrate_function({"object_name": "`cat`.`sch`.`fn1`"}, **deps)
        assert result["status"] == "failed"
        assert "ROUTINE_NOT_FOUND" in result["error_message"]
