"""Unit tests for comments_worker — Phase 3 Task 32.

Delta DEEP CLONE preserves comments/TBLPROPERTIES, so this worker only
runs for catalogs, schemas, and non-Delta tables. Tests focus on:
  - COMMENT ON SQL is correctly shaped for each securable type
  - single-quote escaping
  - dry_run skips execution
  - execute_and_poll failure → status=failed with error captured
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestCommentsWorkerEmit:
    """``_emit_comment`` is the single path every securable takes. Cover
    it directly — the higher-level ``run()`` just enumerates rows."""

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_catalog_comment_shape(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        result = _emit_comment(
            "CATALOG",
            "`c`",
            "top-level catalog",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        sql = mock_execute.call_args[0][2]
        assert sql == "COMMENT ON CATALOG `c` IS 'top-level catalog'"
        assert result["status"] == "validated"
        assert result["object_type"] == "comment"
        assert result["object_name"] == "COMMENT_CATALOG_`c`"

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_schema_comment_shape(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        _emit_comment(
            "SCHEMA",
            "`c`.`s`",
            "schema desc",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        sql = mock_execute.call_args[0][2]
        assert sql == "COMMENT ON SCHEMA `c`.`s` IS 'schema desc'"

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_single_quotes_doubled(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        _emit_comment(
            "TABLE",
            "`c`.`s`.`t`",
            "owner's table",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        sql = mock_execute.call_args[0][2]
        # single quote in body should become '' so the SQL is valid
        assert "'owner''s table'" in sql

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_dry_run_short_circuits(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.0]
        result = _emit_comment(
            "TABLE",
            "`c`.`s`.`t`",
            "hi",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=True,
        )
        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_failed_execution_captured(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.5]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PERMISSION_DENIED",
            "statement_id": "s",
        }
        result = _emit_comment(
            "TABLE",
            "`c`.`s`.`t`",
            "hi",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert result["status"] == "failed"
        assert "PERMISSION_DENIED" in result["error_message"]


class TestCommentsWorkerErrorSuppression:
    """``_SuppressLog`` turns a raised exception into a ``failed`` row
    rather than aborting the whole worker. Critical for the loop —
    one bad catalog can't halt the rest."""

    def test_exception_becomes_failed_row(self):
        from migrate.comments_worker import _SuppressLog

        results: list[dict] = []
        with _SuppressLog(results, "mycatalog", "CATALOG"):
            raise RuntimeError("boom")
        assert len(results) == 1
        row = results[0]
        assert row["status"] == "failed"
        assert row["object_type"] == "comment"
        assert row["object_name"] == "COMMENT_CATALOG_mycatalog"
        assert "boom" in row["error_message"]

    def test_no_exception_no_row(self):
        from migrate.comments_worker import _SuppressLog

        results: list[dict] = []
        with _SuppressLog(results, "mycatalog", "CATALOG"):
            pass
        assert results == []


