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

    def test_escape_handles_backslashes(self):
        """Backslashes must be doubled (Spark SQL interprets ``\\`` in literals)."""
        from migrate.comments_worker import _escape

        assert _escape("path\\to\\thing") == "path\\\\to\\\\thing"

    def test_escape_strips_semicolons(self):
        """Semicolons would terminate the COMMENT ON ... IS '...' statement."""
        from migrate.comments_worker import _escape

        assert _escape("note; DROP TABLE users") == "note DROP TABLE users"

    def test_escape_combined_order(self):
        """Backslash doubling must happen before single-quote doubling so the
        escape-doubled ``\\'`` doesn't get re-broken; semicolons dropped."""
        from migrate.comments_worker import _escape

        assert _escape("O'Brien\\; etc") == "O''Brien\\\\ etc"

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


class TestCommentsWorkerBatching:
    """M7: ``run()`` issues at most one info_schema query per
    (catalog, schema) per info_schema view, not one per object."""

    def _fake_row(self, **kw):
        r = MagicMock()
        for k, v in kw.items():
            setattr(r, k, v)
        return r

    def _make_collectable(self, rows):
        m = MagicMock()
        m.collect.return_value = rows
        return m

    def test_run_batches_info_schema_by_schema(self, monkeypatch):
        """With 2 non-Delta tables in (c1, s1), expect 1 columns query and
        1 tables query against c1.information_schema, not 2 of each."""
        from migrate import comments_worker

        issued: list[str] = []

        def fake_sql(text):
            issued.append(text)
            if "discovery_inventory" in text and "DISTINCT catalog_name " in text and "schema_name" not in text:
                return self._make_collectable([self._fake_row(catalog_name="c1")])
            if "discovery_inventory" in text and "DISTINCT catalog_name, schema_name" in text:
                return self._make_collectable([
                    self._fake_row(catalog_name="c1", schema_name="s1"),
                ])
            if "discovery_inventory" in text and "lower(format) <> 'delta'" in text:
                return self._make_collectable([
                    self._fake_row(object_name="`c1`.`s1`.`t1`", format="parquet"),
                    self._fake_row(object_name="`c1`.`s1`.`t2`", format="parquet"),
                ])
            if "discovery_inventory" in text and "object_type IN ('external_table','managed_table')" in text:
                return self._make_collectable([
                    self._fake_row(object_name="`c1`.`s1`.`t1`"),
                    self._fake_row(object_name="`c1`.`s1`.`t2`"),
                ])
            if "discovery_inventory" in text and "object_type = 'volume'" in text:
                return self._make_collectable([])
            if "information_schema.catalogs" in text:
                return self._make_collectable([self._fake_row(comment=None)])
            if "information_schema.schemata" in text:
                return self._make_collectable([self._fake_row(comment=None)])
            if "information_schema.tables" in text:
                return self._make_collectable([])
            if "information_schema.columns" in text:
                return self._make_collectable([])
            if "information_schema.volumes" in text:
                return self._make_collectable([])
            return self._make_collectable([])

        spark = MagicMock()
        spark.sql.side_effect = fake_sql
        dbutils = MagicMock()

        fake_config = MagicMock(
            tracking_catalog="t",
            tracking_schema="ts",
            dry_run=True,
        )
        monkeypatch.setattr(
            comments_worker.MigrationConfig,
            "from_workspace_file",
            classmethod(lambda cls: fake_config),
        )
        monkeypatch.setattr(comments_worker, "AuthManager", MagicMock())
        monkeypatch.setattr(comments_worker, "TrackingManager", MagicMock())
        monkeypatch.setattr(comments_worker, "find_warehouse", lambda auth: "wh1")

        comments_worker.run(dbutils, spark)

        columns_qs = [q for q in issued if "information_schema.columns" in q]
        tables_qs = [q for q in issued if "information_schema.tables" in q]
        volumes_qs = [q for q in issued if "information_schema.volumes" in q]

        assert len(columns_qs) == 1, (
            f"expected 1 batched columns query per schema, got {len(columns_qs)}: {columns_qs}"
        )
        assert len(tables_qs) == 1, (
            f"expected 1 batched tables query per schema, got {len(tables_qs)}: {tables_qs}"
        )
        # Volumes query fires per schema regardless of how many volumes —
        # the schema in this fixture has zero volumes, but the batched
        # SELECT still runs.
        assert len(volumes_qs) == 1


