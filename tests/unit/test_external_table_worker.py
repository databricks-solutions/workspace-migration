from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMigrateExternalTable:
    """Tests for the external_table_worker.migrate_external_table function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        explorer = MagicMock()
        validator = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "explorer": explorer,
            "validator": validator,
            "wh_id": "wh-456",
        }

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.rewrite_ddl")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_migrate_success(self, mock_execute, mock_rewrite, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_rewrite.return_value = (
            "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`ext_tbl` USING DELTA LOCATION 's3://bucket'"
        )
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        deps[
            "explorer"
        ].get_create_statement.return_value = "CREATE TABLE `cat`.`sch`.`ext_tbl` USING DELTA LOCATION 's3://bucket'"
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 200,
            "target_count": 200,
        }

        table_info = {"object_name": "`cat`.`sch`.`ext_tbl`"}
        result = migrate_external_table(table_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "external_table"
        assert result["source_row_count"] == 200
        assert result["error_message"] is None
        deps["tracker"].append_migration_status.assert_called_once()

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.rewrite_ddl")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_migrate_dry_run(self, mock_execute, mock_rewrite, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 100.1]
        mock_rewrite.return_value = "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`ext_tbl`"

        deps = self._make_deps(dry_run=True)
        deps["explorer"].get_create_statement.return_value = "CREATE TABLE `cat`.`sch`.`ext_tbl`"

        table_info = {"object_name": "`cat`.`sch`.`ext_tbl`"}
        result = migrate_external_table(table_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.external_table_worker.time")
    def test_migrate_ddl_failure(self, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 100.5]

        deps = self._make_deps()
        deps["explorer"].get_create_statement.side_effect = RuntimeError("catalog unavailable")

        table_info = {"object_name": "`cat`.`sch`.`ext_tbl`"}
        result = migrate_external_table(table_info, **deps)

        assert result["status"] == "failed"
        assert "Failed to get DDL" in result["error_message"]
        assert "catalog unavailable" in result["error_message"]

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.rewrite_ddl")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_migrate_ddl_rewrite(self, mock_execute, mock_rewrite, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        rewritten = "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`ext_tbl` USING DELTA"
        mock_rewrite.return_value = rewritten
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}

        deps = self._make_deps()
        deps["explorer"].get_create_statement.return_value = "CREATE TABLE `cat`.`sch`.`ext_tbl` USING DELTA"
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 10,
            "target_count": 10,
        }

        table_info = {"object_name": "`cat`.`sch`.`ext_tbl`"}
        migrate_external_table(table_info, **deps)

        # Verify rewrite_ddl was called with the CREATE TABLE pattern
        mock_rewrite.assert_called_once_with(
            "CREATE TABLE `cat`.`sch`.`ext_tbl` USING DELTA",
            r"CREATE\s+TABLE\b",
            "CREATE TABLE IF NOT EXISTS",
        )
        # Verify the rewritten DDL was passed to execute_and_poll
        mock_execute.assert_called_once_with(deps["auth"], "wh-456", rewritten)


class TestExternalTableWorkerStripsFilterMask:
    """external_table_worker must strip ``WITH ROW FILTER`` / inline ``MASK``
    clauses from the CREATE TABLE DDL before replay on target, because the
    filter/mask functions haven't been migrated yet at that stage
    (functions_worker depends on tables, not the other way round).

    Without this sanitization, replay fails with ROUTINE_NOT_FOUND.
    row_filters_worker / column_masks_worker apply the clauses later,
    after target functions exist.
    """

    def _make_deps(self, **overrides):
        deps = {
            "config": MagicMock(dry_run=False),
            "auth": MagicMock(),
            "tracker": MagicMock(),
            "explorer": MagicMock(),
            "validator": MagicMock(),
            "wh_id": "wh-strip",
        }
        deps.update(overrides)
        return deps

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_invokes_strip_filter_mask_clauses(self, mock_execute, mock_time):
        """The DDL passed to execute_and_poll must have had WITH ROW FILTER
        and inline MASK stripped. Verified by checking the actual SQL sent,
        not by mocking the sanitizer (which would let wiring regress
        silently)."""
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-strip"}

        deps = self._make_deps()
        # Source DDL mimics SHOW CREATE TABLE output on a UC table that
        # had ALTER TABLE SET ROW FILTER + ALTER COLUMN SET MASK applied.
        dirty_ddl = (
            "CREATE TABLE `c`.`s`.`t` ("
            "id INT MASK `c`.`s`.`mask_id`,"
            "region STRING"
            ") USING delta "
            "LOCATION 'abfss://x@y.dfs.core.windows.net/p' "
            "WITH ROW FILTER `c`.`s`.`rf` ON (region)"
        )
        deps["explorer"].get_create_statement.return_value = dirty_ddl
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 1,
            "target_count": 1,
        }

        table_info = {"object_name": "`c`.`s`.`t`"}
        migrate_external_table(table_info, **deps)

        executed_sql = mock_execute.call_args[0][2]
        # Both filter and mask clauses must be gone.
        assert "ROW FILTER" not in executed_sql, f"WITH ROW FILTER clause leaked into replayed DDL: {executed_sql}"
        assert "MASK" not in executed_sql, f"Inline MASK clause leaked into replayed DDL: {executed_sql}"
        # But the core CREATE remains.
        assert "CREATE TABLE" in executed_sql
        assert "LOCATION" in executed_sql


class TestExternalTableWorkerPreservesPartitioning:
    """CREATE TABLE with PARTITIONED BY / CLUSTER BY must survive the
    replay unchanged. Partition metadata is critical for query
    performance — silent loss would be a real-world blast radius.
    """

    def _make_deps(self, **overrides):
        deps = {
            "config": MagicMock(dry_run=False),
            "auth": MagicMock(),
            "tracker": MagicMock(),
            "explorer": MagicMock(),
            "validator": MagicMock(),
            "wh_id": "wh-part",
        }
        deps.update(overrides)
        return deps

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_partitioned_by_preserved_in_ddl(self, mock_execute, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-p"}

        deps = self._make_deps()
        dirty_ddl = (
            "CREATE TABLE `c`.`s`.`events` ("
            "event_id INT, region STRING, event_date DATE"
            ") USING DELTA "
            "PARTITIONED BY (region, event_date) "
            "LOCATION 'abfss://x@y.dfs.core.windows.net/events'"
        )
        deps["explorer"].get_create_statement.return_value = dirty_ddl
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 0,
            "target_count": 0,
        }

        migrate_external_table({"object_name": "`c`.`s`.`events`"}, **deps)

        replayed = mock_execute.call_args[0][2]
        assert "PARTITIONED BY (region, event_date)" in replayed, f"PARTITIONED BY clause stripped: {replayed}"
        # LOCATION also preserved.
        assert "LOCATION 'abfss://x@y.dfs.core.windows.net/events'" in replayed
        # CREATE TABLE → CREATE TABLE IF NOT EXISTS, not OR REPLACE.
        assert replayed.startswith("CREATE TABLE IF NOT EXISTS")

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_cluster_by_preserved_in_ddl(self, mock_execute, mock_time):
        """Liquid clustering (CLUSTER BY) is a newer feature. Same
        preservation contract."""
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-c"}

        deps = self._make_deps()
        dirty_ddl = (
            "CREATE TABLE `c`.`s`.`orders` ("
            "order_id INT, customer_id INT, region STRING"
            ") USING DELTA "
            "CLUSTER BY (customer_id, region) "
            "LOCATION 'abfss://x@y.dfs.core.windows.net/orders'"
        )
        deps["explorer"].get_create_statement.return_value = dirty_ddl
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 0,
            "target_count": 0,
        }

        migrate_external_table({"object_name": "`c`.`s`.`orders`"}, **deps)

        replayed = mock_execute.call_args[0][2]
        assert "CLUSTER BY (customer_id, region)" in replayed
        assert "LOCATION 'abfss://x@y.dfs.core.windows.net/orders'" in replayed

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_tblproperties_preserved_in_ddl(self, mock_execute, mock_time):
        """TBLPROPERTIES (Delta features, retention, etc.) must survive —
        dropping them would silently downgrade the table's Delta features."""
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-t"}

        deps = self._make_deps()
        dirty_ddl = (
            "CREATE TABLE `c`.`s`.`events` (id INT) "
            "USING DELTA "
            "LOCATION 'abfss://x@y.dfs.core.windows.net/events' "
            "TBLPROPERTIES ("
            "'delta.enableDeletionVectors' = 'true',"
            "'delta.autoOptimize.optimizeWrite' = 'true'"
            ")"
        )
        deps["explorer"].get_create_statement.return_value = dirty_ddl
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 0,
            "target_count": 0,
        }

        migrate_external_table({"object_name": "`c`.`s`.`events`"}, **deps)

        replayed = mock_execute.call_args[0][2]
        assert "TBLPROPERTIES" in replayed
        assert "'delta.enableDeletionVectors' = 'true'" in replayed
        assert "'delta.autoOptimize.optimizeWrite' = 'true'" in replayed
