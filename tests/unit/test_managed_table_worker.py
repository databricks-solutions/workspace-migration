from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestCloneTable:
    """Tests for the managed_table_worker.clone_table function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        # Realistic defaults: overwrite off, target does not pre-exist → the
        # normal CREATE OR REPLACE clone path runs. Individual tests override.
        config.overwrite_existing = False
        auth = MagicMock()
        tracker = MagicMock()
        # No staging by default → exercise the original-consumer DEEP CLONE
        # / CTAS branches the rest of the suite asserts on.
        tracker.get_staging_for_original.return_value = None
        validator = MagicMock()
        validator.validate_object_exists.return_value = False
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "validator": validator,
            "wh_id": "wh-123",
            "share_name": "cp_migration_share",
        }

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_clone_table_success(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 42,
            "target_count": 42,
        }

        table_info = {"object_name": "`cat`.`sch`.`tbl`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "managed_table"
        assert result["source_row_count"] == 42
        assert result["target_row_count"] == 42
        assert result["error_message"] is None
        deps["tracker"].append_migration_status.assert_called_once()
        mock_execute.assert_called_once()

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_clone_table_dry_run(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(dry_run=True)
        table_info = {"object_name": "`cat`.`sch`.`tbl`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_clone_table_clone_failure(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 115.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "TABLE_NOT_FOUND",
            "statement_id": "s-2",
        }

        deps = self._make_deps()
        table_info = {"object_name": "`cat`.`sch`.`tbl`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "failed"
        assert "TABLE_NOT_FOUND" in result["error_message"]

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_clone_table_validation_mismatch(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-3"}

        deps = self._make_deps()
        deps["validator"].validate_row_count.return_value = {
            "match": False,
            "source_count": 100,
            "target_count": 50,
        }

        table_info = {"object_name": "`cat`.`sch`.`tbl`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "validation_failed"
        assert "Row count mismatch" in result["error_message"]
        assert result["source_row_count"] == 100
        assert result["target_row_count"] == 50

    def test_clone_table_malformed_fqn(self):
        from migrate.managed_table_worker import clone_table

        deps = self._make_deps()
        table_info = {"object_name": "just_a_table_name"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "failed"
        assert "Malformed FQN" in result["error_message"]
        assert result["duration_seconds"] == 0.0

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_existing_target_is_not_overwritten_when_flag_off(self, mock_execute, mock_time):
        """Review finding #2: CREATE OR REPLACE … DEEP CLONE had no existence
        gate, so an orphaned-in_progress resume (or any re-trigger) re-clobbered
        the target. With overwrite_existing off and the target already present,
        the worker must validate without issuing a CREATE OR REPLACE."""
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]

        deps = self._make_deps()
        deps["config"].overwrite_existing = False
        deps["validator"].validate_object_exists.return_value = True  # target already there
        deps["validator"].validate_row_count.return_value = {
            "match": True, "source_count": 42, "target_count": 42,
        }

        result = clone_table({"object_name": "`cat`.`sch`.`tbl`"}, **deps)

        assert result["status"] == "validated"
        # No DEEP CLONE / CREATE OR REPLACE was issued.
        clone_sqls = [c.args[2] for c in mock_execute.call_args_list if "DEEP CLONE" in c.args[2]]
        assert clone_sqls == []

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_overwrite_existing_flag_forces_clone(self, mock_execute, mock_time):
        """With overwrite_existing=True, a present target IS replaced."""
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps()
        deps["config"].overwrite_existing = True
        deps["validator"].validate_object_exists.return_value = True
        deps["validator"].validate_row_count.return_value = {
            "match": True, "source_count": 42, "target_count": 42,
        }

        result = clone_table({"object_name": "`cat`.`sch`.`tbl`"}, **deps)

        assert result["status"] == "validated"
        clone_sqls = [c.args[2] for c in mock_execute.call_args_list if "DEEP CLONE" in c.args[2]]
        assert len(clone_sqls) == 1


class TestIcebergManagedTable:
    """Tests for the Iceberg Option A branch of clone_table."""

    def _make_deps(self, *, dry_run: bool = False, iceberg_strategy: str = "") -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        config.iceberg_strategy = iceberg_strategy
        auth = MagicMock()
        tracker = MagicMock()
        tracker.get_staging_for_original.return_value = None
        validator = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "validator": validator,
            "wh_id": "wh-ice",
            "share_name": "cp_migration_share",
        }

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_without_opt_in_is_skipped(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(iceberg_strategy="")  # not opted in
        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            "create_statement": "CREATE TABLE ...",
        }
        result = clone_table(table_info, **deps)

        # skipped_by_config (not plain 'skipped') so re-runs with opt-in
        # configured pick the table back up via get_pending_objects.
        assert result["status"] == "skipped_by_config"
        assert "iceberg_strategy" in result["error_message"]
        mock_execute.assert_not_called()

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_ddl_replay_success(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 130.0, 160.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps(iceberg_strategy="ddl_replay")
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 10,
            "target_count": 10,
        }

        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            "create_statement": ("CREATE TABLE `cat`.`sch`.`ice_tbl` USING ICEBERG AS SELECT 1"),
        }
        result = clone_table(table_info, **deps)

        assert result["status"] == "validated"
        assert result["source_row_count"] == 10
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("USING ICEBERG" in s for s in sqls)  # CREATE executed
        assert any("INSERT OVERWRITE" in s for s in sqls)  # idempotent re-ingest (#3)
        assert any("FROM `cp_migration_share_consumer`.`sch`.`ice_tbl`" in s for s in sqls)

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_is_idempotent_on_retry(self, mock_execute, mock_time):
        """Review finding #3: the Iceberg branch did CREATE (non-idempotent)
        then INSERT INTO (append). A retry after a successful CREATE would
        append a second full copy. The CREATE must be IF NOT EXISTS and the
        load must be INSERT OVERWRITE so a re-run can't double rows."""
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 130.0, 160.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps(iceberg_strategy="ddl_replay")
        deps["validator"].validate_row_count.return_value = {
            "match": True, "source_count": 10, "target_count": 10,
        }
        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            "create_statement": "CREATE TABLE `cat`.`sch`.`ice_tbl` (id INT) USING ICEBERG",
        }
        clone_table(table_info, **deps)

        sqls = [c.args[2] for c in mock_execute.call_args_list]
        create_sql = next(s for s in sqls if "USING ICEBERG" in s)
        insert_sql = next(s for s in sqls if "INSERT" in s)
        assert "CREATE TABLE IF NOT EXISTS" in create_sql
        assert "INSERT OVERWRITE" in insert_sql
        assert "INSERT INTO" not in insert_sql

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_missing_create_statement_fails(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(iceberg_strategy="ddl_replay")
        # Simulate: batch dict has no create_statement AND discovery row is
        # also missing it (real "no DDL anywhere" scenario).
        deps["tracker"].get_row.return_value = None
        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            "create_statement": "",
        }
        result = clone_table(table_info, **deps)

        assert result["status"] == "failed"
        assert "create_statement" in result["error_message"]

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_rehydrates_create_statement_from_tracker(self, mock_execute, mock_time):
        """When create_statement is stripped from the batch (to stay under
        Jobs' 3000-byte for_each limit), the worker falls back to
        tracker.get_row to re-hydrate it from discovery_inventory."""
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps(iceberg_strategy="ddl_replay")
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 7,
            "target_count": 7,
        }
        deps["tracker"].get_row.return_value = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "create_statement": "CREATE TABLE `cat`.`sch`.`ice_tbl` (id INT) USING ICEBERG",
        }

        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            # create_statement absent — stripped by orchestrator
        }
        result = clone_table(table_info, **deps)

        deps["tracker"].get_row.assert_called_once_with("managed_table", "`cat`.`sch`.`ice_tbl`")
        assert result["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("USING ICEBERG" in s for s in sqls)

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_rerun_after_strategy_flip(self, mock_execute, mock_time):
        """Backlog 2.5.1 — Iceberg re-run after strategy flip.

        Simulates the two-run operator workflow in a single test to pin the
        re-run contract end-to-end:

        * Run 1: ``iceberg_strategy=""`` — the same ``table_info`` dict yields
          ``status='skipped_by_config'`` (the distinct, structured skip reason,
          NOT plain ``'skipped'`` which operators treat as "intentionally
          dropped forever").
        * Run 2: operator flips ``iceberg_strategy='ddl_replay'`` and reruns.
          The SAME ``table_info`` now flows through the DDL + re-ingest path,
          executes BOTH the CREATE (``USING ICEBERG``) and the INSERT from the
          share-consumer catalog, and ends as ``validated``.

        If the worker ever reorders the branches or changes the skip-reason
        string, this test catches it — and with it, the signal that
        ``get_pending_objects`` uses to decide "retry this row or not" for
        Iceberg on the next run. Keeping the emitted status verbatim
        (``skipped_by_config``) is the single load-bearing contract between
        the worker and the tracking filter.
        """
        from migrate.managed_table_worker import clone_table

        # Enough time.time() values for both runs: start+end per-run, plus
        # the extra validator call on run 2.
        mock_time.time.side_effect = [100.0, 100.1, 200.0, 230.0, 260.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        # Same discovery row reused across both runs — the orchestrator
        # re-reads discovery_inventory each run, so the table_info payload
        # is identical modulo config.
        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            "create_statement": "CREATE TABLE `cat`.`sch`.`ice_tbl` (id INT) USING ICEBERG",
        }

        # --- Run 1: operator has NOT opted in ---
        deps1 = self._make_deps(iceberg_strategy="")
        r1 = clone_table(table_info, **deps1)

        assert r1["status"] == "skipped_by_config", (
            "First run must emit the structured skip reason so the re-run "
            "contract stays distinguishable from plain 'skipped'."
        )
        assert r1["status"] != "skipped"  # explicit negative assertion — re-run hinges on this
        # No SQL executed on the skip path.
        assert mock_execute.call_count == 0

        # --- Run 2: operator flips iceberg_strategy and reruns ---
        deps2 = self._make_deps(iceberg_strategy="ddl_replay")
        deps2["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 3,
            "target_count": 3,
        }
        r2 = clone_table(table_info, **deps2)

        assert r2["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        # Both ddl_replay statements must have run on the second pass.
        assert any("USING ICEBERG" in s for s in sqls), "CREATE DDL not replayed"
        assert any("INSERT OVERWRITE" in s for s in sqls), "Data re-ingest not executed"
        # Ingest reads from the consumer catalog exposed by the share.
        assert any("`cp_migration_share_consumer`.`sch`.`ice_tbl`" in s for s in sqls)

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_delta_format_still_uses_deep_clone(self, mock_execute, mock_time):
        """Regression: explicit format='delta' should behave like no format set."""
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 110.0, 115.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps()
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 5,
            "target_count": 5,
        }

        table_info = {
            "object_name": "`cat`.`sch`.`tbl`",
            "format": "delta",
        }
        result = clone_table(table_info, **deps)

        assert result["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("DEEP CLONE" in s for s in sqls)
        assert not any("INSERT INTO" in s for s in sqls)


class TestStagingCopyDeepClone:
    """Path A — staging_copy. When tracker.get_staging_for_original returns a
    staging FQN, clone_table must DEEP CLONE from the consumer-side staging
    path (`<consumer>.cp_migration_staging.<staging_table>`), NOT from the
    original consumer path. Staging tables preserve full schema/properties so
    DEEP CLONE works directly without any CTAS fallback.
    """

    def _make_deps(self, *, staging_fqn: str | None) -> dict:
        config = MagicMock()
        config.dry_run = False
        config.rls_cm_strategy = "staging_copy"
        tracker = MagicMock()
        tracker.get_staging_for_original.return_value = staging_fqn
        validator = MagicMock()
        validator.validate_row_count.return_value = {
            "match": True,
            "source_count": 5,
            "target_count": 5,
        }
        return {
            "config": config,
            "auth": MagicMock(),
            "tracker": tracker,
            "validator": validator,
            "wh_id": "wh-id",
            "share_name": "cp_migration_share",
        }

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_deep_clones_from_staging_consumer_path_when_staging_exists(
        self, mock_execute, mock_time
    ):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 110.0, 115.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps(
            staging_fqn="`tcat`.`cp_migration_staging`.`stg_abcdef123456`"
        )
        table_info = {"object_name": "`c`.`s`.`rls_table`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        # DEEP CLONE fires, sourced from consumer-side staging path.
        assert any("DEEP CLONE" in s for s in sqls), sqls
        assert any("cp_migration_staging" in s for s in sqls), sqls
        assert any("stg_abcdef123456" in s for s in sqls), sqls
        # Consumer catalog is `<share_name>_consumer`. Source must be
        # `<consumer>.cp_migration_staging.<staging_table>`, NOT the original.
        assert any(
            "`cp_migration_share_consumer`.`cp_migration_staging`.`stg_abcdef123456`" in s
            for s in sqls
        ), sqls
        # Must NOT be a CTAS, must NOT clone from the original consumer path.
        assert not any("AS SELECT * FROM" in s for s in sqls), sqls
        assert not any("`cp_migration_share_consumer`.`s`.`rls_table`" in s for s in sqls), sqls

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_no_staging_falls_through_to_original_consumer_deep_clone(
        self, mock_execute, mock_time
    ):
        """When tracker has no staging entry for the table, clone_table must
        fall through to DEEP CLONE from the original consumer path."""
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 110.0, 115.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps(staging_fqn=None)
        table_info = {"object_name": "`c`.`s`.`plain_table`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("DEEP CLONE" in s for s in sqls), sqls
        # Original consumer path, not staging.
        assert any("`cp_migration_share_consumer`.`s`.`plain_table`" in s for s in sqls), sqls
        assert not any("cp_migration_staging" in s for s in sqls), sqls
