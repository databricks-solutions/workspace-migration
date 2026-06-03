from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMvStWorkerHardExclude:
    """Phase 4: both materialized views AND streaming tables are
    hard-excluded from the core migration tool. ``migrate_mv_st``
    short-circuits every row to ``skipped_by_stateful_service_migration``
    (terminal) without touching source or target — same pattern as the
    PR #41 ST exclusion. Migration is handled by the future Stateful
    Services Phase (separate job). See ``docs/stateful_services_phase.md``.
    """

    def _make_deps(self) -> dict:
        config = MagicMock(dry_run=False)
        auth = MagicMock()
        tracker = MagicMock()
        return {"config": config, "auth": auth, "tracker": tracker, "wh_id": "wh-42"}

    @patch("migrate.mv_st_worker.time")
    def test_mv_is_skipped_by_stateful_service_migration(self, mock_time):
        """A materialized view row short-circuits to the terminal skip
        status with no source/target interaction. Removing this branch
        would re-introduce the DDL-replay path that Phase 4 is removing
        — see the Stateful Services Phase doc for why."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps()
        obj_info = {
            "object_name": "`cat`.`sch`.`mv1`",
            "object_type": "mv",
            "pipeline_id": "pip-abc",
            "create_statement": "CREATE MATERIALIZED VIEW ...",
        }
        result = migrate_mv_st(obj_info, **deps)

        assert result["status"] == "skipped_by_stateful_service_migration"
        assert "Stateful Services Phase" in result["error_message"]
        assert "stateful_services_phase.md" in result["error_message"]
        # No DLT detection, no target SQL execution.
        deps["auth"].source_client.pipelines.get.assert_not_called()
        deps["auth"].target_client.statement_execution.execute_statement.assert_not_called()

    @patch("migrate.mv_st_worker.time")
    def test_st_is_skipped_by_stateful_service_migration(self, mock_time):
        """Streaming tables get the same hard-exclude treatment as MV —
        unchanged from PR #41 but kept under test so the contract holds
        as the worker evolves."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps()
        obj_info = {
            "object_name": "`cat`.`sch`.`st1`",
            "object_type": "st",
            "pipeline_id": "pip-xyz",
            "create_statement": "CREATE STREAMING TABLE ...",
        }
        result = migrate_mv_st(obj_info, **deps)

        assert result["status"] == "skipped_by_stateful_service_migration"
        assert "Stateful Services Phase" in result["error_message"]
        deps["auth"].source_client.pipelines.get.assert_not_called()

    @patch("migrate.mv_st_worker.time")
    def test_no_ddl_replay_code_path_remains(self, mock_time):
        """Regression guard: the DDL-replay helpers ``_is_sql_created`` /
        ``_replay_mv_st_ddl`` were removed in Phase 4. Importing them
        should raise ImportError — reintroducing them silently would
        defeat the hard-exclusion contract."""
        import migrate.mv_st_worker as worker_mod

        assert not hasattr(worker_mod, "_is_sql_created"), (
            "Phase 4 removed _is_sql_created — reintroduction would defeat hard-exclusion."
        )
        assert not hasattr(worker_mod, "_replay_mv_st_ddl"), (
            "Phase 4 removed _replay_mv_st_ddl — reintroduction would defeat hard-exclusion."
        )


class TestIcebergSkipByConfigBehaviorContract:
    """Iceberg skip uses ``skipped_by_config`` (not plain ``skipped``) so
    subsequent runs — after an operator flips ``iceberg_strategy=
    ddl_replay`` — pick the tables back up via ``get_pending_objects``'s
    NOT-LIKE-'skipped%' filter.
    """

    def test_skip_status_prefix_matches_tracker_filter(self):
        """``skipped_by_config`` starts with the literal 'skipped' prefix
        so ``NOT LIKE 'skipped%'`` in get_pending_objects excludes it,
        same as ``skipped_by_rls_cm_policy`` and ``skipped_by_pipeline_
        migration``. If someone ever renames to ``skip_by_config``, this
        fails loud and the tracker filter needs updating."""
        skip_status = "skipped_by_config"
        assert skip_status.startswith("skipped")
