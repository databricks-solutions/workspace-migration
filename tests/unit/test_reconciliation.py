"""Unit tests for :mod:`migrate.reconciliation` (X.1).

Covers:
- ``reconcile_stale_runs``: orphan detection, status decisions, cleanup
  hook dispatch, current_job_run_id guard, dry_run behaviour.
- ``maybe_kill``: no-op when unset, refusal outside test mode, SystemExit
  when tripped.
- ``resolve_current_job_run_id``: safe fallbacks (None on failure).
- Per-worker cleanup hooks (``volume_worker.cleanup_partial_target``,
  ``sharing_worker.cleanup_partial_share``): successful deletes,
  NOT_FOUND tolerance, unrelated errors re-raised.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _mock_status_row(object_name, object_type, status, job_run_id=None, migrated_at=None):
    m = MagicMock()
    m.object_name = object_name
    m.object_type = object_type
    m.status = status
    m.job_run_id = job_run_id
    m.migrated_at = migrated_at or "2026-04-23T00:00:00Z"
    return m


def _spark_returning(rows):
    spark = MagicMock()
    result = MagicMock()
    result.collect.return_value = rows
    spark.sql.return_value = result
    return spark


# -----------------------------------------------------------------------------
# reconcile_stale_runs
# -----------------------------------------------------------------------------


class TestReconcileStaleRuns:
    """Core reconciliation decision table coverage."""

    def test_empty_status_table_noop(self, mock_config):
        from migrate.reconciliation import reconcile_stale_runs

        spark = _spark_returning([])
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="run-1"
        )
        assert out["reset_count"] == 0
        assert out["validation_failed_count"] == 0
        assert out["cleanup_count"] == 0
        assert out["decisions"] == []
        tracker.append_migration_status.assert_not_called()

    def test_missing_status_table_is_warning_not_raise(self, mock_config):
        """Fresh install: migration_status may not exist yet. Reconciliation
        must degrade to no-op rather than fail the whole migrate."""
        from migrate.reconciliation import reconcile_stale_runs

        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id=None
        )
        assert out["reset_count"] == 0
        tracker.append_migration_status.assert_not_called()

    def test_validated_row_is_noop(self, mock_config):
        from migrate.reconciliation import reconcile_stale_runs

        spark = _spark_returning([_mock_status_row("cat.sch.t", "managed_table", "validated", "old-run")])
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="new-run"
        )
        assert out["reset_count"] == 0
        tracker.append_migration_status.assert_not_called()

    def test_skipped_terminal_is_noop(self, mock_config):
        """skipped_by_pipeline_migration / skipped_target_exists /
        skipped_by_stateful_service_migration are terminal statuses; the
        reconciler must leave them alone. The last status covers
        streaming tables hard-excluded from the core tool (migrated by
        the future Stateful Services Phase)."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [
            _mock_status_row("cat.sch.mv1", "mv", "skipped_by_pipeline_migration", "old-run"),
            _mock_status_row("cat.sch.t2", "managed_table", "skipped_target_exists", "old-run"),
            _mock_status_row("cat.sch.st1", "st", "skipped_by_stateful_service_migration", "old-run"),
        ]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="new-run"
        )
        assert out["reset_count"] == 0
        tracker.append_migration_status.assert_not_called()

    def test_failed_row_is_noop(self, mock_config):
        """``failed`` rows are already re-picked-up by get_pending_objects;
        the reconciler does not rewrite them."""
        from migrate.reconciliation import reconcile_stale_runs

        spark = _spark_returning([_mock_status_row("cat.sch.t", "managed_table", "failed", "old-run")])
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="new-run"
        )
        assert out["reset_count"] == 0
        tracker.append_migration_status.assert_not_called()

    def test_validation_failed_surfaced_not_reset(self, mock_config):
        """validation_failed needs operator attention; surface the count
        but do not reset the row."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("cat.sch.t", "managed_table", "validation_failed", "old-run")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="new-run"
        )
        assert out["reset_count"] == 0
        assert out["validation_failed_count"] == 1
        assert out["decisions"][0]["action"] == "surface"
        tracker.append_migration_status.assert_not_called()

    def test_in_progress_from_prior_run_resets(self, mock_config):
        """The core case: orphaned in_progress from a prior job run is
        reset to pending via a new append."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("cat.sch.t", "managed_table", "in_progress", "old-run")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="new-run"
        )
        assert out["reset_count"] == 1
        tracker.append_migration_status.assert_called_once()
        appended = tracker.append_migration_status.call_args[0][0]
        assert len(appended) == 1
        assert appended[0]["status"] == "pending"
        assert appended[0]["object_name"] == "cat.sch.t"
        assert appended[0]["object_type"] == "managed_table"
        assert appended[0]["job_run_id"] == "new-run"
        assert "Reconciled orphaned in_progress" in (appended[0]["error_message"] or "")

    def test_in_progress_from_current_run_is_noop(self, mock_config):
        """If the in_progress row belongs to this very run (rare — worker
        processing in parallel with reconciliation), do not self-reset."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("cat.sch.t", "managed_table", "in_progress", "run-current")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="run-current"
        )
        assert out["reset_count"] == 0
        tracker.append_migration_status.assert_not_called()

    def test_in_progress_null_job_run_id_resets(self, mock_config):
        """Null job_run_id means we cannot tell which run owned the row.
        Reconciler treats it as orphaned (safe direction: retry vs. hang)."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("cat.sch.t", "managed_table", "in_progress", None)]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="new-run"
        )
        assert out["reset_count"] == 1

    def test_dry_run_does_not_append(self, mock_config):
        """dry_run: log the intent but do not mutate the tracker."""
        from migrate.reconciliation import reconcile_stale_runs

        mock_config.dry_run = True
        rows = [_mock_status_row("cat.sch.t", "managed_table", "in_progress", "old-run")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="new-run"
        )
        assert out["reset_count"] == 1
        tracker.append_migration_status.assert_not_called()
        mock_config.dry_run = False

    def test_cleanup_hook_dispatched_for_volume(self, mock_config):
        """A volume in_progress row triggers cleanup_partial_target."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("`cat`.`sch`.`v`", "volume", "in_progress", "old-run")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        auth = MagicMock()

        # Patch the volume cleanup so we can observe the call without actually
        # invoking SDK.
        with patch("migrate.volume_worker.cleanup_partial_target") as cleanup:
            out = reconcile_stale_runs(
                spark=spark,
                config=mock_config,
                tracker=tracker,
                auth=auth,
                current_job_run_id="new-run",
            )
            cleanup.assert_called_once()
            assert cleanup.call_args.args[0] == "`cat`.`sch`.`v`"
        assert out["cleanup_count"] == 1
        assert out["reset_count"] == 1

    def test_cleanup_hook_error_does_not_block_reset(self, mock_config):
        """If cleanup fails, the reset row is still appended — X.2 makes
        retry safe even without successful cleanup."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("`cat`.`sch`.`v`", "volume", "in_progress", "old-run")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        auth = MagicMock()

        with patch("migrate.volume_worker.cleanup_partial_target", side_effect=RuntimeError("boom")):
            out = reconcile_stale_runs(
                spark=spark,
                config=mock_config,
                tracker=tracker,
                auth=auth,
                current_job_run_id="new-run",
            )
        assert out["reset_count"] == 1
        tracker.append_migration_status.assert_called_once()
        # cleanup_count still records the attempt (success or failure).
        assert out["cleanup_count"] == 1

    def test_cleanup_skipped_without_auth(self, mock_config):
        """Unit-test convenience: if auth is None, skip the cleanup hook
        but still reset the row. Used by tests that do not need per-worker
        cleanup wiring."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("`cat`.`sch`.`v`", "volume", "in_progress", "old-run")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark, config=mock_config, tracker=tracker, auth=None, current_job_run_id="new-run"
        )
        assert out["reset_count"] == 1
        assert out["cleanup_count"] == 0

    def test_cleanup_hook_dispatched_for_registered_model(self, mock_config):
        """C5: a registered_model in_progress row triggers
        models_worker.cleanup_partial_target so partial multi-version
        state is dropped before the row is reset."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("MODEL_c.s.m", "registered_model", "in_progress", "old-run")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        auth = MagicMock()

        with patch("migrate.models_worker.cleanup_partial_target") as cleanup:
            out = reconcile_stale_runs(
                spark=spark,
                config=mock_config,
                tracker=tracker,
                auth=auth,
                current_job_run_id="new-run",
            )
            cleanup.assert_called_once()
            assert cleanup.call_args.args[0] == "MODEL_c.s.m"
        assert out["cleanup_count"] == 1
        assert out["reset_count"] == 1

    def test_no_hook_for_unrelated_worker(self, mock_config):
        """managed_table has no cleanup hook (DEEP CLONE is CREATE OR
        REPLACE). Reset should still happen but cleanup_count stays 0."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [_mock_status_row("cat.sch.t", "managed_table", "in_progress", "old-run")]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        auth = MagicMock()
        out = reconcile_stale_runs(
            spark=spark,
            config=mock_config,
            tracker=tracker,
            auth=auth,
            current_job_run_id="new-run",
        )
        assert out["reset_count"] == 1
        assert out["cleanup_count"] == 0

    def test_mixed_rows(self, mock_config):
        """Every branch of the decision table in one invocation."""
        from migrate.reconciliation import reconcile_stale_runs

        rows = [
            _mock_status_row("cat.sch.ok", "managed_table", "validated", "old-run"),
            _mock_status_row("cat.sch.orphan", "managed_table", "in_progress", "old-run"),
            _mock_status_row("cat.sch.failed", "managed_table", "failed", "old-run"),
            _mock_status_row("cat.sch.val_failed", "managed_table", "validation_failed", "old-run"),
            _mock_status_row("cat.sch.skip_target", "managed_table", "skipped_target_exists", "old-run"),
        ]
        spark = _spark_returning(rows)
        tracker = MagicMock()
        out = reconcile_stale_runs(
            spark=spark,
            config=mock_config,
            tracker=tracker,
            auth=None,
            current_job_run_id="new-run",
        )
        assert out["reset_count"] == 1
        assert out["validation_failed_count"] == 1


# -----------------------------------------------------------------------------
# maybe_kill (test-only)
# -----------------------------------------------------------------------------


class TestMaybeKill:
    def _cfg(self, kill_after):
        """Return a lightweight object exposing test_kill_after — cannot
        use MagicMock because the int isinstance guard skips mocks."""

        class _C:
            pass

        c = _C()
        c.test_kill_after = kill_after
        return c

    def test_unset_is_noop(self):
        from migrate.reconciliation import maybe_kill

        maybe_kill(self._cfg(None), counter=5, worker_name="managed_table_worker")  # no raise

    def test_zero_is_noop(self):
        from migrate.reconciliation import maybe_kill

        maybe_kill(self._cfg(0), counter=5, worker_name="managed_table_worker")

    def test_set_in_non_test_mode_raises(self, monkeypatch):
        """Safety guard: the flag must be refused outside a test profile."""
        from migrate.reconciliation import maybe_kill

        monkeypatch.delenv("WSM_TEST_MODE", raising=False)
        monkeypatch.delenv("DATABRICKS_ENVIRONMENT", raising=False)
        with pytest.raises(RuntimeError, match="not a test profile"):
            maybe_kill(self._cfg(2), counter=1, worker_name="mt")

    def test_below_counter_is_noop_in_test_mode(self, monkeypatch):
        from migrate.reconciliation import maybe_kill

        monkeypatch.setenv("WSM_TEST_MODE", "1")
        maybe_kill(self._cfg(5), counter=3, worker_name="mt")

    def test_at_counter_raises_systemexit(self, monkeypatch):
        from migrate.reconciliation import maybe_kill

        monkeypatch.setenv("WSM_TEST_MODE", "1")
        with pytest.raises(SystemExit, match="test_kill_after tripped"):
            maybe_kill(self._cfg(2), counter=2, worker_name="mt")

    def test_past_counter_raises(self, monkeypatch):
        from migrate.reconciliation import maybe_kill

        monkeypatch.setenv("WSM_TEST_MODE", "1")
        with pytest.raises(SystemExit):
            maybe_kill(self._cfg(2), counter=5, worker_name="mt")

    def test_databricks_environment_test_prefix_enables(self, monkeypatch):
        from migrate.reconciliation import maybe_kill

        monkeypatch.delenv("WSM_TEST_MODE", raising=False)
        monkeypatch.setenv("DATABRICKS_ENVIRONMENT", "test-uksouth")
        with pytest.raises(SystemExit):
            maybe_kill(self._cfg(1), counter=1, worker_name="mt")

    def test_magicmock_config_is_silently_ignored(self):
        """Unit tests passing MagicMock() as config must not trip the
        safety check because they arent asking to kill."""
        from migrate.reconciliation import maybe_kill

        maybe_kill(MagicMock(), counter=5, worker_name="mt")

    def test_bool_config_ignored(self):
        """bool is a subclass of int — we explicitly reject it so a
        stray ``test_kill_after: true`` in config.yaml does not kill."""
        from migrate.reconciliation import maybe_kill

        class _C:
            pass

        c = _C()
        c.test_kill_after = True  # noqa: FBT003 — deliberate misuse
        maybe_kill(c, counter=1, worker_name="mt")


# -----------------------------------------------------------------------------
# resolve_current_job_run_id
# -----------------------------------------------------------------------------


class TestResolveCurrentJobRunId:
    def test_none_dbutils(self):
        from migrate.reconciliation import resolve_current_job_run_id

        assert resolve_current_job_run_id(None) is None

    def test_getcontext_raises_returns_none(self):
        from migrate.reconciliation import resolve_current_job_run_id

        dbutils = MagicMock()
        dbutils.notebook.entry_point.getDbutils.side_effect = RuntimeError("no jobs ctx")
        assert resolve_current_job_run_id(dbutils) is None

    def test_currentrunid_getter_returns_string(self):
        from migrate.reconciliation import resolve_current_job_run_id

        dbutils = MagicMock()
        ctx = dbutils.notebook.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value
        # Remove all attrs, set only currentRunId
        import contextlib

        for attr in ("jobRunId", "tags"):
            if hasattr(ctx, attr):
                with contextlib.suppress(AttributeError):
                    delattr(ctx, attr)
        # Scala Option-like: val.get() returns the string
        option = MagicMock()
        option.get.return_value = "12345"
        ctx.currentRunId.return_value = option
        out = resolve_current_job_run_id(dbutils)
        assert out == "12345"


# -----------------------------------------------------------------------------
# Per-worker cleanup hooks
# -----------------------------------------------------------------------------


class TestVolumeCleanupPartialTarget:
    def test_successful_drop(self):
        from migrate.volume_worker import cleanup_partial_target

        auth = MagicMock()
        cleanup_partial_target("`cat`.`sch`.`v`", auth=auth, spark=None, config=None)
        auth.target_client.volumes.delete.assert_called_once_with(name="cat.sch.v")

    def test_not_found_is_tolerated(self):
        from migrate.volume_worker import cleanup_partial_target

        auth = MagicMock()
        auth.target_client.volumes.delete.side_effect = RuntimeError("VOLUME_DOES_NOT_EXIST")
        # Must not raise
        cleanup_partial_target("`cat`.`sch`.`v`", auth=auth, spark=None, config=None)

    def test_other_error_reraised(self):
        from migrate.volume_worker import cleanup_partial_target

        auth = MagicMock()
        auth.target_client.volumes.delete.side_effect = RuntimeError("PERMISSION_DENIED")
        with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
            cleanup_partial_target("`cat`.`sch`.`v`", auth=auth, spark=None, config=None)


class TestModelsCleanupPartialTarget:
    """C5: registered_model needs a cleanup hook so a crash mid multi-version
    copy doesn't leave the next run wedged on partial state."""

    def test_strips_model_prefix(self):
        """object_name comes through as ``MODEL_<fqn>``; the SDK call must
        receive the bare ``catalog.schema.name`` form."""
        from migrate.models_worker import cleanup_partial_target

        auth = MagicMock()
        cleanup_partial_target("MODEL_c.s.m", auth=auth, spark=None, config=None)
        auth.target_client.registered_models.delete.assert_called_once_with(full_name="c.s.m")

    def test_strips_backticks(self):
        from migrate.models_worker import cleanup_partial_target

        auth = MagicMock()
        cleanup_partial_target("MODEL_`c`.`s`.`m`", auth=auth, spark=None, config=None)
        auth.target_client.registered_models.delete.assert_called_once_with(full_name="c.s.m")

    def test_plain_fqn_accepted(self):
        """If called with no ``MODEL_`` prefix, treat as bare fqn."""
        from migrate.models_worker import cleanup_partial_target

        auth = MagicMock()
        cleanup_partial_target("c.s.m", auth=auth, spark=None, config=None)
        auth.target_client.registered_models.delete.assert_called_once_with(full_name="c.s.m")

    def test_not_found_tolerated(self):
        from migrate.models_worker import cleanup_partial_target

        auth = MagicMock()
        auth.target_client.registered_models.delete.side_effect = RuntimeError(
            "RESOURCE_DOES_NOT_EXIST"
        )
        # Must not raise — crash before CREATE landed leaves nothing to drop.
        cleanup_partial_target("MODEL_c.s.m", auth=auth, spark=None, config=None)

    def test_other_error_reraised(self):
        from migrate.models_worker import cleanup_partial_target

        auth = MagicMock()
        auth.target_client.registered_models.delete.side_effect = RuntimeError(
            "PERMISSION_DENIED"
        )
        with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
            cleanup_partial_target("MODEL_c.s.m", auth=auth, spark=None, config=None)


class TestSharingCleanupPartialShare:
    def test_strips_share_prefix(self):
        from migrate.sharing_worker import cleanup_partial_share

        auth = MagicMock()
        cleanup_partial_share("SHARE_retail_share", auth=auth, spark=None, config=None)
        auth.target_client.shares.delete.assert_called_once_with(name="retail_share")

    def test_plain_share_name_accepted(self):
        """If called with a bare share name (no SHARE_ prefix), treat as-is."""
        from migrate.sharing_worker import cleanup_partial_share

        auth = MagicMock()
        cleanup_partial_share("retail_share", auth=auth, spark=None, config=None)
        auth.target_client.shares.delete.assert_called_once_with(name="retail_share")

    def test_not_found_tolerated(self):
        from migrate.sharing_worker import cleanup_partial_share

        auth = MagicMock()
        auth.target_client.shares.delete.side_effect = RuntimeError("RESOURCE_NOT_FOUND")
        cleanup_partial_share("SHARE_missing", auth=auth, spark=None, config=None)

    def test_other_error_reraised(self):
        from migrate.sharing_worker import cleanup_partial_share

        auth = MagicMock()
        auth.target_client.shares.delete.side_effect = RuntimeError("PERMISSION_DENIED")
        with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
            cleanup_partial_share("SHARE_x", auth=auth, spark=None, config=None)


# -----------------------------------------------------------------------------
# Kill-injection counter reuse
# -----------------------------------------------------------------------------


class TestManagedTableKillCounter:
    """The thread-safe module counter in managed_table_worker is the site
    used by the integration fixture. Pin its core behaviour so a future
    refactor does not silently drop the counter semantics."""

    def test_counter_increments(self):
        from migrate.managed_table_worker import _bump_kill_counter, _reset_kill_counter

        _reset_kill_counter()
        assert _bump_kill_counter() == 1
        assert _bump_kill_counter() == 2
        assert _bump_kill_counter() == 3
        _reset_kill_counter()

    def test_counter_reset(self):
        from migrate.managed_table_worker import _bump_kill_counter, _reset_kill_counter

        _reset_kill_counter()
        _bump_kill_counter()
        _bump_kill_counter()
        _reset_kill_counter()
        assert _bump_kill_counter() == 1
        _reset_kill_counter()


# -----------------------------------------------------------------------------
# Config: test_kill_after parsing
# -----------------------------------------------------------------------------


class TestTestKillAfterConfig:
    """Config-load validation of the test_kill_after field."""

    def _write(self, tmp_path, extra: str = ""):
        p = tmp_path / "config.yaml"
        body = f"""
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: scope
spn_secret_key: key
{extra}
"""
        p.write_text(body)
        return p

    def test_default_is_none(self, tmp_path):
        from common.config import MigrationConfig

        cfg = MigrationConfig.from_workspace_file(path=str(self._write(tmp_path)))
        assert cfg.test_kill_after is None

    def test_zero_coerces_to_none(self, tmp_path):
        from common.config import MigrationConfig

        cfg = MigrationConfig.from_workspace_file(
            path=str(self._write(tmp_path, "test_kill_after: 0\n"))
        )
        assert cfg.test_kill_after is None

    def test_positive_value_accepted(self, tmp_path):
        from common.config import MigrationConfig

        cfg = MigrationConfig.from_workspace_file(
            path=str(self._write(tmp_path, "test_kill_after: 3\n"))
        )
        assert cfg.test_kill_after == 3

    def test_negative_rejected(self, tmp_path):
        from common.config import MigrationConfig

        with pytest.raises(ValueError, match="non-negative"):
            MigrationConfig.from_workspace_file(
                path=str(self._write(tmp_path, "test_kill_after: -1\n"))
            )

    def test_non_int_rejected(self, tmp_path):
        from common.config import MigrationConfig

        with pytest.raises(ValueError, match="non-negative integer"):
            MigrationConfig.from_workspace_file(
                path=str(self._write(tmp_path, "test_kill_after: abc\n"))
            )


# -----------------------------------------------------------------------------
# Orchestrator wiring (reconciliation is invoked with live config)
# -----------------------------------------------------------------------------


class TestOrchestratorReconcileImport:
    """The orchestrator module imports reconcile_stale_runs — pin that the
    symbol is wired and the import itself succeeds."""

    def test_orchestrator_imports_reconciliation(self):
        import migrate.orchestrator as orch

        assert hasattr(orch, "reconcile_stale_runs")
        assert hasattr(orch, "resolve_current_job_run_id")
