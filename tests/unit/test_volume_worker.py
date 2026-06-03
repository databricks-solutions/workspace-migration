from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMigrateVolume:
    """Tests for the volume_worker.migrate_volume function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        source_spark = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "wh_id": "wh-789",
            "source_spark": source_spark,
            "notebook_uploaded": True,  # avoid the workspace.import_ path in most tests
        }

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_external_volume(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        vol_info = {
            "object_name": "`cat`.`sch`.`ext_vol`",
            "table_type": "EXTERNAL",
            "storage_location": "abfss://container@storage.dfs.core.windows.net/path",
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "volume"
        assert result["error_message"] is None
        called_sql = mock_execute.call_args[0][2]
        assert "CREATE EXTERNAL VOLUME" in called_sql
        assert "IF NOT EXISTS" in called_sql
        assert "LOCATION 'abfss://container@storage.dfs.core.windows.net/path'" in called_sql

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_external_volume_missing_location_fails(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps()
        vol_info = {
            "object_name": "`cat`.`sch`.`ext_vol`",
            "table_type": "EXTERNAL",
            "storage_location": "",  # missing
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "failed"
        assert "storage_location" in result["error_message"]
        mock_execute.assert_not_called()

    @patch("migrate.volume_worker._run_target_volume_copy")
    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_managed_volume_copies_data(self, mock_execute, mock_time, mock_copy):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 130.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}
        mock_copy.return_value = {"bytes_copied": 12345, "file_count": 7}

        deps = self._make_deps()
        deps["source_spark"].sql = MagicMock()

        vol_info = {
            "object_name": "`cat`.`sch`.`mgd_vol`",
            "table_type": "MANAGED",
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "validated"
        assert result["error_message"] is None
        # target-side copy was invoked with the correct share consumer path
        mock_copy.assert_called_once()
        args, kwargs = mock_copy.call_args
        src_path = args[1] if len(args) > 1 else kwargs.get("src_path")
        dst_path = args[2] if len(args) > 2 else kwargs.get("dst_path")
        assert src_path == "/Volumes/cp_migration_share_consumer/sch/mgd_vol"
        assert dst_path == "/Volumes/cat/sch/mgd_vol"
        # Share add + remove SQL were emitted
        sql_calls = [c.args[0] for c in deps["source_spark"].sql.call_args_list]
        assert any("ALTER SHARE cp_migration_share ADD VOLUME" in s for s in sql_calls)
        assert any("ALTER SHARE cp_migration_share REMOVE VOLUME" in s for s in sql_calls)
        # Target CREATE VOLUME (not EXTERNAL)
        called_sqls = [c.args[0] if len(c.args) == 1 else c.args[2] for c in mock_execute.call_args_list]
        assert any("CREATE VOLUME IF NOT EXISTS `cat`.`sch`.`mgd_vol`" in s for s in called_sqls)
        # File count surfaced via source/target_row_count
        assert result["source_row_count"] == 7
        assert result["target_row_count"] == 7

    @patch("migrate.volume_worker._run_target_volume_copy")
    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_managed_volume_removes_from_share_on_copy_failure(self, mock_execute, mock_time, mock_copy):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}
        mock_copy.side_effect = RuntimeError("copy timed out")

        deps = self._make_deps()
        deps["source_spark"].sql = MagicMock()

        vol_info = {"object_name": "`cat`.`sch`.`mgd_vol`", "table_type": "MANAGED"}
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "failed"
        assert "copy timed out" in result["error_message"]
        # REMOVE VOLUME still called
        sql_calls = [c.args[0] for c in deps["source_spark"].sql.call_args_list]
        assert any("REMOVE VOLUME" in s for s in sql_calls)

    @patch("migrate.volume_worker._run_target_volume_copy")
    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_managed_volume_deletes_target_volume_on_copy_failure(
        self, mock_execute, mock_time, mock_copy
    ):
        """Target copy failure must drop the half-populated target volume so
        re-runs don't leak broken state."""
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}
        mock_copy.side_effect = RuntimeError("copy failed mid-run")

        deps = self._make_deps()
        deps["source_spark"].sql = MagicMock()

        vol_info = {"object_name": "`cat`.`sch`.`mgd_vol`", "table_type": "MANAGED"}
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "failed"
        assert "copy failed mid-run" in result["error_message"]
        # The partially-populated target volume must be deleted
        deps["auth"].target_client.volumes.delete.assert_called_once()
        call = deps["auth"].target_client.volumes.delete.call_args
        # Accept either positional or kw `name=` — worker uses kw
        name_arg = call.kwargs.get("name") or (call.args[0] if call.args else None)
        assert name_arg == "cat.sch.mgd_vol"

    @patch("migrate.volume_worker._run_target_volume_copy")
    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_managed_volume_full_rollback_contract_on_copy_failure(
        self, mock_execute, mock_time, mock_copy
    ):
        """Full rollback on target-side copy failure: share removed, target
        volume deleted, failed status recorded."""
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}
        mock_copy.side_effect = RuntimeError("copy boom")

        deps = self._make_deps()
        deps["source_spark"].sql = MagicMock()

        vol_info = {"object_name": "`cat`.`sch`.`mgd_vol`", "table_type": "MANAGED"}
        result, _ = migrate_volume(vol_info, **deps)

        # 1. Status row is "failed" with the copy error surfaced
        assert result["status"] == "failed"
        assert result["object_type"] == "volume"
        assert "copy boom" in result["error_message"]

        # 2. Source volume removed from share
        sql_calls = [c.args[0] for c in deps["source_spark"].sql.call_args_list]
        assert any(
            "ALTER SHARE cp_migration_share REMOVE VOLUME `cat`.`sch`.`mgd_vol`" in s
            for s in sql_calls
        )

        # 3. Partially-populated target volume deleted
        deps["auth"].target_client.volumes.delete.assert_called_once()

    @patch("migrate.volume_worker._run_target_volume_copy")
    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_managed_volume_cleanup_failure_does_not_mask_copy_error(
        self, mock_execute, mock_time, mock_copy
    ):
        """If the best-effort target-volume cleanup itself raises, the original
        copy failure must still be the error recorded on the status row, and
        the share-removal finally block must still run."""
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}
        mock_copy.side_effect = RuntimeError("original copy error")

        deps = self._make_deps()
        deps["source_spark"].sql = MagicMock()
        # volumes.delete itself blows up (e.g. permission / already-gone)
        deps["auth"].target_client.volumes.delete.side_effect = RuntimeError(
            "cleanup also failed"
        )

        vol_info = {"object_name": "`cat`.`sch`.`mgd_vol`", "table_type": "MANAGED"}
        result, _ = migrate_volume(vol_info, **deps)

        # Original copy error wins; cleanup error is swallowed
        assert result["status"] == "failed"
        assert "original copy error" in result["error_message"]
        assert "cleanup also failed" not in result["error_message"]
        # Share removal still happened
        sql_calls = [c.args[0] for c in deps["source_spark"].sql.call_args_list]
        assert any("REMOVE VOLUME" in s for s in sql_calls)

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_dry_run(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(dry_run=True)
        vol_info = {
            "object_name": "`cat`.`sch`.`vol1`",
            "table_type": "MANAGED",
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_failure(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 108.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PERMISSION_DENIED",
            "statement_id": "s-3",
        }

        deps = self._make_deps()
        vol_info = {
            "object_name": "`cat`.`sch`.`vol_fail`",
            "table_type": "EXTERNAL",
            "storage_location": "s3://bucket/path",
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "failed"
        assert "PERMISSION_DENIED" in result["error_message"]


class TestVolumeWorkerSdkTypes:
    """Regression guards for the SDK-compat bugs that failed with
    ``AttributeError: 'str' object has no attribute 'value'`` (for the
    workspace.import_ format argument) and ``AttributeError: 'dict'
    object has no attribute 'as_dict'`` (for jobs.submit tasks). The
    SDK now requires real dataclasses / enums, not raw strings / dicts.
    """

    def test_ensure_copy_notebook_uses_import_format_enum(self):
        """workspace.import_ is called with ``format=ImportFormat.SOURCE``
        (Enum) and ``language=Language.PYTHON``, not raw strings."""
        from databricks.sdk.service.workspace import ImportFormat, Language

        from migrate.volume_worker import _ensure_copy_notebook_on_target

        auth = MagicMock()
        _ensure_copy_notebook_on_target(auth)

        kwargs = auth.target_client.workspace.import_.call_args.kwargs
        assert kwargs["format"] is ImportFormat.SOURCE, (
            f"format argument must be ImportFormat.SOURCE enum, got "
            f"{type(kwargs['format']).__name__}: {kwargs['format']!r}"
        )
        assert kwargs["language"] is Language.PYTHON, (
            f"language argument must be Language.PYTHON enum, got "
            f"{type(kwargs['language']).__name__}: {kwargs['language']!r}"
        )
        assert kwargs["overwrite"] is True

    def test_run_target_volume_copy_uses_sdk_dataclasses(self):
        """jobs.submit receives SubmitTask / NotebookTask / JobEnvironment
        dataclasses — not raw dicts. Raw dicts trigger 'dict has no
        attribute as_dict' inside the SDK."""
        from databricks.sdk.service.jobs import (
            JobEnvironment,
            NotebookTask,
            SubmitTask,
        )

        from migrate.volume_worker import _run_target_volume_copy

        auth = MagicMock()
        run_obj = MagicMock()
        run_obj.state.life_cycle_state = "TERMINATED"
        run_obj.state.result_state = "SUCCESS"
        task = MagicMock(run_id="task-run-1")
        run_obj.tasks = [task]
        auth.target_client.jobs.get_run.return_value = run_obj

        submit_response = MagicMock(run_id="run-1")
        auth.target_client.jobs.submit.return_value = submit_response

        out = MagicMock()
        out.notebook_output.result = '{"bytes_copied": 0, "file_count": 0}'
        auth.target_client.jobs.get_run_output.return_value = out

        _run_target_volume_copy(auth, "/src/x", "/dst/x", "run-name")

        call_kwargs = auth.target_client.jobs.submit.call_args.kwargs
        tasks_arg = call_kwargs["tasks"]
        envs_arg = call_kwargs["environments"]

        assert len(tasks_arg) == 1
        assert isinstance(tasks_arg[0], SubmitTask), (
            f"tasks[0] must be a SubmitTask dataclass, got {type(tasks_arg[0]).__name__}"
        )
        assert isinstance(tasks_arg[0].notebook_task, NotebookTask), (
            f"notebook_task must be NotebookTask, got {type(tasks_arg[0].notebook_task).__name__}"
        )
        assert tasks_arg[0].notebook_task.base_parameters == {
            "src": "/src/x",
            "dst": "/dst/x",
        }

        assert len(envs_arg) == 1
        assert isinstance(envs_arg[0], JobEnvironment), (
            f"environments[0] must be a JobEnvironment, got {type(envs_arg[0]).__name__}"
        )


class TestExternalVolumeCreationContract:
    """External volume migration creates the volume on target at the same
    storage location. The ACCESS path (target access connector permissions
    on that LOCATION) is an infra-level concern — terraform configures
    target external locations; the tool just CREATEs the volume pointer.

    These tests lock in the contract so a future refactor can't silently
    drop the IF NOT EXISTS clause or break the LOCATION clause shape.
    """

    def _make_deps(self, **overrides):
        deps = {
            "config": MagicMock(dry_run=False),
            "auth": MagicMock(),
            "tracker": MagicMock(),
            "wh_id": "wh-v",
            "source_spark": MagicMock(),
            "notebook_uploaded": False,
        }
        deps.update(overrides)
        return deps

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_create_uses_if_not_exists(self, mock_execute, mock_time):
        """Idempotency: a re-run must not fail because the volume
        already exists on target."""
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-idem"}

        deps = self._make_deps()
        vol = {
            "object_name": "`c`.`s`.`v`",
            "table_type": "EXTERNAL",
            "storage_location": "abfss://x@y.dfs.core.windows.net/v",
        }
        migrate_volume(vol, **deps)
        sql = mock_execute.call_args[0][2]
        assert "IF NOT EXISTS" in sql

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_create_preserves_exact_location(self, mock_execute, mock_time):
        """The LOCATION passed to CREATE EXTERNAL VOLUME on target must be
        byte-identical to the source's storage_location. Silent quoting /
        escaping bugs here would point the volume at the wrong path."""
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-loc"}

        source_loc = "abfss://data@acct123.dfs.core.windows.net/path/with spaces/"
        deps = self._make_deps()
        vol = {
            "object_name": "`c`.`s`.`spaced`",
            "table_type": "EXTERNAL",
            "storage_location": source_loc,
        }
        migrate_volume(vol, **deps)
        sql = mock_execute.call_args[0][2]
        assert f"LOCATION '{source_loc}'" in sql

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_target_creation_failure_propagates(self, mock_execute, mock_time):
        """If CREATE EXTERNAL VOLUME on target fails (e.g. access
        connector not granted on the external location), the worker
        records status=failed with the SQL error message surfaced so
        operators can diagnose. This is the real-world PERMISSION_DENIED
        path when target infra isn't wired up."""
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PERMISSION_DENIED: External location 'x' not found",
            "statement_id": "s-denied",
        }

        deps = self._make_deps()
        vol = {
            "object_name": "`c`.`s`.`v`",
            "table_type": "EXTERNAL",
            "storage_location": "abfss://nope@nope.dfs.core.windows.net/",
        }
        result, _ = migrate_volume(vol, **deps)
        assert result["status"] == "failed"
        assert "PERMISSION_DENIED" in result["error_message"]
