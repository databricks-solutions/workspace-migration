"""Unit tests for the Hive workers — views, functions, external tables,
managed non-DBFS, managed DBFS-root, grants.

These workers had zero unit coverage before this file. Focus is on the
contracts: DDL rewrites target the correct catalog, LOCATION / partition
clauses survive, OWN grants are skipped, etc.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _config_mock(**overrides):
    cfg = MagicMock()
    cfg.dry_run = False
    cfg.hive_target_catalog = "hive_upgraded"
    cfg.hive_dbfs_target_path = "abfss://x@y.dfs.core.windows.net/hive_data"
    cfg.spn_client_id = "test-spn"
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


# ----------------------------------------------------------------------
# hive_views_worker
# ----------------------------------------------------------------------


class TestHiveViewsWorker:
    """hive_views_worker replays Hive view DDL into hive_metastore
    unchanged (like-for-like) and swaps CREATE VIEW → CREATE OR REPLACE
    VIEW. We hit a real TABLE_OR_VIEW_NOT_FOUND bug here earlier; these
    tests guard the replay contract."""

    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_replays_view_ddl_into_hive_metastore_unchanged(self, mock_execute, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        ddl = (
            "CREATE OR REPLACE VIEW `hive_metastore`.`integration_test_hive`.`big_orders` AS "
            "SELECT * FROM hive_metastore.integration_test_hive.managed_orders WHERE amount > 15"
        )
        cfg = _config_mock()
        migrate_hive_view(
            {"object_name": "`hive_metastore`.`integration_test_hive`.`big_orders`"},
            ddl, config=cfg, auth=MagicMock(), wh_id="wh-hv",
        )
        replayed = mock_execute.call_args[0][2]
        assert "hive_upgraded" not in replayed
        assert "hive_metastore.integration_test_hive.managed_orders" in replayed
        assert replayed.startswith("CREATE OR REPLACE VIEW")

    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_dry_run_skips_execution(self, mock_execute, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 100.1]
        cfg = _config_mock(dry_run=True)
        result = migrate_hive_view(
            {"object_name": "`hive_metastore`.`s`.`v`"},
            "CREATE VIEW hive_metastore.s.v AS SELECT 1",
            config=cfg,
            auth=MagicMock(),
            wh_id="wh",
        )
        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_failed_target_sql_marks_view_failed(self, mock_execute, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 100.5]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "TABLE_OR_VIEW_NOT_FOUND",
            "statement_id": "s",
        }
        cfg = _config_mock()
        result = migrate_hive_view(
            {"object_name": "`hive_metastore`.`s`.`v`"},
            "CREATE VIEW hive_metastore.s.v AS SELECT * FROM hive_metastore.s.missing",
            config=cfg,
            auth=MagicMock(),
            wh_id="wh",
        )
        assert result["status"] == "failed"
        assert "TABLE_OR_VIEW_NOT_FOUND" in result["error_message"]


# ----------------------------------------------------------------------
# hive_external_worker — LOCATION preservation contract
# ----------------------------------------------------------------------


class TestHiveViewsWorkerSourceGuards:
    """Source-level guards for hive_views_worker like-for-like behavior.

    The run() function builds the target view FQN as hive_metastore (no catalog
    rewrite); this contract has no unit coverage (run() requires Spark). These
    source-text guards ensure the FQN construction remains identity-preserving
    and rejects any hive_target_catalog rewrite reintroduction."""

    def test_builds_view_fqn_in_hive_metastore(self):
        """The view FQN constructor explicitly targets hive_metastore, not any
        other catalog. Line ~195 in run() builds target_fqn with the backticked
        hive_metastore header."""
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_views_worker.py"
        ).read_text()
        # Assert the backticked hive_metastore pattern is in the FQN builder.
        # This pins the identity construction in run() and guards against
        # migration to any other catalog.
        assert "`hive_metastore`.`" in src, (
            "hive_views_worker must build the target view FQN with `hive_metastore` "
            "(like-for-like identity migration). The pattern `hive_metastore`.` "
            "must appear in the target_fqn construction."
        )

    def test_no_catalog_rewrite_to_hive_target_catalog(self):
        """Guard against reintroduction of catalog-rewrite logic.

        Like-for-like Hive views migrate to hive_metastore, never to
        hive_target_catalog or any other UC catalog. If this assertion fails,
        someone has reintroduced the catalog-rewrite path (which would break
        the contract for views that have intra-view dependencies)."""
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_views_worker.py"
        ).read_text()
        assert "hive_target_catalog" not in src, (
            "hive_views_worker must NOT reference hive_target_catalog — views "
            "migrate unchanged to hive_metastore (like-for-like). Any reference to "
            "hive_target_catalog indicates a catalog-rewrite path has been added, "
            "which breaks the identity contract."
        )


class TestHiveExternalWorker:
    """Like-for-like: the external table is recreated in hive_metastore with
    the SAME FQN and the replayed DDL keeps its hive_metastore namespace."""

    @patch("migrate.hive_external_worker.append_migration_status_via_warehouse")
    @patch("migrate.hive_external_worker.warehouse_table_count")
    @patch("migrate.hive_external_worker.time")
    @patch("migrate.hive_external_worker.execute_and_poll")
    def test_replays_ddl_into_hive_metastore_unchanged(
        self, mock_exec, mock_time, mock_wh_count, mock_append
    ):
        from migrate.hive_external_worker import migrate_hive_external_table

        mock_time.time.side_effect = [100.0, 105.0]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_wh_count.return_value = 7

        explorer = MagicMock()
        explorer.get_create_statement.return_value = (
            "CREATE TABLE hive_metastore.db.ext (id INT) USING delta "
            "LOCATION 'abfss://ext@acct.dfs.core.windows.net/ext'"
        )
        explorer.get_table_row_count.return_value = 7

        res = migrate_hive_external_table(
            {"object_name": "`hive_metastore`.`db`.`ext`"},
            config=_config_mock(),
            auth=MagicMock(),
            explorer=explorer,
            wh_id="wh",
            tracking_fqn="migration_tracking.cp_migration",
            job_run_id="jr-1",
            status_wh_id="wh-src",
        )

        replayed = mock_exec.call_args[0][2]
        # No namespace rewrite: hive_metastore stays, no hive_upgraded leak.
        assert "hive_metastore.db.ext" in replayed
        assert "hive_upgraded" not in replayed
        # IF NOT EXISTS still injected for resumability.
        assert "CREATE TABLE IF NOT EXISTS" in replayed
        assert res["status"] == "validated"


# ----------------------------------------------------------------------
# hive_functions_worker
# ----------------------------------------------------------------------


class TestHiveFunctionsWorker:
    def test_module_imports_cleanly(self):
        from migrate import hive_functions_worker

        assert hasattr(hive_functions_worker, "run")

    @patch("migrate.hive_functions_worker.get_hive_function_ddl")
    @patch("migrate.hive_functions_worker.time")
    @patch("migrate.hive_functions_worker.execute_and_poll")
    def test_replays_function_ddl_into_hive_metastore_unchanged(
        self, mock_execute, mock_time, mock_ddl
    ):
        from migrate.hive_functions_worker import migrate_hive_function

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_ddl.return_value = (
            "CREATE FUNCTION hive_metastore.db.triple(x DOUBLE) RETURNS DOUBLE RETURN x * 3"
        )

        res = migrate_hive_function(
            {"object_name": "`hive_metastore`.`db`.`triple`"},
            config=_config_mock(), auth=MagicMock(), tracker=MagicMock(),
            spark=MagicMock(), wh_id="wh",
        )
        replayed = mock_execute.call_args[0][2]
        assert "hive_metastore.db.triple" in replayed
        assert "hive_upgraded" not in replayed
        assert replayed.startswith("CREATE OR REPLACE FUNCTION")
        assert res["status"] == "validated"


# ----------------------------------------------------------------------
# hive_grants_worker — Hive→UC privilege translation + TABLE gap
# ----------------------------------------------------------------------


class TestHiveGrantsWorker:
    def test_source_uses_hive_to_uc_privileges_map(self):
        """hive_grants_worker must consume the documented map from
        hive_common.py. If someone forks the map inline, this test
        fails loud so the change is visible."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_grants_worker.py").read_text()
        assert "HIVE_TO_UC_PRIVILEGES" in src

    def test_object_type_map_covers_all_hive_table_categories(self):
        """_OBJECT_TYPE_TO_SECURABLE maps every discovery object_type
        a Hive migration produces. Missing entries mean the worker
        falls through with "Skipping unknown object_type" and drops
        grants for that table category — was the original gap."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_grants_worker.py").read_text()
        # Every hive category discovery emits must be in the map:
        for hive_type in (
            "hive_external",
            "hive_managed_dbfs_root",
            "hive_managed_nondbfs",
            "hive_view",
            "hive_function",
        ):
            assert f'"{hive_type}"' in src, (
                f"hive_grants_worker._OBJECT_TYPE_TO_SECURABLE is missing "
                f"{hive_type!r} — grants on those objects will be silently "
                f"dropped with 'Skipping unknown object_type'."
            )

    def test_skip_decision_when_already_owned(self):
        from migrate.hive_grants_worker import _should_skip_owner_transfer

        assert _should_skip_owner_transfer("alice@corp.com", "alice@corp.com") is True
        assert _should_skip_owner_transfer("ALICE@corp.com", "alice@corp.com") is True
        assert _should_skip_owner_transfer("bob@corp.com", "alice@corp.com") is False
        assert _should_skip_owner_transfer(None, "alice@corp.com") is False

    @patch("migrate.hive_grants_worker._current_owner")
    @patch("migrate.hive_grants_worker.time")
    @patch("migrate.hive_grants_worker.execute_and_poll")
    def test_schema_own_grants_before_transfer(self, mock_exec, mock_time, mock_owner):
        """SCHEMA OWN must GRANT USAGE, CREATE to the SPN BEFORE ALTER OWNER."""
        from migrate.hive_grants_worker import _emit_grant

        mock_time.time.side_effect = [100.0, 100.1, 100.2, 100.3]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_owner.return_value = "someoneelse@corp.com"  # not yet owned by target

        _emit_grant(
            action_type="OWN", securable_keyword="SCHEMA",
            target_fqn="`hive_metastore`.`db`", principal="alice@corp.com",
            auth=MagicMock(), wh_id="wh", dry_run=False,
            transfer_ownership=True, spn_client_id="spn-123",
        )
        executed = [c.args[2] for c in mock_exec.call_args_list]
        grant_idx = next(i for i, s in enumerate(executed) if s.startswith("GRANT USAGE, CREATE ON SCHEMA"))
        owner_idx = next(i for i, s in enumerate(executed) if s.startswith("ALTER SCHEMA"))
        assert "`hive_metastore`.`db`" in executed[grant_idx]
        assert "`spn-123`" in executed[grant_idx]
        assert grant_idx < owner_idx, "GRANT USAGE, CREATE must precede ALTER OWNER"

    @patch("migrate.hive_grants_worker._current_owner")
    @patch("migrate.hive_grants_worker.time")
    @patch("migrate.hive_grants_worker.execute_and_poll")
    def test_owner_transfer_skipped_when_already_owned(self, mock_exec, mock_time, mock_owner):
        from migrate.hive_grants_worker import _emit_grant

        mock_time.time.side_effect = [100.0, 100.1]
        mock_owner.return_value = "alice@corp.com"  # target already owns
        res = _emit_grant(
            action_type="OWN", securable_keyword="SCHEMA",
            target_fqn="`hive_metastore`.`db`", principal="alice@corp.com",
            auth=MagicMock(), wh_id="wh", dry_run=False,
            transfer_ownership=True, spn_client_id="spn-123",
        )
        assert res["status"] == "skipped"
        assert "already owned" in res["error_message"].lower()
        assert not any(c.args[2].startswith("ALTER SCHEMA") for c in mock_exec.call_args_list)

    def test_run_skips_own_at_catalog_level(self):
        """The built-in hive_metastore catalog ownership is never transferred."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_grants_worker.py").read_text()
        # No hive_target_catalog anywhere; catalog branch does not transfer OWN.
        assert "hive_target_catalog" not in src
        assert "hive_metastore catalog ownership not transferred" in src

    def test_grant_target_skip_predicate(self):
        from migrate.hive_grants_worker import _grant_target_not_migrated

        not_migrated = {"`hive_metastore`.`db`.`dbfs_orders`"}
        assert _grant_target_not_migrated("`hive_metastore`.`db`.`dbfs_orders`", not_migrated) is True
        assert _grant_target_not_migrated("`hive_metastore`.`db`.`good`", not_migrated) is False

    def test_skipped_grant_record_shape(self):
        from migrate.hive_grants_worker import _skipped_dependency_grant_row

        row = _skipped_dependency_grant_row("`hive_metastore`.`db`.`dbfs_orders`")
        assert row["object_type"] == "hive_grant"
        assert row["status"] == "skipped_dependency_not_migrated"
        assert "dbfs_orders" in row["error_message"]


# ----------------------------------------------------------------------
# hive_managed_dbfs_worker
# ----------------------------------------------------------------------


class TestHiveManagedDbfsWorker:
    """DBFS-root migration copies bytes from /dbfs/... to
    ``hive_dbfs_target_path`` on target ADLS and registers an EXTERNAL
    table on target pointing there. Full behavior drives spark.read /
    write so we test source-level contracts only."""

    def test_module_uses_staging_path_config(self):
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_managed_dbfs_worker.py"
        ).read_text()
        assert "hive_dbfs_staging_path" in src
        assert "hive_dbfs_target_path" not in src

    def test_target_table_is_managed_no_location(self):
        """STAGE 2 lands a MANAGED table in the target DBFS root — the target
        CREATE TABLE must NOT carry a LOCATION clause (that would make it
        external and defeat the DBFS-root rehome)."""
        from migrate.hive_managed_dbfs_worker import _staging_ctas_sql

        sql = _staging_ctas_sql("db", "t", "abfss://stage@a.dfs.core.windows.net/hive", [])
        assert "LOCATION" not in sql.upper()
        assert sql.startswith("CREATE OR REPLACE TABLE `hive_metastore`.`db`.`t`")
        assert "USING DELTA" in sql.upper()
        assert "delta.`abfss://stage@a.dfs.core.windows.net/hive/db/t/`" in sql

    def test_ctas_sql_preserves_partitions(self):
        from migrate.hive_managed_dbfs_worker import _staging_ctas_sql

        sql = _staging_ctas_sql("db", "t", "abfss://s@a.dfs.core.windows.net/h", ["country", "yr"])
        assert "PARTITIONED BY (`country`, `yr`)" in sql

    @staticmethod
    def _describe_rows(*pairs, partition_cols=()):
        """Build mock DESCRIBE TABLE rows: column pairs, then (optionally) the
        partition section (blank sep, '# Partition Information', '# col_name',
        then the partition columns repeated)."""
        rows = []

        def _row(col_name, data_type=""):
            r = MagicMock()
            r.asDict.return_value = {"col_name": col_name, "data_type": data_type, "comment": ""}
            return r

        for name, dtype in pairs:
            rows.append(_row(name, dtype))
        if partition_cols:
            rows.append(_row("", ""))
            rows.append(_row("# Partition Information", ""))
            rows.append(_row("# col_name", "data_type"))
            for pc in partition_cols:
                rows.append(_row(pc, "string"))
        return rows

    def _dbfs_config(self):
        cfg = _config_mock()
        cfg.migrate_hive_dbfs_root = True
        cfg.hive_dbfs_staging_path = "abfss://stage@acct.dfs.core.windows.net/hive_stage"
        return cfg

    @patch("migrate.hive_managed_dbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_dbfs_worker.time")
    @patch("migrate.hive_managed_dbfs_worker.execute_and_poll")
    def test_two_hop_stage_then_target_managed_ctas(self, mock_exec, mock_time, mock_wh_count):
        from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_wh_count.return_value = 5  # target managed count (via target warehouse)

        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 5
        spark.read.table.return_value = df
        spark.sql.return_value.collect.return_value = self._describe_rows(("id", "int"))

        res = migrate_hive_managed_dbfs(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=self._dbfs_config(), auth=MagicMock(), tracker=MagicMock(),
            spark=spark, wh_id="wh",
        )

        # STAGE 1: wrote df to the shared staging path (not the final home).
        staged = df.write.mode.return_value.format.return_value.save.call_args[0][0]
        assert staged == "abfss://stage@acct.dfs.core.windows.net/hive_stage/db/t/"
        # STAGE 2: target-side managed CTAS ran via the warehouse.
        ctas = mock_exec.call_args[0][2]
        assert ctas.startswith("CREATE OR REPLACE TABLE `hive_metastore`.`db`.`t`")
        assert "LOCATION" not in ctas.upper()
        assert res["status"] == "validated"
        assert res["target_row_count"] == 5

    @patch("migrate.hive_managed_dbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_dbfs_worker.time")
    @patch("migrate.hive_managed_dbfs_worker.execute_and_poll")
    def test_target_count_mismatch_is_validation_failed(self, mock_exec, mock_time, mock_wh_count):
        from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_wh_count.return_value = 3  # target has fewer rows than source

        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 5
        spark.read.table.return_value = df
        spark.sql.return_value.collect.return_value = self._describe_rows(("id", "int"))

        res = migrate_hive_managed_dbfs(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=self._dbfs_config(), auth=MagicMock(), tracker=MagicMock(),
            spark=spark, wh_id="wh",
        )
        assert res["status"] == "validation_failed"
        assert res["source_row_count"] == 5
        assert res["target_row_count"] == 3

    @patch("migrate.hive_managed_dbfs_worker.time")
    def test_missing_staging_path_fails_fast(self, mock_time):
        from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs

        mock_time.time.side_effect = [100.0, 100.0]
        cfg = self._dbfs_config()
        cfg.hive_dbfs_staging_path = ""
        res = migrate_hive_managed_dbfs(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=cfg, auth=MagicMock(), tracker=MagicMock(),
            spark=MagicMock(), wh_id="wh",
        )
        assert res["status"] == "failed"
        assert "hive_dbfs_staging_path" in res["error_message"]


# ----------------------------------------------------------------------
# hive_managed_nondbfs_worker
# ----------------------------------------------------------------------


class TestHiveManagedNondbfsWorker:
    """MANAGED non-DBFS tables have an explicit LOCATION off DBFS root.
    Migration registers them as EXTERNAL on target pointing at the same
    LOCATION (zero-copy — target reads the same bytes via its external
    location)."""

    def test_module_imports_cleanly(self):
        from migrate import hive_managed_nondbfs_worker

        assert hasattr(hive_managed_nondbfs_worker, "run")

    @staticmethod
    def _orchestrator_record():
        """The exact shape hive_orchestrator emits — keyed ``object_name``,
        with NO ``fqn`` key (see hive_orchestrator.py)."""
        return {
            "object_name": "`hive_metastore`.`integration_test_hive`.`nondbfs_sales`",
            "object_type": "hive_managed_nondbfs",
            "catalog_name": "hive_metastore",
            "schema_name": "integration_test_hive",
            "data_category": "hive_managed_nondbfs",
            "table_type": "MANAGED",
            "provider": "delta",
            "storage_location": "abfss://ext@acct.dfs.core.windows.net/nondbfs_sales",
        }

    @patch("migrate.hive_managed_nondbfs_worker.append_migration_status_via_warehouse")
    @patch("migrate.hive_managed_nondbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_nondbfs_worker.time")
    @patch("migrate.hive_managed_nondbfs_worker.execute_and_poll")
    def test_orchestrator_shaped_record_does_not_raise_keyerror(
        self, mock_execute, mock_time, mock_wh_count, mock_append
    ):
        """Regression for review finding #1: the worker read record['fqn'] but
        the orchestrator emits 'object_name' — every non-DBFS managed table
        threw KeyError. The migrated status must be keyed by object_name."""
        from migrate.hive_managed_nondbfs_worker import migrate_hive_managed_nondbfs

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        # NON-UC compute: target row-count comes via the warehouse, source via
        # the worker's spark explorer.
        mock_wh_count.return_value = 3

        rec = self._orchestrator_record()
        explorer = MagicMock()
        explorer.get_create_statement.return_value = (
            "CREATE TABLE hive_metastore.integration_test_hive.nondbfs_sales (id INT) "
            "USING delta LOCATION 'abfss://ext@acct.dfs.core.windows.net/nondbfs_sales'"
        )
        explorer.get_table_row_count.return_value = 3

        result = migrate_hive_managed_nondbfs(
            rec,
            config=_config_mock(),
            auth=MagicMock(),
            explorer=explorer,
            wh_id="wh-hv",
            tracking_fqn="migration_tracking.cp_migration",
            job_run_id="jr-1",
            status_wh_id="wh-src",
        )

        assert result["object_name"] == rec["object_name"]
        assert result["status"] == "validated"

    @patch("migrate.hive_managed_nondbfs_worker.append_migration_status_via_warehouse")
    @patch("migrate.hive_managed_nondbfs_worker.time")
    def test_ddl_fetch_failure_records_object_name_not_keyerror(self, mock_time, mock_append):
        """When DDL fetch fails, the failure row must still be keyed by
        object_name (the old code referenced the absent 'fqn' key)."""
        from migrate.hive_managed_nondbfs_worker import migrate_hive_managed_nondbfs

        mock_time.time.side_effect = [100.0, 100.5]
        rec = self._orchestrator_record()
        explorer = MagicMock()
        explorer.get_create_statement.side_effect = RuntimeError("boom")

        result = migrate_hive_managed_nondbfs(
            rec,
            config=_config_mock(),
            auth=MagicMock(),
            explorer=explorer,
            wh_id="wh-hv",
            tracking_fqn="migration_tracking.cp_migration",
            job_run_id="jr-1",
            status_wh_id="wh-src",
        )

        assert result["object_name"] == rec["object_name"]
        assert result["status"] == "failed"
        assert "boom" in result["error_message"]

    @patch("migrate.hive_managed_nondbfs_worker.append_migration_status_via_warehouse")
    @patch("migrate.hive_managed_nondbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_nondbfs_worker.time")
    @patch("migrate.hive_managed_nondbfs_worker.execute_and_poll")
    def test_replays_into_hive_metastore_and_keeps_location(
        self, mock_exec, mock_time, mock_wh_count, mock_append
    ):
        from migrate.hive_managed_nondbfs_worker import migrate_hive_managed_nondbfs

        mock_time.time.side_effect = [100.0, 105.0]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_wh_count.return_value = 3

        rec = self._orchestrator_record()
        explorer = MagicMock()
        # SHOW CREATE with no LOCATION -> worker must inject storage_location.
        explorer.get_create_statement.return_value = (
            "CREATE TABLE hive_metastore.integration_test_hive.nondbfs_sales (id INT) USING delta"
        )
        explorer.get_table_row_count.return_value = 3

        res = migrate_hive_managed_nondbfs(
            rec, config=_config_mock(), auth=MagicMock(), explorer=explorer,
            wh_id="wh", tracking_fqn="migration_tracking.cp_migration",
            job_run_id="jr-1", status_wh_id="wh-src",
        )
        replayed = mock_exec.call_args[0][2]
        assert "hive_metastore.integration_test_hive.nondbfs_sales" in replayed
        assert "hive_upgraded" not in replayed
        assert "LOCATION 'abfss://ext@acct.dfs.core.windows.net/nondbfs_sales'" in replayed
        assert res["status"] == "validated"


# ----------------------------------------------------------------------
# hive_orchestrator batching — covered by test_hive_orchestrator.py
# ----------------------------------------------------------------------


class TestHiveViewDependencySkip:
    def test_flags_backticked_reference(self):
        from migrate.hive_views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `hive_metastore`.`db`.`v` AS SELECT * FROM `hive_metastore`.`db`.`dbfs_orders`"
        not_migrated = {"`hive_metastore`.`db`.`dbfs_orders`"}
        assert view_dependency_skip(ddl, not_migrated) == "`hive_metastore`.`db`.`dbfs_orders`"

    def test_flags_dotted_reference(self):
        from migrate.hive_views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW hive_metastore.db.v AS SELECT * FROM hive_metastore.db.dbfs_orders"
        not_migrated = {"`hive_metastore`.`db`.`dbfs_orders`"}
        assert view_dependency_skip(ddl, not_migrated) == "`hive_metastore`.`db`.`dbfs_orders`"

    def test_none_when_all_deps_validated(self):
        from migrate.hive_views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `hive_metastore`.`db`.`v` AS SELECT * FROM `hive_metastore`.`db`.`good`"
        assert view_dependency_skip(ddl, {"`hive_metastore`.`db`.`dbfs_orders`"}) is None

    def test_empty_not_migrated_never_skips(self):
        from migrate.hive_views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `hive_metastore`.`db`.`v` AS SELECT * FROM `hive_metastore`.`db`.`x`"
        assert view_dependency_skip(ddl, set()) is None

    def test_transitive_view_on_skipped_view(self):
        """A view on a view that was itself skipped is caught once the skipped
        view's FQN is added to the not-migrated set (same-run transitivity)."""
        from migrate.hive_views_worker import view_dependency_skip

        # v2 selects from v1; v1 was skipped this run and added to the set.
        ddl_v2 = "CREATE OR REPLACE VIEW `hive_metastore`.`db`.`v2` AS SELECT * FROM `hive_metastore`.`db`.`v1`"
        not_migrated = {"`hive_metastore`.`db`.`v1`"}
        assert view_dependency_skip(ddl_v2, not_migrated) == "`hive_metastore`.`db`.`v1`"


class TestHiveViewCascadeInMigrate:
    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_view_on_not_migrated_table_is_skipped_not_executed(self, mock_exec, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 100.1]
        ddl = "CREATE VIEW `hive_metastore`.`db`.`v_orders` AS SELECT * FROM `hive_metastore`.`db`.`dbfs_orders`"
        cfg = _config_mock()
        res = migrate_hive_view(
            {"object_name": "`hive_metastore`.`db`.`v_orders`"},
            ddl,
            config=cfg,
            auth=MagicMock(),
            wh_id="wh",
            not_migrated_names={"`hive_metastore`.`db`.`dbfs_orders`"},
        )
        assert res["status"] == "skipped_dependency_not_migrated"
        assert "dbfs_orders" in res["error_message"]
        mock_exec.assert_not_called()

    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_view_with_validated_deps_migrates(self, mock_exec, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 100.5]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        ddl = "CREATE VIEW `hive_metastore`.`db`.`v` AS SELECT * FROM `hive_metastore`.`db`.`good`"
        cfg = _config_mock()
        res = migrate_hive_view(
            {"object_name": "`hive_metastore`.`db`.`v`"},
            ddl,
            config=cfg,
            auth=MagicMock(),
            wh_id="wh",
            not_migrated_names=set(),
        )
        assert res["status"] == "validated"
        mock_exec.assert_called_once()


class TestHiveOrchestratorBatching:
    """The existing test_hive_orchestrator.py covers the short-circuit
    path. These tests add batching-path guards."""

    def test_publishes_separate_task_values_per_category(self):
        """Source-level contract: orchestrator emits task values for each
        Hive category. The ``_batches`` keys are built via f-string
        (``f"{cat}_batches"``), so we check for the category iteration
        tuples directly. The list-style keys are literal strings.
        """
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_orchestrator.py").read_text()
        for cat in ("hive_external", "hive_managed_nondbfs", "hive_managed_dbfs_root"):
            assert f'"{cat}"' in src, (
                f"hive_orchestrator must iterate category {cat!r} when emitting batched task values"
            )
        for list_key in ("hive_view_list", "hive_function_list"):
            assert list_key in src, f"hive_orchestrator must publish {list_key}"

    def test_creates_target_database_before_category_batches(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_orchestrator.py").read_text()
        create_idx = src.find("CREATE DATABASE IF NOT EXISTS `hive_metastore`")
        last_batch_idx = src.rfind('("hive_external", "hive_managed_nondbfs", "hive_managed_dbfs_root")')
        assert create_idx != -1, "Orchestrator must ensure target databases exist"
        assert last_batch_idx != -1, "Category-iteration tuple not found in orchestrator"
        assert create_idx < last_batch_idx, (
            "Target database creation must precede the populate-path category "
            "iteration — otherwise downstream workers hit NO_SUCH_DATABASE."
        )
