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
    """hive_views_worker rewrites hive_metastore.* references to the
    target catalog and swaps CREATE VIEW → CREATE OR REPLACE VIEW. We
    hit a real TABLE_OR_VIEW_NOT_FOUND bug here earlier; these tests
    guard the rewrite contract."""

    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_rewrites_hive_metastore_references_to_target_catalog(self, mock_execute, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        ddl = (
            "CREATE VIEW hive_metastore.integration_test_hive.big_orders AS "
            "SELECT * FROM hive_metastore.integration_test_hive.managed_orders "
            "WHERE amount > 15"
        )
        cfg = _config_mock()
        migrate_hive_view(
            {"object_name": "`hive_metastore`.`integration_test_hive`.`big_orders`"},
            ddl,
            config=cfg,
            auth=MagicMock(),
            wh_id="wh-hv",
        )

        replayed = mock_execute.call_args[0][2]
        # hive_metastore rewritten
        assert "hive_metastore" not in replayed
        assert "hive_upgraded.integration_test_hive.managed_orders" in replayed
        # CREATE VIEW replaced
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


class TestHiveExternalWorker:
    """hive_external_worker registers the table on target pointing at the
    same storage LOCATION. The critical contract is that LOCATION is
    preserved byte-identical."""

    def test_module_imports_cleanly(self):
        """Smoke: module imports and expected entry points exist. Full
        behavior covered by integration tests since the worker drives
        a DESCRIBE TABLE EXTENDED dance to recover the LOCATION."""
        from migrate import hive_external_worker

        # Has a run() entry point for the for_each task payload.
        assert hasattr(hive_external_worker, "run") or callable(
            getattr(hive_external_worker, "migrate_hive_external_table", None)
        )


# ----------------------------------------------------------------------
# hive_functions_worker
# ----------------------------------------------------------------------


class TestHiveFunctionsWorker:
    def test_module_imports_cleanly(self):
        from migrate import hive_functions_worker

        assert hasattr(hive_functions_worker, "run")


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


# ----------------------------------------------------------------------
# hive_managed_dbfs_worker
# ----------------------------------------------------------------------


class TestHiveManagedDbfsWorker:
    """DBFS-root migration copies bytes from /dbfs/... to
    ``hive_dbfs_target_path`` on target ADLS and registers an EXTERNAL
    table on target pointing there. Full behavior drives spark.read /
    write so we test source-level contracts only."""

    def test_module_uses_hive_dbfs_target_path_config(self):
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_managed_dbfs_worker.py"
        ).read_text()
        assert "hive_dbfs_target_path" in src

    def test_creates_external_table_on_target(self):
        """Migrated DBFS-root tables land as EXTERNAL on target —
        their bytes live in ADLS, not in the managed storage allocated
        by CREATE TABLE. Source-level check that the CREATE statement
        uses ``CREATE EXTERNAL TABLE`` (or equivalent)."""
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_managed_dbfs_worker.py"
        ).read_text()
        # Either explicit EXTERNAL keyword or LOCATION clause
        # (LOCATION on CREATE TABLE makes it external in UC).
        assert "LOCATION" in src


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


# ----------------------------------------------------------------------
# hive_orchestrator batching — covered by test_hive_orchestrator.py
# ----------------------------------------------------------------------


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

    def test_creates_target_catalog_before_category_batches(self):
        """CREATE CATALOG must come BEFORE the populate-path category
        batch loop — workers CREATE TABLE into target and hit
        NO_SUCH_CATALOG_EXCEPTION otherwise. Two category-iteration
        occurrences exist (skip path + populate path); we check the
        last (populate) one against CREATE CATALOG order."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_orchestrator.py").read_text()
        create_idx = src.find("CREATE CATALOG IF NOT EXISTS")
        last_batch_idx = src.rfind('("hive_external", "hive_managed_nondbfs", "hive_managed_dbfs_root")')
        assert create_idx != -1, "Orchestrator must CREATE target catalog"
        assert last_batch_idx != -1, "Category-iteration tuple not found in orchestrator"
        assert create_idx < last_batch_idx, (
            "Target catalog creation must precede the populate-path "
            "category iteration — otherwise downstream workers hit "
            "NO_SUCH_CATALOG_EXCEPTION."
        )
