"""Source-level regression guards for the teardown notebooks.

Integration runs that left target-side catalogs stale broke subsequent
runs with TABLE_OR_VIEW_ALREADY_EXISTS (iceberg_sales) and its Hive
counterpart. Teardown now cleans BOTH sides — source via spark.sql,
target via auth_mgr + execute_and_poll on the target warehouse.

These tests ensure the cleanup paths don't silently disappear in a
future refactor.
"""

from __future__ import annotations

import pathlib


def _teardown_uc_text() -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / "tests" / "integration" / "teardown_uc.py").read_text()


def _teardown_hive_text() -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / "tests" / "integration" / "teardown_hive.py").read_text()


class TestTeardownUcTargetCleanup:
    """teardown_uc must drop on source AND target."""

    def test_drops_source_integration_catalogs(self):
        src = _teardown_uc_text()
        assert "DROP CATALOG IF EXISTS integration_test_src CASCADE" in src

    def test_drops_share_consumer_on_target(self):
        """cp_migration_share_consumer is target-side — without dropping
        it the next migrate run's CREATE CATALOG USING SHARE fails."""
        src = _teardown_uc_text()
        assert "cp_migration_share_consumer" in src

    def test_uses_execute_and_poll_for_target_drop(self):
        """Target-side DROPs run through execute_and_poll on the target
        warehouse (not source spark), so the right metastore is hit."""
        src = _teardown_uc_text()
        assert "execute_and_poll" in src
        assert "find_warehouse" in src

    def test_restores_config_yaml_backup(self):
        """After drops, teardown must move the
        ``.pre-integration-test.bak`` back into place so subsequent
        operator runs aren't poisoned by test-only toggles."""
        src = _teardown_uc_text()
        assert ".pre-integration-test.bak" in src
        assert "shutil.move" in src or "shutil.copy2" in src

    def test_clears_fixture_rows_from_tracking_tables(self):
        """Without this, stale ``validated`` rows make
        ``get_pending_objects`` treat fixtures as already-migrated on the
        NEXT run — migrate workers produce no work, Phase 3 workers fail
        trying to apply to target tables this run never created.
        DELETE is scoped to integration_test_src / test_schema so any
        real state sharing the catalog isn't touched."""
        src = _teardown_uc_text()
        assert "DELETE FROM migration_tracking.cp_migration.migration_status" in src
        assert "integration_test_src" in src
        assert "DELETE FROM migration_tracking.cp_migration.discovery_inventory" in src


class TestTeardownHiveTargetCleanup:
    """teardown_hive must drop the migrated target hive_metastore test
    database on both sides — like-for-like migration lands Hive objects
    back into hive_metastore, so no UC catalog is created anymore."""

    def test_drops_source_database(self):
        src = _teardown_hive_text()
        assert "DROP DATABASE IF EXISTS hive_metastore.integration_test_hive CASCADE" in src

    def test_teardown_hive_drops_target_hive_metastore_database(self):
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2]
            / "tests" / "integration" / "teardown_hive.py"
        ).read_text()
        assert "DROP DATABASE IF EXISTS `hive_metastore`" in src
        assert "hive_target_catalog" not in src

    def test_uses_execute_and_poll_for_target(self):
        src = _teardown_hive_text()
        assert "execute_and_poll" in src
        assert "find_warehouse" in src

    def test_restores_config_yaml_backup(self):
        src = _teardown_hive_text()
        assert ".pre-integration-test.bak" in src

    def test_clears_fixture_rows_from_tracking_tables(self):
        """Same rationale as the UC teardown — stale ``validated`` rows
        on hive_* object_types block re-runs."""
        src = _teardown_hive_text()
        assert "DELETE FROM migration_tracking.cp_migration.migration_status" in src
        assert "DELETE FROM migration_tracking.cp_migration.discovery_inventory" in src
        assert "integration_test_hive" in src
