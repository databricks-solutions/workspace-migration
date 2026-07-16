"""Regression tests for src/migrate/hive_orchestrator.py.

The notebook runs its main block at module import time (gated by the
``_is_notebook()`` helper which returns False outside Databricks). That
makes it awkward to drive the full flow in a pytest, but we can still
guard against the historical ``sys.exit`` regression at the source
level.
"""

from __future__ import annotations


def _source_text() -> str:
    """Read the hive_orchestrator notebook source directly — the simplest
    regression guard that doesn't depend on import ordering."""
    import pathlib

    path = pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_orchestrator.py"
    return path.read_text()


class TestHiveOrchestratorExitIdiom:
    """Source-level regression guard for the dbutils.notebook.exit fix.

    sys.exit(0) looked like a clean exit in Python, but Databricks Jobs
    treats any SystemExit raised from a notebook cell as FAILED. Keep
    the orchestrator free of sys.exit() calls.
    """

    def test_does_not_use_sys_exit(self):
        src = _source_text()
        # ``sys.exit(`` (the function call) is forbidden — use
        # dbutils.notebook.exit() for notebook short-circuits. Plain
        # ``import sys`` is allowed (used for sys.path bootstrap).
        assert "sys.exit(" not in src, (
            "hive_orchestrator must not call sys.exit() — use "
            "dbutils.notebook.exit(...) so Jobs treats the short-circuit "
            "as SUCCESS."
        )


class TestHivePendingAntiJoinIdempotency:
    """Regression guard for finding #12 (hive re-runs never idempotent).

    Discovery records hive tables as object_type='hive_table', but the
    workers record migration status under the CLASSIFIED type
    ('hive_external' / 'hive_managed_nondbfs' / 'hive_managed_dbfs_root').
    The pending-list anti-join originally matched on BOTH object_name AND
    object_type, so it NEVER subtracted an already-migrated hive table:
    every re-run re-created it and the target external path collided
    (LOCATION_OVERLAP). Both sides share the same object_name (the source
    hive FQN, which is namespace-unique), so the join must key on
    object_name ALONE. This is the cheap CI guard; the live re-run leg
    (RERUN_EXEMPT['migrate_hive']) is the lab-coupled complement.
    """

    @staticmethod
    def _norm(src: str) -> str:
        import re

        return re.sub(r"\s+", " ", src)

    def test_anti_join_subtracts_validated_objects(self):
        # The idempotency subtraction must exist at all.
        assert "LEFT ANTI JOIN" in _source_text()

    def test_anti_join_matches_on_object_name(self):
        norm = self._norm(_source_text())
        assert "ON i.object_name = c.object_name" in norm, (
            "hive pending anti-join must key on object_name so already-"
            "migrated hive tables are subtracted on re-run (finding #12)."
        )

    def test_anti_join_does_not_gate_on_object_type(self):
        norm = self._norm(_source_text())
        # The exact #12 regression: gating the join on object_type makes it
        # never match hive tables (discovery 'hive_table' vs status
        # 'hive_external'/…), so re-runs re-create + collide.
        assert "object_type = c.object_type" not in norm, (
            "hive pending anti-join must NOT gate on object_type — discovery "
            "and the workers record different object_types for the same "
            "table, so this join would never match and re-runs would re-"
            "create the table (finding #12)."
        )


class TestHiveOrchestratorCreatesDatabase:
    """Like-for-like: the orchestrator ensures target DATABASES exist in
    hive_metastore, and must NOT create a UC catalog."""

    def test_creates_database_in_hive_metastore(self):
        src = _source_text()
        assert "CREATE DATABASE IF NOT EXISTS `hive_metastore`" in src

    def test_does_not_create_catalog(self):
        src = _source_text()
        assert "CREATE CATALOG" not in src

    def test_no_reference_to_hive_target_catalog(self):
        src = _source_text()
        assert "hive_target_catalog" not in src
