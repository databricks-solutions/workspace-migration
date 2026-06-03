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
