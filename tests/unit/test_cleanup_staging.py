"""Tests for src/migrate/cleanup_staging.py — Path A post-migrate task."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest


@contextmanager
def _patch_config(config):
    with patch("common.config.MigrationConfig.from_workspace_file", return_value=config):
        yield


@contextmanager
def _patch_tracker(tracker):
    with patch("migrate.cleanup_staging.TrackingManager", return_value=tracker), \
         patch("migrate.cleanup_staging.AuthManager"):
        yield


def test_cleanup_staging_skips_when_strategy_not_staging_copy():
    """If rls_cm_strategy is not 'staging_copy', do nothing."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "drop_and_restore"  # not staging_copy
    config.include_uc = True

    with _patch_config(config), _patch_tracker(MagicMock()):
        run(MagicMock(), spark)

    spark.sql.assert_not_called()


def test_cleanup_staging_skips_when_include_uc_false():
    """include_uc=false also short-circuits (parity with restore_rls_cm)."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    config.include_uc = False
    tracker = MagicMock()

    with _patch_config(config), _patch_tracker(tracker):
        run(MagicMock(), spark)

    spark.sql.assert_not_called()
    tracker.get_active_stagings.assert_not_called()


def test_cleanup_staging_skips_when_no_active_stagings():
    """No-op when there are no active stagings."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    config.include_uc = True
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = []

    with _patch_config(config), _patch_tracker(tracker):
        run(MagicMock(), spark)

    spark.sql.assert_not_called()
    tracker.mark_staging_dropped.assert_not_called()


def test_cleanup_staging_drops_each_active_staging_and_marks_manifest():
    """For each active staging, issue DROP TABLE and mark dropped_at."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    config.include_uc = True
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = [
        {"original_fqn": "`c`.`s`.`t1`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_a`",
         "created_at": None, "run_id": "r1"},
        {"original_fqn": "`c`.`s`.`t2`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_b`",
         "created_at": None, "run_id": "r1"},
    ]

    with _patch_config(config), _patch_tracker(tracker):
        run(MagicMock(), spark)

    drop_calls = [c.args[0] for c in spark.sql.call_args_list if "DROP TABLE" in c.args[0]]
    assert len(drop_calls) == 2
    # Order matches active-stagings order (oldest first).
    assert "stg_a" in drop_calls[0]
    assert "stg_b" in drop_calls[1]
    assert tracker.mark_staging_dropped.call_count == 2


def test_cleanup_staging_continues_on_per_table_failure_then_raises():
    """One drop fails → mark_staging_drop_failed; others still attempted; final RuntimeError."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    config.include_uc = True
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = [
        {"original_fqn": "`c`.`s`.`t1`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_a`",
         "created_at": None, "run_id": "r1"},
        {"original_fqn": "`c`.`s`.`t2`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_b`",
         "created_at": None, "run_id": "r1"},
    ]
    # First drop raises, second succeeds.
    spark.sql.side_effect = [Exception("boom"), None]

    with _patch_config(config), _patch_tracker(tracker):
        with pytest.raises(RuntimeError) as excinfo:
            run(MagicMock(), spark)

    # Failed first
    tracker.mark_staging_drop_failed.assert_called_once()
    failed_args = tracker.mark_staging_drop_failed.call_args
    assert "stg_a" in failed_args.args[0]
    # Succeeded second
    tracker.mark_staging_dropped.assert_called_once()
    succ_args = tracker.mark_staging_dropped.call_args
    assert "stg_b" in succ_args.args[0]
    # Final RuntimeError mentions count and the failing FQN
    assert "1" in str(excinfo.value)
    assert "stg_a" in str(excinfo.value)
