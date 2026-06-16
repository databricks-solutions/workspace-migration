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
    config.rls_cm_strategy = ""  # default skip — not staging_copy

    with _patch_config(config), _patch_tracker(MagicMock()):
        run(MagicMock(), spark)

    spark.sql.assert_not_called()


def test_cleanup_staging_skips_when_no_active_stagings():
    """No-op when there are no active stagings."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = []

    with _patch_config(config), _patch_tracker(tracker):
        run(MagicMock(), spark)

    spark.sql.assert_not_called()
    tracker.mark_staging_dropped.assert_not_called()


def test_cleanup_staging_removes_from_share_then_drops_each_staging():
    """For each active staging: ALTER SHARE REMOVE TABLE, then DROP TABLE,
    then mark dropped_at. UC rejects DROP if the table is still in an
    active Delta Share, so the share-removal step is mandatory before drop.
    """
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = [
        {"original_fqn": "`c`.`s`.`t1`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_a`",
         "created_at": None, "run_id": "r1"},
        {"original_fqn": "`c`.`s`.`t2`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_b`",
         "created_at": None, "run_id": "r1"},
    ]

    with _patch_config(config), _patch_tracker(tracker):
        run(MagicMock(), spark)

    sql_calls = [c.args[0] for c in spark.sql.call_args_list]
    alter_calls = [s for s in sql_calls if "ALTER SHARE" in s and "REMOVE TABLE" in s]
    drop_calls = [s for s in sql_calls if "DROP TABLE" in s]
    assert len(alter_calls) == 2
    assert len(drop_calls) == 2
    # Per-staging ordering: ALTER SHARE comes before DROP TABLE for the same fqn.
    a_alter_idx = next(i for i, s in enumerate(sql_calls) if "stg_a" in s and "ALTER SHARE" in s)
    a_drop_idx = next(i for i, s in enumerate(sql_calls) if "stg_a" in s and "DROP TABLE" in s)
    assert a_alter_idx < a_drop_idx
    # Outer ordering: a before b (oldest first).
    assert "stg_a" in alter_calls[0]
    assert "stg_b" in alter_calls[1]
    assert tracker.mark_staging_dropped.call_count == 2


def test_cleanup_staging_swallows_not_in_share_on_alter_share():
    """Re-run after partial cleanup: a previously removed staging table
    should produce a benign 'not in share' / 'does not exist' message
    on ALTER SHARE; cleanup must continue and DROP TABLE the orphan."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = [
        {"original_fqn": "`c`.`s`.`t1`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_a`",
         "created_at": None, "run_id": "r1"},
    ]
    # ALTER SHARE raises "not shared", DROP TABLE succeeds.
    spark.sql.side_effect = [Exception("Table is not shared"), None]

    with _patch_config(config), _patch_tracker(tracker):
        run(MagicMock(), spark)

    tracker.mark_staging_dropped.assert_called_once()
    tracker.mark_staging_drop_failed.assert_not_called()


def test_cleanup_staging_continues_on_per_table_failure_then_raises():
    """One drop fails → mark_staging_drop_failed; others still attempted; final RuntimeError."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = [
        {"original_fqn": "`c`.`s`.`t1`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_a`",
         "created_at": None, "run_id": "r1"},
        {"original_fqn": "`c`.`s`.`t2`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_b`",
         "created_at": None, "run_id": "r1"},
    ]
    # Per-staging: ALTER SHARE succeeds, DROP TABLE raises (first), then
    # ALTER SHARE succeeds + DROP TABLE succeeds (second).
    spark.sql.side_effect = [None, Exception("boom"), None, None]

    with _patch_config(config), _patch_tracker(tracker), pytest.raises(RuntimeError) as excinfo:
        run(MagicMock(), spark)

    tracker.mark_staging_drop_failed.assert_called_once()
    failed_args = tracker.mark_staging_drop_failed.call_args
    assert "stg_a" in failed_args.args[0]
    tracker.mark_staging_dropped.assert_called_once()
    succ_args = tracker.mark_staging_dropped.call_args
    assert "stg_b" in succ_args.args[0]
    assert "1" in str(excinfo.value)
    assert "stg_a" in str(excinfo.value)
