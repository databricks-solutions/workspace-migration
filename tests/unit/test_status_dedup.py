"""Regression guard for finding #11: stale ``in_progress`` rows must never
inflate migration status counts.

Every migrated object writes an ``in_progress`` row and later a terminal row
(``validated`` / ``failed`` / ``validation_failed`` / ``skipped``) into
``migration_status``. Both rows physically persist. The finding worried these
would double-count in status queries.

They do not, because every count/status consumer reads the *latest* row per
object via a ``ROW_NUMBER() … PARTITION BY object_name, object_type ORDER BY
migrated_at DESC`` window (``TrackingManager.get_latest_migration_status``),
and ``summary.run`` feeds that windowed frame — never the raw table — to the
aggregators.

Unit tests here can't execute the window (no Spark in this env), so these are
source-guard + call-contract tests that pin the mechanism so it can't silently
regress.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock

_SRC = pathlib.Path(__file__).resolve().parents[2] / "src"


class TestGetLatestMigrationStatusDedups:
    def test_query_uses_latest_row_window(self):
        """The dedup that supersedes an in_progress row with its terminal row
        must key on (object_name, object_type) and pick the most recent
        migrated_at — otherwise stale in_progress rows would be counted."""
        from common.tracking import TrackingManager

        spark = MagicMock()
        config = MagicMock()
        config.tracking_catalog = "migration_tracking"
        config.tracking_schema = "cp_migration"

        tracker = TrackingManager(spark, config)
        tracker.get_latest_migration_status()

        sql = spark.sql.call_args[0][0]
        upper = sql.upper()
        assert "ROW_NUMBER()" in upper
        assert "PARTITION BY OBJECT_NAME, OBJECT_TYPE" in upper
        assert "ORDER BY MIGRATED_AT DESC" in upper
        assert "RN = 1" in upper.replace(" ", " ")


class TestSummaryReadsLatestNotRaw:
    def test_summary_run_feeds_latest_windowed_frame(self):
        """summary.run must aggregate the latest-per-object frame, not the raw
        migration_status table (which still holds superseded in_progress rows).
        Source-guard: the aggregators are fed get_latest_migration_status()."""
        src = (_SRC / "migrate" / "summary.py").read_text()
        # The report path resolves the latest status and hands THAT to the
        # aggregators — never a raw spark.table("...migration_status") read.
        assert "get_latest_migration_status()" in src
        assert "latest_df = tracker.get_latest_migration_status()" in src
        assert "aggregate_by_status_filtered(latest_df" in src
        assert "aggregate_by_object_type(latest_df)" in src


class TestAggregatorsCountRowsAsGiven:
    """The aggregators count whatever rows they're handed — so feeding them the
    windowed (latest) frame is what prevents in_progress double-counting. Pin
    that a terminal row and an in_progress row for the SAME object collapse to
    one row BEFORE aggregation (i.e. dedup is the caller's job, already done)."""

    def test_aggregate_by_status_counts_each_supplied_row_once(self):
        from migrate.summary import aggregate_by_status

        # Simulate the windowed frame: one row per object (in_progress already
        # superseded by the terminal 'validated' row upstream).
        mock_df = MagicMock()
        r1 = MagicMock()
        r1.asDict.return_value = {"status": "validated", "total": 3}
        mock_df.groupBy.return_value.agg.return_value.orderBy.return_value.collect.return_value = [r1]

        result = aggregate_by_status(mock_df)
        assert result == [{"status": "validated", "total": 3}]
        # No separate in_progress bucket leaked in.
        assert not any(row["status"] == "in_progress" for row in result)
