from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from migrate.rls_cm import (
    capture_rls_cm,
    has_rls_cm,
    restore_rls_cm,
    strip_rls_cm,
)


class TestCaptureRlsCm:
    def test_captures_row_filter_and_masks(self):
        auth = MagicMock()
        info = MagicMock()
        info.row_filter = MagicMock(
            function_name="cat.sec.only_my_team",
            input_column_names=["team_id", "region"],
        )
        col_a = MagicMock()
        col_a.name = "ssn"
        col_a.mask = MagicMock(function_name="cat.sec.redact_ssn", using_column_names=[])
        col_b = MagicMock()
        col_b.name = "dob"
        col_b.mask = MagicMock(function_name="cat.sec.redact_dob", using_column_names=["country"])
        col_c = MagicMock()
        col_c.name = "amount"
        col_c.mask = None
        info.columns = [col_a, col_b, col_c]
        auth.source_client.tables.get.return_value = info

        captured = capture_rls_cm(auth, "`c`.`s`.`t`")

        assert captured["filter_fn_fqn"] == "cat.sec.only_my_team"
        assert captured["filter_columns"] == ["team_id", "region"]
        assert len(captured["masks"]) == 2
        mask_ssn = next(m for m in captured["masks"] if m["column"] == "ssn")
        mask_dob = next(m for m in captured["masks"] if m["column"] == "dob")
        assert mask_ssn["fn_fqn"] == "cat.sec.redact_ssn"
        assert mask_ssn["using_columns"] == []
        assert mask_dob["using_columns"] == ["country"]
        auth.source_client.tables.get.assert_called_once_with("c.s.t")

    def test_captures_empty_when_no_policy(self):
        auth = MagicMock()
        info = MagicMock()
        info.row_filter = None
        info.columns = []
        auth.source_client.tables.get.return_value = info
        captured = capture_rls_cm(auth, "c.s.t")
        assert captured == {"filter_fn_fqn": None, "filter_columns": [], "masks": []}
        assert has_rls_cm(captured) is False

    def test_ignores_columns_without_mask(self):
        auth = MagicMock()
        info = MagicMock()
        info.row_filter = None
        col = MagicMock()
        col.name = "email"
        col.mask = None
        info.columns = [col]
        auth.source_client.tables.get.return_value = info
        captured = capture_rls_cm(auth, "c.s.t")
        assert captured["masks"] == []


class TestStripRlsCm:
    def test_strips_filter_and_masks(self):
        spark = MagicMock()
        captured = {
            "filter_fn_fqn": "cat.sec.fn",
            "filter_columns": ["x"],
            "masks": [
                {"column": "ssn", "fn_fqn": "cat.sec.m1", "using_columns": []},
                {"column": "dob", "fn_fqn": "cat.sec.m2", "using_columns": ["country"]},
            ],
        }
        strip_rls_cm(spark, "`c`.`s`.`t`", captured)
        calls = [c.args[0] for c in spark.sql.call_args_list]
        assert any("DROP ROW FILTER" in c for c in calls)
        assert any("ALTER COLUMN `ssn` DROP MASK" in c for c in calls)
        assert any("ALTER COLUMN `dob` DROP MASK" in c for c in calls)

    def test_no_filter_no_drop_row_filter_call(self):
        spark = MagicMock()
        captured = {
            "filter_fn_fqn": None,
            "filter_columns": [],
            "masks": [{"column": "ssn", "fn_fqn": "m1", "using_columns": []}],
        }
        strip_rls_cm(spark, "c.s.t", captured)
        calls = [c.args[0] for c in spark.sql.call_args_list]
        assert not any("DROP ROW FILTER" in c for c in calls)
        assert any("ALTER COLUMN `ssn` DROP MASK" in c for c in calls)

    def test_drop_row_filter_idempotent_on_already_gone(self):
        """If a previous crashed run dropped the filter but didn't record
        it, re-running strip should NOT raise — UC's 'does not exist'
        error is swallowed."""
        spark = MagicMock()
        spark.sql.side_effect = [Exception("ROW_FILTER_DOES_NOT_EXIST")]
        captured = {"filter_fn_fqn": "fn", "filter_columns": [], "masks": []}
        strip_rls_cm(spark, "c.s.t", captured)  # must not raise

    def test_drop_row_filter_propagates_unexpected_error(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("PERMISSION_DENIED")
        captured = {"filter_fn_fqn": "fn", "filter_columns": [], "masks": []}
        with pytest.raises(Exception, match="PERMISSION_DENIED"):
            strip_rls_cm(spark, "c.s.t", captured)


class TestRestoreRlsCm:
    def test_restores_filter_and_masks(self):
        spark = MagicMock()
        captured = {
            "filter_fn_fqn": "cat.sec.fn",
            "filter_columns": ["team_id", "region"],
            "masks": [
                {"column": "ssn", "fn_fqn": "cat.sec.m1", "using_columns": []},
                {"column": "dob", "fn_fqn": "cat.sec.m2", "using_columns": ["country", "region"]},
            ],
        }
        restore_rls_cm(spark, "`c`.`s`.`t`", captured)
        calls = [c.args[0] for c in spark.sql.call_args_list]

        rf_call = next(c for c in calls if "SET ROW FILTER" in c)
        assert "cat.sec.fn" in rf_call
        assert "`team_id`" in rf_call and "`region`" in rf_call

        ssn_call = next(c for c in calls if "`ssn`" in c and "SET MASK" in c)
        assert "cat.sec.m1" in ssn_call
        # no USING suffix when using_columns is empty
        assert "USING COLUMNS" not in ssn_call

        dob_call = next(c for c in calls if "`dob`" in c and "SET MASK" in c)
        assert "cat.sec.m2" in dob_call
        assert "USING COLUMNS (`country`, `region`)" in dob_call

    def test_restore_with_no_filter_only_masks(self):
        spark = MagicMock()
        captured = {
            "filter_fn_fqn": None,
            "filter_columns": [],
            "masks": [{"column": "ssn", "fn_fqn": "m1", "using_columns": []}],
        }
        restore_rls_cm(spark, "c.s.t", captured)
        calls = [c.args[0] for c in spark.sql.call_args_list]
        assert not any("SET ROW FILTER" in c for c in calls)
        assert any("`ssn` SET MASK" in c for c in calls)

    def test_skips_mask_entries_missing_column_or_fn(self):
        """Malformed manifest rows don't crash restore — the corrupt
        entry is skipped; others replay normally."""
        spark = MagicMock()
        captured = {
            "filter_fn_fqn": None,
            "filter_columns": [],
            "masks": [
                {"column": None, "fn_fqn": "m1"},
                {"column": "ssn", "fn_fqn": None},
                {"column": "valid", "fn_fqn": "m2", "using_columns": []},
            ],
        }
        restore_rls_cm(spark, "c.s.t", captured)
        calls = [c.args[0] for c in spark.sql.call_args_list]
        assert len(calls) == 1
        assert "`valid` SET MASK" in calls[0]


class TestMakeStagingTableFqn:
    def test_deterministic_and_short(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("c.s.t", "run-1", "tcat")
        b = make_staging_table_fqn("c.s.t", "run-1", "tcat")
        assert a == b
        # FQN: `tcat`.`cp_migration_staging`.`stg_<hash>`
        assert a.startswith("`tcat`.`cp_migration_staging`.`stg_")
        assert a.endswith("`")
        # 12-char hash → "stg_xxxxxxxxxxxx" inside backticks
        last_part = a.split("`")[-2]
        assert last_part.startswith("stg_")
        assert len(last_part) == len("stg_") + 12

    def test_different_runs_different_staging_names(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("c.s.t", "run-1", "tcat")
        b = make_staging_table_fqn("c.s.t", "run-2", "tcat")
        assert a != b

    def test_different_originals_different_staging_names(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("c.s.t1", "run-1", "tcat")
        b = make_staging_table_fqn("c.s.t2", "run-1", "tcat")
        assert a != b

    def test_handles_backticked_input(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("`c`.`s`.`t`", "run-1", "tcat")
        b = make_staging_table_fqn("c.s.t", "run-1", "tcat")
        # Backticked vs unbacked must hash to same value (canonicalized).
        assert a == b

    def test_uses_provided_tracking_catalog(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("c.s.t", "run-1", "main_tracking")
        assert "`main_tracking`.`cp_migration_staging`" in a
