from __future__ import annotations

from unittest.mock import MagicMock

from migrate.rls_cm import (
    capture_rls_cm,
    has_rls_cm,
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


class TestHasRlsCm:
    def test_true_when_filter_present(self):
        assert has_rls_cm({"filter_fn_fqn": "fn", "filter_columns": [], "masks": []}) is True

    def test_true_when_masks_present(self):
        assert (
            has_rls_cm(
                {
                    "filter_fn_fqn": None,
                    "filter_columns": [],
                    "masks": [{"column": "x", "fn_fqn": "m"}],
                }
            )
            is True
        )

    def test_false_when_empty(self):
        assert has_rls_cm({"filter_fn_fqn": None, "filter_columns": [], "masks": []}) is False


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
