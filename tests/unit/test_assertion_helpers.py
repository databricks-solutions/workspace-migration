"""Unit tests for the shared ``expect_validated`` helper.

Closes review H11: the helper enforces ``status='validated'`` AND empty
``error_message`` so workers that record ``validated`` with a warning
in ``error_message`` (e.g. ``WARNING: rebuilt with stale schema``)
don't silently pass an assertion that meant to say "the migration
succeeded clean".
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.integration._assertion_helpers import (
    assert_migrate_idempotent,
    assert_target_matches_ground_truth,
    expect_validated,
)


def _spark_count(n):
    """Mock spark whose COUNT(*) query returns a row with .n == n."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = {"n": n}
    return spark


def _spark_rows(rows):
    """Mock spark whose SELECT returns the given dict rows via .collect()."""
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = rows
    return spark


def _run(result_state, message=""):
    return SimpleNamespace(
        state=SimpleNamespace(
            result_state=SimpleNamespace(value=result_state),
            state_message=message,
        )
    )


class TestGroundTruth:
    """assert_target_matches_ground_truth reads the TARGET, not migration_status."""

    def test_count_match_passes(self):
        errs: list[str] = []
        assert assert_target_matches_ground_truth(
            _spark_count(4), "cat.sch.orders", errs, expected_count=4
        ) is True
        assert errs == []

    def test_count_mismatch_fails_with_data_loss_hint(self):
        errs: list[str] = []
        # target has 2 of the 4 seeded rows — the #21 row-filter data loss
        assert assert_target_matches_ground_truth(
            _spark_count(2), "cat.sch.orders", errs, expected_count=4
        ) is False
        assert errs and "2 != expected 4" in errs[0]

    def test_masked_values_caught_by_value_set(self):
        errs: list[str] = []
        # target emails came across masked -> value set differs from ground truth
        spark = _spark_rows([{"customer_id": 1, "email": "***@***"}])
        ok = assert_target_matches_ground_truth(
            spark, "cat.sch.customers", errs,
            expected_rows=[(1, "alice@example.com")],
            select_cols=["customer_id", "email"],
        )
        assert ok is False
        assert errs and "target values != ground truth" in errs[0]

    def test_value_set_match_passes(self):
        errs: list[str] = []
        spark = _spark_rows([{"customer_id": 1, "email": "alice@example.com"}])
        assert assert_target_matches_ground_truth(
            spark, "cat.sch.customers", errs,
            expected_rows=[(1, "alice@example.com")],
            select_cols=["customer_id", "email"],
        ) is True
        assert errs == []


class TestIdempotent:
    """assert_migrate_idempotent runs the job again and demands a clean result."""

    def test_clean_rerun_passes(self):
        errs: list[str] = []
        wc = MagicMock()
        wc.jobs.run_now_and_wait.return_value = _run("SUCCESS", "")
        assert assert_migrate_idempotent(wc, 123, errs) is True
        assert errs == []
        wc.jobs.run_now_and_wait.assert_called_once_with(job_id=123)

    def test_failed_rerun_fails(self):
        errs: list[str] = []
        wc = MagicMock()
        wc.jobs.run_now_and_wait.return_value = _run(
            "FAILED", "LOCATION_OVERLAP: input path overlaps ..."
        )
        assert assert_migrate_idempotent(wc, 1, errs) is False
        assert errs and "not idempotent" in errs[0]

    def test_success_but_bad_signature_fails(self):
        # e.g. SUCCESS_WITH_FAILURES-style message carrying a known signature
        errs: list[str] = []
        wc = MagicMock()
        wc.jobs.run_now_and_wait.return_value = _run(
            "SUCCESS", "ResourceAlreadyExists: Shared Table already exists"
        )
        assert assert_migrate_idempotent(wc, 1, errs) is False
        assert errs and "non-idempotent signature" in errs[0]


class TestExpectValidatedDict:
    """The helper accepts plain dict rows — used when integration tests
    manipulate rows via ``.asDict()``."""

    def test_validated_with_empty_error_returns_true(self):
        errs: list[str] = []
        row = {"status": "validated", "error_message": None}
        assert expect_validated(row, "label", errs) is True
        assert errs == []

    def test_validated_with_empty_string_error_returns_true(self):
        """``error_message=""`` (empty string) is treated as no error.
        Workers historically write None OR empty string depending on
        whether they explicitly assign on success."""
        errs: list[str] = []
        row = {"status": "validated", "error_message": ""}
        assert expect_validated(row, "label", errs) is True
        assert errs == []

    def test_non_validated_status_appends_error(self):
        errs: list[str] = []
        row = {"status": "failed", "error_message": "boom"}
        assert expect_validated(row, "MyTest", errs) is False
        assert len(errs) == 1
        assert "MyTest" in errs[0]
        assert "'failed'" in errs[0]
        assert "boom" in errs[0]

    def test_validated_with_warning_in_error_message_fails(self):
        """The H11 case: status='validated' but error_message is set →
        false-green. Must fail the assertion."""
        errs: list[str] = []
        row = {
            "status": "validated",
            "error_message": "WARNING: rebuilt with stale schema",
        }
        assert expect_validated(row, "MV refresh", errs) is False
        assert len(errs) == 1
        assert "MV refresh" in errs[0]
        assert "WARNING" in errs[0]
        assert "H11" in errs[0]


class TestExpectValidatedRowLike:
    """The helper also accepts attribute-style row-like objects (PySpark
    Row exposes both subscript and attribute access — SimpleNamespace
    only exposes attributes)."""

    def test_attribute_access_when_subscript_unavailable(self):
        errs: list[str] = []
        row = SimpleNamespace(status="validated", error_message=None)
        assert expect_validated(row, "label", errs) is True
        assert errs == []

    def test_attribute_access_with_warning(self):
        errs: list[str] = []
        row = SimpleNamespace(status="validated", error_message="WARNING: degraded")
        assert expect_validated(row, "label", errs) is False
        assert "WARNING" in errs[0]


class TestExpectValidatedSparkRowShape:
    """PySpark Row exposes both subscript and attribute access. We test
    both paths land at the same answer."""

    @pytest.mark.parametrize(
        "row",
        [
            {"status": "validated", "error_message": None},
            SimpleNamespace(status="validated", error_message=None),
        ],
    )
    def test_validated_clean_passes_either_shape(self, row):
        errs: list[str] = []
        assert expect_validated(row, "label", errs) is True

    @pytest.mark.parametrize(
        "row",
        [
            {"status": "validation_failed", "error_message": "x"},
            SimpleNamespace(status="validation_failed", error_message="x"),
        ],
    )
    def test_non_validated_fails_either_shape(self, row):
        errs: list[str] = []
        assert expect_validated(row, "label", errs) is False
        assert len(errs) == 1


class TestExpectValidatedErrorMessageQuality:
    """The failure message must be specific enough to debug from —
    label + status + error_message all visible in the appended string."""

    def test_failure_includes_label(self):
        errs: list[str] = []
        row = {"status": "failed", "error_message": None}
        expect_validated(row, "2.5.B iceberg_sales", errs)
        assert "2.5.B iceberg_sales" in errs[0]

    def test_false_green_message_explains_the_pattern(self):
        """The H11 false-green message tells the reader WHY this is
        treated as a failure — otherwise future contributors will look
        at the warning text and think 'looks fine'."""
        errs: list[str] = []
        row = {"status": "validated", "error_message": "WARNING: x"}
        expect_validated(row, "label", errs)
        msg = errs[0]
        assert "validated" in msg
        assert "warning under a passing status" in msg
