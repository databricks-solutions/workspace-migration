"""Unit tests for the shared ``expect_validated`` helper.

Closes review H11: the helper enforces ``status='validated'`` AND empty
``error_message`` so workers that record ``validated`` with a warning
in ``error_message`` (e.g. ``WARNING: rebuilt with stale schema``)
don't silently pass an assertion that meant to say "the migration
succeeded clean".
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.integration._assertion_helpers import expect_validated


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
