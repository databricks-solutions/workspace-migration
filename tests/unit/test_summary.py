from __future__ import annotations

from unittest.mock import MagicMock

from migrate.summary import (
    aggregate_by_status,
    aggregate_by_status_filtered,
    get_failed_objects,
    print_failures,
    print_object_type_table,
    print_status_table,
)

# --------------------------------------------------------------------------- #
#  print_status_table
# --------------------------------------------------------------------------- #


class TestPrintStatusTable:
    def test_print_status_table(self, capsys):
        rows = [
            {"status": "validated", "total": 5},
            {"status": "failed", "total": 2},
        ]

        print_status_table(rows)

        captured = capsys.readouterr().out
        assert "validated" in captured
        assert "5" in captured
        assert "failed" in captured
        assert "2" in captured
        assert "TOTAL" in captured
        assert "7" in captured


# --------------------------------------------------------------------------- #
#  print_object_type_table
# --------------------------------------------------------------------------- #


class TestPrintObjectTypeTable:
    def test_print_object_type_table(self, capsys):
        rows = [
            {
                "object_type": "managed_table",
                "total": 10,
                "validated": 8,
                "failed": 1,
                "validation_failed": 0,
                "skipped": 1,
                "in_progress": 0,
            },
        ]

        print_object_type_table(rows)

        captured = capsys.readouterr().out
        assert "managed_table" in captured
        assert "10" in captured
        assert "8" in captured
        assert "OBJECT TYPE" in captured.upper()


# --------------------------------------------------------------------------- #
#  print_failures
# --------------------------------------------------------------------------- #


class TestPrintFailures:
    def test_print_failures_empty(self, capsys):
        print_failures([])

        captured = capsys.readouterr().out
        assert "No failures detected" in captured

    def test_print_failures_with_items(self, capsys):
        failed = [
            {
                "object_name": "catalog.schema.bad_table",
                "object_type": "managed_table",
                "status": "failed",
                "error_message": "Permission denied",
            },
            {
                "object_name": "catalog.schema.bad_view",
                "object_type": "view",
                "status": "validation_failed",
                "error_message": "Row count mismatch",
            },
        ]

        print_failures(failed)

        captured = capsys.readouterr().out
        assert "catalog.schema.bad_table" in captured
        assert "Permission denied" in captured
        assert "catalog.schema.bad_view" in captured
        assert "Row count mismatch" in captured
        assert "2 object(s) failed" in captured


# --------------------------------------------------------------------------- #
#  aggregate_by_status
# --------------------------------------------------------------------------- #


class TestAggregateByStatus:
    def test_aggregate_by_status(self):
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"status": "validated", "total": 5}
        mock_df.groupBy.return_value.agg.return_value.orderBy.return_value.collect.return_value = [mock_row]

        result = aggregate_by_status(mock_df)

        assert len(result) == 1
        assert result[0] == {"status": "validated", "total": 5}
        mock_df.groupBy.assert_called_once_with("status")


# --------------------------------------------------------------------------- #
#  get_failed_objects
# --------------------------------------------------------------------------- #


class TestGetFailedObjects:
    def test_get_failed_objects(self):
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_row.asDict.return_value = {
            "object_name": "catalog.schema.tbl",
            "object_type": "managed_table",
            "status": "failed",
            "error_message": "timeout",
        }
        mock_df.filter.return_value.select.return_value.orderBy.return_value.collect.return_value = [mock_row]

        result = get_failed_objects(mock_df)

        assert len(result) == 1
        assert result[0]["object_name"] == "catalog.schema.tbl"
        assert result[0]["error_message"] == "timeout"
        mock_df.filter.assert_called_once()


# --------------------------------------------------------------------------- #
#  aggregate_by_status_filtered
# --------------------------------------------------------------------------- #


class TestAggregateByStatusFiltered:
    """summary's aggregate_by_status_filtered must only count rows whose
    object_type is in the supplied object_types list (per-workflow slicing)."""

    def test_filters_by_object_types_when_list_provided(self):
        # When object_types is non-empty, df.filter must be invoked before
        # the groupBy/agg/orderBy chain runs on the *filtered* DataFrame.
        mock_df = MagicMock()
        filtered_df = MagicMock()
        mock_df.filter.return_value = filtered_df

        mock_row = MagicMock()
        mock_row.asDict.return_value = {"status": "validated", "total": 2}
        filtered_df.groupBy.return_value.agg.return_value.orderBy.return_value.collect.return_value = [mock_row]

        result = aggregate_by_status_filtered(mock_df, object_types=["managed_table"])

        assert result == [{"status": "validated", "total": 2}]
        mock_df.filter.assert_called_once()
        filtered_df.groupBy.assert_called_once_with("status")

    def test_governance_slice_returns_filtered_rows(self):
        mock_df = MagicMock()
        filtered_df = MagicMock()
        mock_df.filter.return_value = filtered_df

        validated_row = MagicMock()
        validated_row.asDict.return_value = {"status": "validated", "total": 1}
        skipped_row = MagicMock()
        skipped_row.asDict.return_value = {"status": "skipped", "total": 1}
        filtered_df.groupBy.return_value.agg.return_value.orderBy.return_value.collect.return_value = [
            skipped_row,
            validated_row,
        ]

        result = aggregate_by_status_filtered(mock_df, object_types=["tag", "row_filter"])

        statuses = {r["status"]: r["total"] for r in result}
        assert statuses == {"validated": 1, "skipped": 1}
        mock_df.filter.assert_called_once()

    def test_empty_object_types_skips_filter_back_compat(self):
        # Empty list = no filter (back-compat with pre-split behaviour).
        mock_df = MagicMock()

        v_row = MagicMock()
        v_row.asDict.return_value = {"status": "validated", "total": 4}
        s_row = MagicMock()
        s_row.asDict.return_value = {"status": "skipped", "total": 1}
        mock_df.groupBy.return_value.agg.return_value.orderBy.return_value.collect.return_value = [s_row, v_row]

        result = aggregate_by_status_filtered(mock_df, object_types=[])

        statuses = {r["status"]: r["total"] for r in result}
        assert statuses == {"validated": 4, "skipped": 1}
        mock_df.filter.assert_not_called()
        mock_df.groupBy.assert_called_once_with("status")
