"""Unit tests for the Vector Search pre-check source-table gate."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from pre_check.pre_check_vector_search import find_missing_source_tables


def _delta_row(source_table):
    return {"object_name": "cat.sch.idx", "object_type": "vector_search_index",
            "metadata_json": json.dumps(
                {"definition": {"index_type": "DELTA_SYNC",
                                "delta_sync_index_spec": {"source_table": source_table}}})}


def _direct_row():
    return {"object_name": "cat.sch.da", "object_type": "vector_search_index",
            "metadata_json": json.dumps({"definition": {"index_type": "DIRECT_ACCESS"}})}


def test_missing_source_table_is_reported():
    client = MagicMock()
    client.tables.get.side_effect = Exception("TABLE_DOES_NOT_EXIST")
    missing = find_missing_source_tables(client, [_delta_row("cat.sch.src")])
    assert missing == ["cat.sch.src"]


def test_present_source_table_is_ok():
    client = MagicMock()
    client.tables.get.return_value = MagicMock()  # exists
    missing = find_missing_source_tables(client, [_delta_row("cat.sch.src")])
    assert missing == []


def test_direct_access_rows_are_excluded_from_source_check():
    client = MagicMock()
    missing = find_missing_source_tables(client, [_direct_row()])
    assert missing == []
    client.tables.get.assert_not_called()
