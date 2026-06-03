"""Unit tests for the Online Tables pre-check source-table gate."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from pre_check.pre_check_online_tables import find_missing_source_tables


def _row(source_table):
    return {"object_name": "cat.sch.ot", "object_type": "online_table",
            "metadata_json": json.dumps({"definition": {"spec": {"source_table_full_name": source_table}}})}


def test_missing_source_table_is_reported():
    client = MagicMock()
    client.tables.get.side_effect = Exception("TABLE_DOES_NOT_EXIST")
    assert find_missing_source_tables(client, [_row("cat.sch.src")]) == ["cat.sch.src"]


def test_present_source_table_is_ok():
    client = MagicMock()
    client.tables.get.return_value = MagicMock()
    assert find_missing_source_tables(client, [_row("cat.sch.src")]) == []


def test_row_without_source_table_is_skipped():
    client = MagicMock()
    row = {"object_name": "x", "object_type": "online_table",
           "metadata_json": json.dumps({"definition": {"spec": {}}})}
    assert find_missing_source_tables(client, [row]) == []
    client.tables.get.assert_not_called()
