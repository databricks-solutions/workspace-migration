"""Unit tests for discovery._discover_stateful and online_table reclassification."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import discovery.discovery as disc


def _now():
    return datetime(2026, 6, 3, tzinfo=timezone.utc)


def test_discover_stateful_tags_rows_and_capability():
    stateful = MagicMock()
    stateful.list_vector_search_indexes.return_value = [
        {"index_name": "cat.s.idx", "endpoint_name": "ep", "definition": {"x": 1}}
    ]
    stateful.list_apps.return_value = [{"app_name": "app1", "definition": {"resources": []}}]
    stateful.list_database_instances.return_value = [{"instance_name": "lb1", "definition": {}}]
    stateful.list_synced_tables.return_value = [
        {"synced_table_name": "cat.s.syn", "instance_name": "lb1", "definition": {}}
    ]
    stateful.list_model_serving_endpoints.return_value = [{"endpoint_name": "ep1", "definition": {}}]
    stateful.list_lfc_pipelines.return_value = [
        {"pipeline_name": "lfc", "pipeline_id": "p1", "definition": {}}
    ]

    rows = disc._discover_stateful(MagicMock(), stateful, _now())

    by_type = {r["object_type"]: r for r in rows}
    assert set(by_type) == {
        "vector_search_index", "app", "database_instance",
        "synced_table", "model_serving_endpoint", "lfc_pipeline",
    }
    assert all(r["source_type"] == "stateful" for r in rows)
    assert by_type["vector_search_index"]["object_name"] == "cat.s.idx"
    assert by_type["lfc_pipeline"]["object_name"] == "lfc"
    meta = json.loads(by_type["database_instance"]["metadata_json"])
    assert meta["capability"] == "lakebase"
    assert json.loads(by_type["app"]["metadata_json"])["capability"] == "compute"
    assert json.loads(by_type["vector_search_index"]["metadata_json"])["definition"] == {"x": 1}


def test_discover_stateful_empty_when_all_surfaces_empty():
    stateful = MagicMock()
    for m in ("list_vector_search_indexes", "list_apps", "list_database_instances",
              "list_synced_tables", "list_model_serving_endpoints", "list_lfc_pipelines"):
        getattr(stateful, m).return_value = []
    assert disc._discover_stateful(MagicMock(), stateful, _now()) == []
