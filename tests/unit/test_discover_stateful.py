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


def test_discover_uc_online_table_reclassified_as_stateful():
    """online_table rows emitted by _discover_uc must be tagged
    source_type='stateful' with capability='online_store' preserved in
    metadata_json, and the original online_table fields must survive."""
    explorer = MagicMock()
    # Empty catalog list so the per-catalog loop is skipped entirely.
    explorer.list_catalogs.return_value = []
    explorer.list_foreign_catalog_names.return_value = set()

    # The one online_table entry under test.
    explorer.list_online_tables.return_value = [
        {
            "online_table_fqn": "cat.s.ot",
            "source_table_fqn": "cat.s.src",
            "definition": {"k": 1},
        }
    ]

    # Workspace-level list_* methods that _discover_uc always calls.
    explorer.list_monitors.return_value = []
    explorer.list_policies.return_value = []
    explorer.list_connections.return_value = []
    explorer.list_foreign_catalogs.return_value = []
    explorer.list_shares.return_value = []
    explorer.list_recipients.return_value = []
    explorer.list_providers.return_value = []

    config = MagicMock()
    config.catalog_filter = []
    config.schema_filter = []
    config.tracking_catalog = "_migration_tracking"
    config.rls_cm_strategy = ""

    rows, _dlt = disc._discover_uc(config, explorer, _now())

    ot_rows = [r for r in rows if r["object_type"] == "online_table"]
    assert len(ot_rows) == 1, f"expected exactly 1 online_table row, got {len(ot_rows)}"

    ot = ot_rows[0]
    assert ot["source_type"] == "stateful"

    meta = json.loads(ot["metadata_json"])
    assert meta["capability"] == "online_store"
    assert meta["source_table_fqn"] == "cat.s.src"
