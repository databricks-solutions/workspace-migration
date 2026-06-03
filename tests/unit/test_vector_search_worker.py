"""Unit tests for the Vector Search migration worker."""

from __future__ import annotations

from unittest.mock import MagicMock

from migrate.vector_search_worker import _build_delta_sync_spec, _is_delta_sync


def _delta_sync_definition():
    return {
        "index_type": "DELTA_SYNC",
        "endpoint_name": "ep1",
        "primary_key": "id",
        "delta_sync_index_spec": {
            "source_table": "cat.sch.src",
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {"name": "text", "embedding_model_endpoint_name": "databricks-gte-large-en"}
            ],
            "pipeline_id": "pl-123",  # response-only, must be dropped on create
        },
    }


class TestClassify:
    def test_is_delta_sync_true(self):
        assert _is_delta_sync(_delta_sync_definition()) is True

    def test_is_delta_sync_false_for_direct_access(self):
        assert _is_delta_sync({"index_type": "DIRECT_ACCESS"}) is False

    def test_is_delta_sync_false_when_missing(self):
        assert _is_delta_sync({}) is False


class TestBuildSpec:
    def test_builds_request_from_definition_and_drops_pipeline_id(self):
        spec = _build_delta_sync_spec(_delta_sync_definition())
        assert spec.source_table == "cat.sch.src"
        # pipeline_type parsed into the SDK enum
        assert str(spec.pipeline_type).endswith("TRIGGERED")
        assert spec.embedding_source_columns[0].name == "text"
        assert (
            spec.embedding_source_columns[0].embedding_model_endpoint_name
            == "databricks-gte-large-en"
        )
        # round-tripping the request must not carry the response-only pipeline_id
        assert "pipeline_id" not in spec.as_dict()
