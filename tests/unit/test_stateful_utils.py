"""Unit tests for StatefulExplorer (stateful-service discovery helpers)."""

from __future__ import annotations

from unittest.mock import MagicMock

from common.stateful_utils import CAPABILITY, StatefulExplorer


def _sdk_obj(as_dict=None, **attrs):
    """A stand-in for a databricks-sdk dataclass: attributes + .as_dict()."""
    obj = MagicMock()
    for k, v in attrs.items():
        setattr(obj, k, v)
    obj.as_dict.return_value = as_dict if as_dict is not None else dict(attrs)
    return obj


class TestScaffold:
    def test_capability_map_covers_every_stateful_object_type(self):
        assert CAPABILITY == {
            "vector_search_index": "vector",
            "app": "compute",
            "database_instance": "lakebase",
            "synced_table": "lakebase",
            "model_serving_endpoint": "compute",
            "lfc_pipeline": "ingestion",
            "online_table": "online_store",
        }

    def test_safe_returns_empty_and_warns_on_error(self, capsys):
        explorer = StatefulExplorer(MagicMock())

        def _boom():
            raise PermissionError("nope")

        result = explorer._safe("vector search", _boom)
        assert result == []
        out = capsys.readouterr().out
        assert "[stateful][warn]" in out
        assert "vector search" in out

    def test_safe_passes_through_on_success(self):
        explorer = StatefulExplorer(MagicMock())
        assert explorer._safe("x", lambda: [1, 2]) == [1, 2]


class TestVectorSearch:
    def test_lists_indexes_across_endpoints_with_full_spec(self):
        auth = MagicMock()
        client = auth.source_client
        client.vector_search_endpoints.list_endpoints.return_value = [
            _sdk_obj(name="ep1")
        ]
        client.vector_search_indexes.list_indexes.return_value = [
            _sdk_obj(name="cat.sch.idx")
        ]
        client.vector_search_indexes.get_index.return_value = _sdk_obj(
            as_dict={"name": "cat.sch.idx", "primary_key": "id",
                     "delta_sync_index_spec": {"source_table": "cat.sch.src"}},
            name="cat.sch.idx",
        )

        rows = StatefulExplorer(auth).list_vector_search_indexes()

        assert len(rows) == 1
        assert rows[0]["index_name"] == "cat.sch.idx"
        assert rows[0]["endpoint_name"] == "ep1"
        assert rows[0]["definition"]["delta_sync_index_spec"]["source_table"] == "cat.sch.src"
        client.vector_search_indexes.list_indexes.assert_called_once_with(endpoint_name="ep1")

    def test_falls_back_to_mini_when_get_index_fails(self):
        auth = MagicMock()
        client = auth.source_client
        client.vector_search_endpoints.list_endpoints.return_value = [_sdk_obj(name="ep1")]
        client.vector_search_indexes.list_indexes.return_value = [
            _sdk_obj(as_dict={"name": "cat.sch.idx"}, name="cat.sch.idx")
        ]
        client.vector_search_indexes.get_index.side_effect = RuntimeError("boom")

        rows = StatefulExplorer(auth).list_vector_search_indexes()
        assert rows[0]["definition"] == {"name": "cat.sch.idx"}
        assert rows[0]["index_name"] == "cat.sch.idx"
        assert rows[0]["endpoint_name"] == "ep1"

    def test_returns_empty_and_warns_when_vs_not_enabled(self, capsys):
        auth = MagicMock()
        auth.source_client.vector_search_endpoints.list_endpoints.side_effect = Exception("404")
        rows = StatefulExplorer(auth).list_vector_search_indexes()
        assert rows == []
        assert "vector search" in capsys.readouterr().out
