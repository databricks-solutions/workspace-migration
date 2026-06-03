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
