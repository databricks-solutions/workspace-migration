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

    def test_one_bad_endpoint_does_not_zero_other_endpoints(self, capsys):
        auth = MagicMock()
        client = auth.source_client
        client.vector_search_endpoints.list_endpoints.return_value = [
            _sdk_obj(name="bad_ep"), _sdk_obj(name="good_ep")
        ]

        def _list_indexes(endpoint_name):
            if endpoint_name == "bad_ep":
                raise RuntimeError("endpoint unhealthy")
            return [_sdk_obj(name="cat.s.idx")]

        client.vector_search_indexes.list_indexes.side_effect = _list_indexes
        client.vector_search_indexes.get_index.return_value = _sdk_obj(
            as_dict={"name": "cat.s.idx"}, name="cat.s.idx"
        )

        rows = StatefulExplorer(auth).list_vector_search_indexes()
        assert [r["index_name"] for r in rows] == ["cat.s.idx"]
        assert "bad_ep" in capsys.readouterr().out


class TestApps:
    def test_lists_apps_with_full_spec(self):
        auth = MagicMock()
        auth.source_client.apps.list.return_value = [
            _sdk_obj(
                as_dict={"name": "myapp", "resources": [{"database": {"instance_name": "lb1"}}]},
                name="myapp",
            )
        ]
        rows = StatefulExplorer(auth).list_apps()
        assert len(rows) == 1
        assert rows[0]["app_name"] == "myapp"
        assert rows[0]["definition"]["resources"][0]["database"]["instance_name"] == "lb1"

    def test_returns_empty_and_warns_when_apps_unavailable(self, capsys):
        auth = MagicMock()
        auth.source_client.apps.list.side_effect = Exception("403")
        assert StatefulExplorer(auth).list_apps() == []
        assert "apps" in capsys.readouterr().out


class TestLakebase:
    def test_lists_database_instances(self):
        auth = MagicMock()
        auth.source_client.database.list_database_instances.return_value = [
            _sdk_obj(as_dict={"name": "lb1", "pg_version": "16"}, name="lb1")
        ]
        rows = StatefulExplorer(auth).list_database_instances()
        assert rows == [{"instance_name": "lb1", "definition": {"name": "lb1", "pg_version": "16"}}]

    def test_lists_synced_tables_per_instance(self):
        auth = MagicMock()
        client = auth.source_client
        client.database.list_database_instances.return_value = [_sdk_obj(name="lb1")]
        client.database.list_synced_database_tables.return_value = [
            _sdk_obj(
                as_dict={"name": "cat.sch.synced", "database_instance_name": "lb1"},
                name="cat.sch.synced",
                database_instance_name="lb1",
            )
        ]
        rows = StatefulExplorer(auth).list_synced_tables()
        assert len(rows) == 1
        assert rows[0]["synced_table_name"] == "cat.sch.synced"
        assert rows[0]["instance_name"] == "lb1"
        assert rows[0]["definition"]["database_instance_name"] == "lb1"
        client.database.list_synced_database_tables.assert_called_once_with(instance_name="lb1")

    def test_lakebase_surfaces_warn_and_empty_when_unavailable(self, capsys):
        auth = MagicMock()
        auth.source_client.database.list_database_instances.side_effect = Exception("404")
        assert StatefulExplorer(auth).list_database_instances() == []
        assert StatefulExplorer(auth).list_synced_tables() == []
        assert "lakebase" in capsys.readouterr().out

    def test_one_bad_instance_does_not_zero_other_instances(self, capsys):
        auth = MagicMock()
        client = auth.source_client
        client.database.list_database_instances.return_value = [
            _sdk_obj(name="bad_lb"), _sdk_obj(name="good_lb")
        ]

        def _list_synced(instance_name):
            if instance_name == "bad_lb":
                raise RuntimeError("instance unhealthy")
            return [_sdk_obj(as_dict={"name": "cat.s.syn"}, name="cat.s.syn")]

        client.database.list_synced_database_tables.side_effect = _list_synced

        rows = StatefulExplorer(auth).list_synced_tables()
        assert [r["synced_table_name"] for r in rows] == ["cat.s.syn"]
        assert "bad_lb" in capsys.readouterr().out


class TestServing:
    def test_lists_serving_endpoints_with_full_config(self):
        auth = MagicMock()
        auth.source_client.serving_endpoints.list.return_value = [
            _sdk_obj(
                as_dict={"name": "ep", "config": {"served_entities": [{"entity_name": "cat.sch.model"}]}},
                name="ep",
            )
        ]
        rows = StatefulExplorer(auth).list_model_serving_endpoints()
        assert len(rows) == 1
        assert rows[0]["endpoint_name"] == "ep"
        assert rows[0]["definition"]["config"]["served_entities"][0]["entity_name"] == "cat.sch.model"

    def test_returns_empty_and_warns_when_unavailable(self, capsys):
        auth = MagicMock()
        auth.source_client.serving_endpoints.list.side_effect = Exception("403")
        assert StatefulExplorer(auth).list_model_serving_endpoints() == []
        assert "serving" in capsys.readouterr().out


class TestLfcPipelines:
    def test_keeps_only_ingestion_pipelines(self):
        auth = MagicMock()
        client = auth.source_client
        client.pipelines.list_pipelines.return_value = [
            _sdk_obj(pipeline_id="p1", name="lfc_one"),
            _sdk_obj(pipeline_id="p2", name="plain_dlt"),
        ]

        def _get(pipeline_id):
            if pipeline_id == "p1":
                spec = MagicMock()
                spec.ingestion_definition = MagicMock()  # present -> LFC
                return _sdk_obj(
                    as_dict={"name": "lfc_one", "spec": {"ingestion_definition": {"connection_name": "sf_conn"}}},
                    name="lfc_one",
                    spec=spec,
                )
            spec = MagicMock()
            spec.ingestion_definition = None  # not LFC
            return _sdk_obj(as_dict={"name": "plain_dlt"}, name="plain_dlt", spec=spec)

        client.pipelines.get.side_effect = _get

        rows = StatefulExplorer(auth).list_lfc_pipelines()
        assert len(rows) == 1
        assert rows[0]["pipeline_name"] == "lfc_one"
        assert rows[0]["pipeline_id"] == "p1"
        assert rows[0]["definition"]["spec"]["ingestion_definition"]["connection_name"] == "sf_conn"

    def test_skips_pipeline_whose_get_fails_without_aborting(self, capsys):
        auth = MagicMock()
        client = auth.source_client
        client.pipelines.list_pipelines.return_value = [
            _sdk_obj(pipeline_id="bad", name="bad"),
            _sdk_obj(pipeline_id="p1", name="lfc_one"),
        ]

        def _get(pipeline_id):
            if pipeline_id == "bad":
                raise RuntimeError("gone")
            spec = MagicMock()
            spec.ingestion_definition = MagicMock()
            return _sdk_obj(as_dict={"name": "lfc_one"}, name="lfc_one", spec=spec)

        client.pipelines.get.side_effect = _get

        rows = StatefulExplorer(auth).list_lfc_pipelines()
        assert [r["pipeline_name"] for r in rows] == ["lfc_one"]
        assert "pipeline bad" in capsys.readouterr().out

    def test_returns_empty_and_warns_when_pipelines_unavailable(self, capsys):
        auth = MagicMock()
        auth.source_client.pipelines.list_pipelines.side_effect = Exception("403")
        assert StatefulExplorer(auth).list_lfc_pipelines() == []
        assert "lakeflow connect" in capsys.readouterr().out
