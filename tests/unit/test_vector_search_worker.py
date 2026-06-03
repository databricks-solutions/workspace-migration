"""Unit tests for the Vector Search migration worker."""

from __future__ import annotations

import json
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


class TestEnsureEndpoint:
    def _ep(self, state):
        ep = MagicMock()
        ep.endpoint_status.state = state
        return ep

    def test_existing_online_endpoint_is_ready_no_create(self):
        from migrate.vector_search_worker import _ensure_endpoint

        client = MagicMock()
        client.vector_search_endpoints.get_endpoint.return_value = self._ep("ONLINE")
        ready = _ensure_endpoint(client, "ep1", "STANDARD", max_attempts=1, sleep_seconds=0, sleep_fn=lambda s: None)
        assert ready is True
        client.vector_search_endpoints.create_endpoint.assert_not_called()

    def test_missing_endpoint_is_created_then_becomes_ready(self):
        from migrate.vector_search_worker import _ensure_endpoint

        client = MagicMock()
        client.vector_search_endpoints.get_endpoint.side_effect = [
            Exception("not found"),
            self._ep("PROVISIONING"),
            self._ep("ONLINE"),
        ]
        ready = _ensure_endpoint(client, "ep1", "STANDARD", max_attempts=5, sleep_seconds=0, sleep_fn=lambda s: None)
        assert ready is True
        client.vector_search_endpoints.create_endpoint.assert_called_once()

    def test_endpoint_never_ready_returns_false(self):
        from migrate.vector_search_worker import _ensure_endpoint

        client = MagicMock()
        client.vector_search_endpoints.get_endpoint.side_effect = [Exception("nf")] + [self._ep("PROVISIONING")] * 3
        ready = _ensure_endpoint(client, "ep1", "STANDARD", max_attempts=3, sleep_seconds=0, sleep_fn=lambda s: None)
        assert ready is False

    def test_already_exists_on_create_falls_through_to_poll(self):
        from databricks.sdk.errors import AlreadyExists

        from migrate.vector_search_worker import _ensure_endpoint

        client = MagicMock()
        # get fails first (transient), create races into AlreadyExists, then polls ONLINE
        client.vector_search_endpoints.get_endpoint.side_effect = [Exception("nf"), self._ep("ONLINE")]
        client.vector_search_endpoints.create_endpoint.side_effect = AlreadyExists("exists")
        ready = _ensure_endpoint(client, "ep1", "STANDARD", max_attempts=2, sleep_seconds=0, sleep_fn=lambda s: None)
        assert ready is True

    def test_blank_endpoint_type_defaults_to_standard(self):
        from databricks.sdk.service.vectorsearch import EndpointType

        from migrate.vector_search_worker import _ensure_endpoint

        client = MagicMock()
        client.vector_search_endpoints.get_endpoint.side_effect = [Exception("nf"), self._ep("ONLINE")]
        _ensure_endpoint(client, "ep1", "", max_attempts=2, sleep_seconds=0, sleep_fn=lambda s: None)
        kwargs = client.vector_search_endpoints.create_endpoint.call_args.kwargs
        assert kwargs["endpoint_type"] == EndpointType.STANDARD


class TestMigrateIndex:
    def _row(self, definition):
        return {"object_name": "cat.sch.idx", "object_type": "vector_search_index",
                "metadata_json": json.dumps({"definition": definition})}

    def _delta_def(self):
        return {
            "index_type": "DELTA_SYNC", "endpoint_name": "ep1", "primary_key": "id",
            "delta_sync_index_spec": {
                "source_table": "cat.sch.src", "pipeline_type": "TRIGGERED",
                "embedding_source_columns": [
                    {"name": "t", "embedding_model_endpoint_name": "databricks-gte-large-en"},
                ],
            },
        }

    def test_direct_access_skipped(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        res = migrate_index(client, self._row({"index_type": "DIRECT_ACCESS"}),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_direct_access_unsupported"
        client.vector_search_indexes.create_index.assert_not_called()

    def test_delta_sync_created_resync_pending(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        ep = MagicMock()
        ep.endpoint_status.state = "ONLINE"
        client.vector_search_endpoints.get_endpoint.return_value = ep
        res = migrate_index(client, self._row(self._delta_def()),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "created_resync_pending"
        kwargs = client.vector_search_indexes.create_index.call_args.kwargs
        assert kwargs["name"] == "cat.sch.idx"
        assert kwargs["endpoint_name"] == "ep1"
        assert kwargs["primary_key"] == "id"
        assert str(kwargs["index_type"]).endswith("DELTA_SYNC")
        assert kwargs["delta_sync_index_spec"].source_table == "cat.sch.src"

    def test_endpoint_not_ready_defers(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        client.vector_search_endpoints.get_endpoint.side_effect = Exception("nf")
        res = migrate_index(client, self._row(self._delta_def()),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_endpoint_not_ready"
        client.vector_search_indexes.create_index.assert_not_called()

    def test_already_exists_is_skipped_target_exists(self):
        from databricks.sdk.errors import AlreadyExists

        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        ep = MagicMock()
        ep.endpoint_status.state = "ONLINE"
        client.vector_search_endpoints.get_endpoint.return_value = ep
        client.vector_search_indexes.create_index.side_effect = AlreadyExists("index exists")
        res = migrate_index(client, self._row(self._delta_def()),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_target_exists"

    def test_create_failure_is_failed(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        ep = MagicMock()
        ep.endpoint_status.state = "ONLINE"
        client.vector_search_endpoints.get_endpoint.return_value = ep
        client.vector_search_indexes.create_index.side_effect = Exception("boom quota exceeded")
        res = migrate_index(client, self._row(self._delta_def()),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        assert "boom" in res["error_message"]

    def test_missing_definition_is_failed(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        row = {"object_name": "cat.sch.idx", "object_type": "vector_search_index",
               "metadata_json": json.dumps({})}  # no "definition"
        res = migrate_index(client, row, sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        client.vector_search_indexes.create_index.assert_not_called()

    def test_malformed_metadata_json_is_failed_not_raised(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        row = {"object_name": "cat.sch.idx", "object_type": "vector_search_index",
               "metadata_json": "{not valid json"}
        res = migrate_index(client, row, sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        client.vector_search_indexes.create_index.assert_not_called()


class TestRun:
    def test_run_reads_list_migrates_and_records(self, monkeypatch):
        import migrate.vector_search_worker as w

        monkeypatch.setattr(w.MigrationConfig, "from_workspace_file", staticmethod(lambda: MagicMock()))
        fake_auth = MagicMock()
        monkeypatch.setattr(w, "AuthManager", lambda *a, **k: fake_auth)
        tracker = MagicMock()
        monkeypatch.setattr(w, "TrackingManager", lambda *a, **k: tracker)

        definition = {"index_type": "DELTA_SYNC", "endpoint_name": "ep1", "primary_key": "id",
                      "delta_sync_index_spec": {"source_table": "cat.sch.src", "pipeline_type": "TRIGGERED"}}
        row = {"object_name": "cat.sch.idx", "object_type": "vector_search_index",
               "metadata_json": json.dumps({"definition": definition})}
        dbutils = MagicMock()
        dbutils.jobs.taskValues.get.return_value = json.dumps([row])

        ep = MagicMock()
        ep.endpoint_status.state = "ONLINE"
        fake_auth.target_client.vector_search_endpoints.get_endpoint.return_value = ep

        monkeypatch.setattr(w.time, "sleep", lambda s: None)

        w.run(dbutils, MagicMock())

        recorded = tracker.append_migration_status.call_args.args[0]
        assert len(recorded) == 1
        assert recorded[0]["object_name"] == "cat.sch.idx"
        assert recorded[0]["status"] == "created_resync_pending"

    def test_run_empty_list_records_nothing(self, monkeypatch):
        import migrate.vector_search_worker as w
        monkeypatch.setattr(w.MigrationConfig, "from_workspace_file", staticmethod(lambda: MagicMock()))
        monkeypatch.setattr(w, "AuthManager", lambda *a, **k: MagicMock())
        tracker = MagicMock()
        monkeypatch.setattr(w, "TrackingManager", lambda *a, **k: tracker)
        dbutils = MagicMock()
        dbutils.jobs.taskValues.get.return_value = json.dumps([])

        w.run(dbutils, MagicMock())
        tracker.append_migration_status.assert_not_called()

    def test_run_isolates_per_index_outcomes(self, monkeypatch):
        import migrate.vector_search_worker as w

        monkeypatch.setattr(w.MigrationConfig, "from_workspace_file", staticmethod(lambda: MagicMock()))
        fake_auth = MagicMock()
        monkeypatch.setattr(w, "AuthManager", lambda *a, **k: fake_auth)
        tracker = MagicMock()
        monkeypatch.setattr(w, "TrackingManager", lambda *a, **k: tracker)

        delta = {"index_type": "DELTA_SYNC", "endpoint_name": "ep1", "primary_key": "id",
                 "delta_sync_index_spec": {"source_table": "cat.sch.src", "pipeline_type": "TRIGGERED"}}
        direct = {"index_type": "DIRECT_ACCESS"}
        rows = [
            {"object_name": "cat.sch.direct", "object_type": "vector_search_index",
             "metadata_json": json.dumps({"definition": direct})},
            {"object_name": "cat.sch.delta", "object_type": "vector_search_index",
             "metadata_json": json.dumps({"definition": delta})},
        ]
        dbutils = MagicMock()
        dbutils.jobs.taskValues.get.return_value = json.dumps(rows)
        ep = MagicMock()
        ep.endpoint_status.state = "ONLINE"
        fake_auth.target_client.vector_search_endpoints.get_endpoint.return_value = ep
        monkeypatch.setattr(w.time, "sleep", lambda s: None)

        w.run(dbutils, MagicMock())

        recorded = tracker.append_migration_status.call_args.args[0]
        by_name = {r["object_name"]: r["status"] for r in recorded}
        assert by_name["cat.sch.direct"] == "skipped_direct_access_unsupported"
        assert by_name["cat.sch.delta"] == "created_resync_pending"

    def test_run_malformed_row_does_not_abort_batch(self, monkeypatch):
        import migrate.vector_search_worker as w

        monkeypatch.setattr(w.MigrationConfig, "from_workspace_file", staticmethod(lambda: MagicMock()))
        fake_auth = MagicMock()
        monkeypatch.setattr(w, "AuthManager", lambda *a, **k: fake_auth)
        tracker = MagicMock()
        monkeypatch.setattr(w, "TrackingManager", lambda *a, **k: tracker)

        good = {"index_type": "DELTA_SYNC", "endpoint_name": "ep1", "primary_key": "id",
                "delta_sync_index_spec": {"source_table": "cat.sch.src", "pipeline_type": "TRIGGERED"}}
        rows = [
            {"object_name": "cat.sch.bad", "object_type": "vector_search_index", "metadata_json": "{not json"},
            {"object_name": "cat.sch.good", "object_type": "vector_search_index",
             "metadata_json": json.dumps({"definition": good})},
        ]
        dbutils = MagicMock()
        dbutils.jobs.taskValues.get.return_value = json.dumps(rows)
        ep = MagicMock()
        ep.endpoint_status.state = "ONLINE"
        fake_auth.target_client.vector_search_endpoints.get_endpoint.return_value = ep
        monkeypatch.setattr(w.time, "sleep", lambda s: None)

        w.run(dbutils, MagicMock())

        recorded = {r["object_name"]: r["status"] for r in tracker.append_migration_status.call_args.args[0]}
        assert recorded["cat.sch.bad"] == "failed"
        assert recorded["cat.sch.good"] == "created_resync_pending"
