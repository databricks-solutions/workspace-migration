"""Unit tests for the Online Tables migration worker."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from databricks.sdk.errors import AlreadyExists

from migrate.online_tables_worker import _build_online_table_spec, migrate_online_table


def _definition():
    return {
        "name": "cat.sch.ot",
        "spec": {
            "source_table_full_name": "cat.sch.src",
            "primary_key_columns": ["id"],
            "run_triggered": {},
            "pipeline_id": "pl-123",
        },
    }


def _row(definition):
    return {"object_name": "cat.sch.ot", "object_type": "online_table",
            "metadata_json": json.dumps({"definition": definition})}


class TestBuildSpec:
    def test_builds_spec_and_drops_pipeline_id(self):
        spec = _build_online_table_spec(_definition())
        assert spec.source_table_full_name == "cat.sch.src"
        assert spec.primary_key_columns == ["id"]
        assert spec.run_triggered is not None
        assert "pipeline_id" not in spec.as_dict()


class TestMigrateOnlineTable:
    def test_created_resync_pending_and_object_name_is_fqn(self):
        client = MagicMock()
        res = migrate_online_table(client, _row(_definition()))
        assert res["status"] == "created_resync_pending"
        assert res["object_name"] == "cat.sch.ot"
        assert res["object_type"] == "online_table"
        ot_arg = client.online_tables.create.call_args.args[0]
        assert ot_arg.name == "cat.sch.ot"
        assert ot_arg.spec.source_table_full_name == "cat.sch.src"

    def test_already_exists_is_skipped_target_exists(self):
        client = MagicMock()
        client.online_tables.create.side_effect = AlreadyExists("exists")
        res = migrate_online_table(client, _row(_definition()))
        assert res["status"] == "skipped_target_exists"

    def test_create_failure_is_failed(self):
        client = MagicMock()
        client.online_tables.create.side_effect = Exception("boom quota")
        res = migrate_online_table(client, _row(_definition()))
        assert res["status"] == "failed"
        assert "boom" in res["error_message"]

    def test_missing_spec_is_failed_not_raised(self):
        client = MagicMock()
        row = {"object_name": "cat.sch.ot", "object_type": "online_table",
               "metadata_json": json.dumps({"definition": {"name": "cat.sch.ot"}})}
        res = migrate_online_table(client, row)
        assert res["status"] == "failed"
        client.online_tables.create.assert_not_called()

    def test_malformed_metadata_is_failed_not_raised(self):
        client = MagicMock()
        row = {"object_name": "cat.sch.ot", "object_type": "online_table", "metadata_json": "{not json"}
        res = migrate_online_table(client, row)
        assert res["status"] == "failed"
        client.online_tables.create.assert_not_called()
