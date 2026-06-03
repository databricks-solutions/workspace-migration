"""Unit tests for the Online Tables -> Lakebase synced table migration worker."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from databricks.sdk.errors import AlreadyExists

from migrate.online_tables_worker import (
    _build_synced_table_spec,
    _scheduling_policy,
    migrate_online_table,
)


def _definition(mode="run_triggered"):
    spec = {"source_table_full_name": "cat.sch.src", "primary_key_columns": ["id"], "pipeline_id": "pl-1"}
    spec[mode] = True if mode == "perform_full_copy" else {}
    return {"name": "cat.sch.ot", "spec": spec}


def _row(definition):
    return {"object_name": "cat.sch.ot", "object_type": "online_table",
            "metadata_json": json.dumps({"definition": definition})}


def _config():
    c = MagicMock()
    c.lakebase_instance_name = "lb1"
    c.lakebase_logical_database = "ldb"
    c.lakebase_capacity = "CU_1"
    return c


def _ready_instance():
    inst = MagicMock()
    inst.state = "AVAILABLE"
    return inst


class TestSchedulingPolicy:
    def test_continuous(self):
        from databricks.sdk.service.database import SyncedTableSchedulingPolicy
        assert _scheduling_policy({"run_continuously": {}}) == SyncedTableSchedulingPolicy.CONTINUOUS

    def test_snapshot(self):
        from databricks.sdk.service.database import SyncedTableSchedulingPolicy
        assert _scheduling_policy({"perform_full_copy": True}) == SyncedTableSchedulingPolicy.SNAPSHOT

    def test_triggered_default(self):
        from databricks.sdk.service.database import SyncedTableSchedulingPolicy
        assert _scheduling_policy({"run_triggered": {}}) == SyncedTableSchedulingPolicy.TRIGGERED
        assert _scheduling_policy({}) == SyncedTableSchedulingPolicy.TRIGGERED


class TestBuildSpec:
    def test_builds_spec_from_definition(self):
        spec = _build_synced_table_spec(_definition())
        assert spec.source_table_full_name == "cat.sch.src"
        assert spec.primary_key_columns == ["id"]
        assert spec.scheduling_policy is not None


class TestMigrate:
    def test_created_resync_pending_and_fqn(self):
        client = MagicMock()
        client.database.get_database_instance.return_value = _ready_instance()
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "created_resync_pending"
        assert res["object_name"] == "cat.sch.ot"
        st = client.database.create_synced_database_table.call_args.args[0]
        assert st.name == "cat.sch.ot"
        assert st.database_instance_name == "lb1"
        assert st.logical_database_name == "ldb"
        assert st.spec.source_table_full_name == "cat.sch.src"

    def test_instance_created_if_missing(self):
        client = MagicMock()
        client.database.get_database_instance.side_effect = [Exception("nf"), _ready_instance()]
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=3, sleep_seconds=0)
        assert res["status"] == "created_resync_pending"
        client.database.create_database_instance.assert_called_once()

    def test_instance_not_ready_defers(self):
        client = MagicMock()
        client.database.get_database_instance.side_effect = Exception("nf")
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_instance_not_ready"
        client.database.create_synced_database_table.assert_not_called()

    def test_already_exists(self):
        client = MagicMock()
        client.database.get_database_instance.return_value = _ready_instance()
        client.database.create_synced_database_table.side_effect = AlreadyExists("exists")
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_target_exists"

    def test_create_failure_is_failed(self):
        client = MagicMock()
        client.database.get_database_instance.return_value = _ready_instance()
        client.database.create_synced_database_table.side_effect = Exception("no primary key")
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        assert "primary key" in res["error_message"]

    def test_missing_source_is_failed(self):
        client = MagicMock()
        row = {"object_name": "cat.sch.ot", "object_type": "online_table",
               "metadata_json": json.dumps({"definition": {"name": "cat.sch.ot", "spec": {}}})}
        res = migrate_online_table(client, row, _config(), sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        client.database.create_synced_database_table.assert_not_called()

    def test_malformed_metadata_is_failed(self):
        client = MagicMock()
        row = {"object_name": "cat.sch.ot", "object_type": "online_table", "metadata_json": "{bad"}
        res = migrate_online_table(client, row, _config(), sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        client.database.create_synced_database_table.assert_not_called()
