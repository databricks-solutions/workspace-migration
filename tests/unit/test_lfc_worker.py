import json
from unittest.mock import MagicMock
from migrate.lfc_worker import migrate_pipeline

DEF = {"spec": {"ingestion_definition": {"connection_name": "src_pg", "objects": [
    {"table": {"source_catalog": "pg", "source_schema": "public", "source_table": "orders",
               "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "orders",
               "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["order_id"],
                                       "cursor_column": "updated_at"}}}]}}}

def _row():
    return {"object_name": "lfc_orders", "object_type": "lfc_pipeline",
            "metadata_json": json.dumps({"definition": DEF})}

def test_migrate_query_based_pipeline_happy_path():
    deps = MagicMock()
    deps.compute_boundary.return_value = "2026-06-10T00:00:00"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_view_exists.return_value = False
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="tgt_pg")
    statuses = {r["status"] for r in results}
    assert "lfc_pipeline_created_incremental" in statuses
    assert "lfc_view_created" in statuses
    spec = deps.create_pipeline.call_args.args[0]
    assert spec["ingestion_definition"]["objects"][0]["table"]["table_configuration"]["row_filter"] \
        == "updated_at >= '2026-06-10T00:00:00'"

def test_non_query_based_pipeline_is_skipped_this_stage():
    cdc = {"object_name": "p", "object_type": "lfc_pipeline",
           "metadata_json": json.dumps({"definition": {"spec": {"ingestion_definition": {"ingestion_gateway_id": "g"}}}})}
    assert migrate_pipeline(cdc, deps=MagicMock(), target_connection_name="t") == []

def test_idempotent_when_view_exists():
    deps = MagicMock()
    deps.target_view_exists.return_value = True
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="t")
    assert all(r["status"] == "skipped_target_pipeline_exists" for r in results)
    deps.create_pipeline.assert_not_called()
