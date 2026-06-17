import json
from unittest.mock import MagicMock

from migrate.lfc_worker import migrate_pipeline

DEF = {"spec": {"catalog": "bronze", "schema": "pg", "ingestion_definition": {
    "connection_name": "src_pg", "source_type": "POSTGRESQL", "objects": [
    {"table": {"source_catalog": "pg", "source_schema": "public", "source_table": "orders",
               "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "orders",
               "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["order_id"],
                                       "query_based_connector_config": {"cursor_columns": ["updated_at"]}}}}]}}}

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
    cdc_def = {"spec": {"ingestion_definition": {"ingestion_gateway_id": "g"}}}
    cdc = {"object_name": "p", "object_type": "lfc_pipeline",
           "metadata_json": json.dumps({"definition": cdc_def})}
    assert migrate_pipeline(cdc, deps=MagicMock(), target_connection_name="t") == []

def test_idempotent_when_view_exists():
    deps = MagicMock()
    deps.target_view_exists.return_value = True
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="t")
    assert all(r["status"] == "skipped_target_pipeline_exists" for r in results)
    deps.create_pipeline.assert_not_called()


def test_batch_table_without_cursor_is_not_cloned_or_viewed():
    two_tables = {"spec": {"catalog": "bronze", "schema": "pg", "ingestion_definition": {
        "connection_name": "src_pg", "source_type": "POSTGRESQL", "objects": [
        {"table": {"source_catalog": "pg", "source_schema": "public", "source_table": "orders",
                   "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "orders",
                   "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["order_id"],
                                           "query_based_connector_config": {"cursor_columns": ["updated_at"]}}}},
        {"table": {"source_catalog": "pg", "source_schema": "public", "source_table": "regions",
                   "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "regions",
                   "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["region_id"]}}},
    ]}}}
    row = {"object_name": "lfc_two", "object_type": "lfc_pipeline",
           "metadata_json": json.dumps({"definition": two_tables})}
    deps = MagicMock()
    deps.compute_boundary.return_value = "2026-06-10T00:00:00"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_view_exists.return_value = False
    results = migrate_pipeline(row, deps=deps, target_connection_name="tgt_pg")
    # the no-cursor "regions" table is neither cloned nor viewed
    assert deps.clone_history.call_count == 1
    assert deps.create_view.call_count == 1
    lfc_tables = [r for r in results if r["object_type"] == "lfc_table"]
    lfc_views = [r for r in results if r["object_type"] == "lfc_view"]
    assert len(lfc_tables) == 1 and "orders_history" in lfc_tables[0]["object_name"]
    assert len(lfc_views) == 1 and lfc_views[0]["object_name"].endswith("orders")
    deps.create_pipeline.assert_called_once()


def test_clone_failure_still_recreates_pipeline_and_records_error():
    deps = MagicMock()
    deps.compute_boundary.return_value = "2026-06-10T00:00:00"
    deps.clone_history.return_value = {"status": "failed", "error_message": "boom"}
    deps.target_view_exists.return_value = False
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="tgt_pg")
    lfc_tables = [r for r in results if r["object_type"] == "lfc_table"]
    assert len(lfc_tables) == 1
    assert lfc_tables[0]["status"] == "failed"
    assert lfc_tables[0]["error_message"] == "boom"
    # pipeline is still recreated (independent of the history clone)...
    deps.create_pipeline.assert_called_once()
    # ...but the unified view is SKIPPED — can't union a missing _history table,
    # and we must not mask the clone error with a downstream create_view failure.
    deps.create_view.assert_not_called()
    assert not any(r["object_type"] == "lfc_view" for r in results)


def test_triggers_pipeline_run_then_builds_view():
    # After creating the recreated pipeline, the worker triggers its first run
    # (so <t>_incr materialises) and THEN builds the unified view.
    deps = MagicMock()
    deps.compute_boundary.return_value = "2026-06-10T00:00:00"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_view_exists.return_value = False     # canonical view absent → not idempotent-skip
    deps.target_table_exists.return_value = True      # <t>_incr present after the triggered run
    deps.create_pipeline.return_value = "pid-123"
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="tgt_pg")
    # pipeline run was triggered with the new pipeline id + the _incr target(s)
    deps.run_pipeline_and_await.assert_called_once()
    assert deps.run_pipeline_and_await.call_args.args[0] == "pid-123"
    assert any("_incr" in t for t in deps.run_pipeline_and_await.call_args.args[1])
    # view is built (not deferred)
    views = [r for r in results if r["object_type"] == "lfc_view"]
    assert len(views) == 1 and views[0]["status"] == "lfc_view_created"
    deps.create_view.assert_called_once()


def test_view_deferred_when_incr_not_materialized_after_trigger():
    # Worker still triggers the run, but if <t>_incr never lands (e.g. the target
    # can't reach the source / its update FAILED), the view is gracefully deferred.
    deps = MagicMock()
    deps.compute_boundary.return_value = "2026-06-10T00:00:00"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_view_exists.return_value = False
    deps.target_table_exists.return_value = False     # <t>_incr never appeared
    deps.create_pipeline.return_value = "pid-123"
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="tgt_pg")
    deps.run_pipeline_and_await.assert_called_once()  # still triggered
    views = [r for r in results if r["object_type"] == "lfc_view"]
    assert len(views) == 1 and views[0]["status"] == "lfc_view_pending_forward_ingest"
    deps.create_view.assert_not_called()


def test_view_create_failure_defers_without_failing_pipeline_or_table():
    # If create_view raises (e.g. <t>_incr streaming table not yet queryable:
    # STREAMING_TABLE_NEEDS_REFRESH), the view is deferred — the already-created
    # pipeline and cloned-history table rows must SURVIVE, not be discarded.
    deps = MagicMock()
    deps.compute_boundary.return_value = "2026-06-10T00:00:00"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_view_exists.return_value = False
    deps.target_table_exists.return_value = True       # _incr object exists...
    deps.create_pipeline.return_value = "pid-123"
    deps.create_view.side_effect = RuntimeError("[STREAMING_TABLE_NEEDS_REFRESH] orders_incr")
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="tgt_pg")
    assert any(r["object_type"] == "lfc_pipeline" and r["status"] == "lfc_pipeline_created_incremental"
               for r in results)
    assert any(r["object_type"] == "lfc_table" and r["status"] == "validated" for r in results)
    views = [r for r in results if r["object_type"] == "lfc_view"]
    assert len(views) == 1 and views[0]["status"] == "lfc_view_pending_forward_ingest"
    assert "create failed" in (views[0]["error_message"] or "")
