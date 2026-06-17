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

def test_unknown_pipeline_is_skipped_this_stage():
    # A non-CDC, non-Tier-1 (unknown) ingestion def is still deferred → [].
    unknown_def = {"spec": {"ingestion_definition": {"objects": []}}}
    unknown = {"object_name": "p", "object_type": "lfc_pipeline",
               "metadata_json": json.dumps({"definition": unknown_def})}
    assert migrate_pipeline(unknown, deps=MagicMock(), target_connection_name="t") == []


def test_cdc_without_nested_gateway_spec_fails_loud():
    # CDC is now handled (Task 5): a CDC row missing its nested gateway_spec can't
    # be recreated, so it fails loudly rather than silently deferring.
    cdc_def = {"spec": {"ingestion_definition": {"ingestion_gateway_id": "g"}}}
    cdc = {"object_name": "p", "object_type": "lfc_pipeline",
           "metadata_json": json.dumps({"definition": cdc_def})}
    results = migrate_pipeline(cdc, deps=MagicMock(), target_connection_name="t")
    assert len(results) == 1 and results[0]["status"] == "failed"

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


def _sf_row(dest="orders", source_type="SALESFORCE"):
    sf_def = {"spec": {"catalog": "bronze", "schema": "sf", "ingestion_definition": {
        "source_type": source_type, "connection_name": "src_sf", "objects": [
        {"table": {"source_table": dest, "destination_catalog": "bronze",
                   "destination_schema": "sf", "destination_table": dest,
                   "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["Id"]}}}]}}}
    return {"object_name": "sf_orders", "object_type": "lfc_pipeline",
            "metadata_json": json.dumps({"definition": sf_def})}


def test_saas_tier1_uses_supplied_cursor_and_sets_row_filter():
    # The cursor is operator-supplied via saas_cursor_columns (keyed by dest FQN);
    # the worker validates it exists, then sets the row_filter — NO auto-guessing.
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.get_columns.return_value = ["Id", "Name", "SystemModstamp", "CreatedDate"]
    deps.compute_boundary.return_value = "2026-06-10T00:00:00.000Z"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_table_exists.return_value = True
    deps.create_pipeline.return_value = "pid-1"
    results = migrate_pipeline(_sf_row(), deps=deps, target_connection_name="tgt_sf",
                               saas_cursor_columns={"bronze.sf.orders": "SystemModstamp"})
    # boundary computed against the SUPPLIED cursor
    deps.compute_boundary.assert_called_once()
    assert deps.compute_boundary.call_args.args[1] == "SystemModstamp"
    # recreate spec carries the SaaS row_filter
    spec = deps.create_pipeline.call_args.args[0]
    tc = spec["ingestion_definition"]["objects"][0]["table"]["table_configuration"]
    assert tc["row_filter"] == "SystemModstamp >= '2026-06-10T00:00:00.000Z'"
    assert any(r["object_type"] == "lfc_view" and r["status"] == "lfc_view_created" for r in results)


def test_saas_tier1_cursor_matched_case_insensitively():
    # Supplied cursor matches a real column case-insensitively; the REAL-cased
    # column name is used in the row_filter.
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.get_columns.return_value = ["Id", "SystemModstamp"]
    deps.compute_boundary.return_value = "2026-06-10T00:00:00.000Z"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_table_exists.return_value = True
    deps.create_pipeline.return_value = "pid-1"
    migrate_pipeline(_sf_row(), deps=deps, target_connection_name="tgt_sf",
                     saas_cursor_columns={"bronze.sf.orders": "systemmodstamp"})
    assert deps.compute_boundary.call_args.args[1] == "SystemModstamp"


def test_saas_tier1_cursor_absent_from_map_fullloads_no_filter():
    # cursor not supplied for this table → full-load: skipped_no_cursor, no clone/view.
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.create_pipeline.return_value = "pid-1"
    results = migrate_pipeline(_sf_row(), deps=deps, target_connection_name="tgt_sf",
                               saas_cursor_columns={})
    deps.get_columns.assert_not_called()
    deps.compute_boundary.assert_not_called()
    deps.clone_history.assert_not_called()
    spec = deps.create_pipeline.call_args.args[0]
    tc = spec["ingestion_definition"]["objects"][0]["table"]["table_configuration"]
    assert "row_filter" not in tc
    assert any(r["object_type"] == "lfc_view" and r["status"] == "lfc_view_skipped_no_cursor"
               for r in results)


def test_saas_tier1_supplied_cursor_not_a_real_column_is_skipped_with_error():
    # Operator supplied a cursor that doesn't exist on the table → skip clone+view,
    # record an error status; do NOT set a bad row_filter.
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.get_columns.return_value = ["Id", "Name"]   # no SystemModstamp
    deps.create_pipeline.return_value = "pid-1"
    results = migrate_pipeline(_sf_row(), deps=deps, target_connection_name="tgt_sf",
                               saas_cursor_columns={"bronze.sf.orders": "SystemModstamp"})
    deps.compute_boundary.assert_not_called()
    deps.clone_history.assert_not_called()
    spec = deps.create_pipeline.call_args.args[0]
    tc = spec["ingestion_definition"]["objects"][0]["table"]["table_configuration"]
    assert "row_filter" not in tc
    views = [r for r in results if r["object_type"] == "lfc_view"]
    assert len(views) == 1 and views[0]["status"] == "lfc_view_skipped_no_cursor"
    assert "SystemModstamp" in (views[0]["error_message"] or "")


def test_saas_tier1_mixed_tables_are_independent():
    # One pipeline, three SaaS tables: a good-cursor table, a no-cursor table,
    # and a bad-cursor table. The good table must clone+filter+view; the other
    # two must skip WITHOUT affecting the good one.
    def _tbl(name):
        return {"table": {"source_table": name, "destination_catalog": "bronze",
                          "destination_schema": "sf", "destination_table": name,
                          "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["Id"]}}}
    sf_def = {"spec": {"catalog": "bronze", "schema": "sf", "ingestion_definition": {
        "source_type": "SALESFORCE", "connection_name": "src_sf",
        "objects": [_tbl("orders"), _tbl("contacts"), _tbl("leads")]}}}
    row = {"object_name": "sf_multi", "object_type": "lfc_pipeline",
           "metadata_json": json.dumps({"definition": sf_def})}
    cols = {"bronze.sf.orders": ["Id", "SystemModstamp"], "bronze.sf.leads": ["Id", "Name"]}
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.get_columns.side_effect = lambda fqn: cols.get(fqn, [])
    deps.compute_boundary.return_value = "2026-06-10T00:00:00.000Z"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_table_exists.return_value = True
    deps.create_pipeline.return_value = "pid-1"
    results = migrate_pipeline(
        row, deps=deps, target_connection_name="tgt_sf",
        # orders: valid cursor; leads: cursor not a real column; contacts: absent
        saas_cursor_columns={"bronze.sf.orders": "SystemModstamp", "bronze.sf.leads": "SystemModstamp"},
    )
    # only the good table is cloned + boundary-computed
    deps.clone_history.assert_called_once()
    assert deps.clone_history.call_args.args[0]["source_table"] == "orders"
    # recreate spec: orders -> _incr + row_filter; contacts/leads untouched (full-load)
    objs = {o["table"]["source_table"]: o["table"] for o in
            deps.create_pipeline.call_args.args[0]["ingestion_definition"]["objects"]}
    assert objs["orders"]["destination_table"] == "orders_incr"
    assert objs["orders"]["table_configuration"]["row_filter"] == "SystemModstamp >= '2026-06-10T00:00:00.000Z'"
    for _t in ("contacts", "leads"):
        assert objs[_t]["destination_table"] == _t
        assert "row_filter" not in objs[_t]["table_configuration"]
    # exactly one created view (orders) + two skipped (contacts, leads)
    views = {r["object_name"]: r["status"] for r in results if r["object_type"] == "lfc_view"}
    assert views["bronze.sf.orders"] == "lfc_view_created"
    assert views["bronze.sf.contacts"] == "lfc_view_skipped_no_cursor"
    assert views["bronze.sf.leads"] == "lfc_view_skipped_no_cursor"


def _cdc_row(name="cdc_orders", gw_id="src-gw-1"):
    d = {"spec": {"catalog": "bronze", "schema": "cdc", "ingestion_definition": {
        "ingestion_gateway_id": gw_id, "objects": [
        {"table": {"source_schema": "dbo", "source_table": "orders",
                   "destination_catalog": "bronze", "destination_schema": "cdc", "destination_table": "orders",
                   "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["id"]}}}]}},
         "gateway_spec": {"spec": {"gateway_definition": {
             "connection_name": "src_sql", "gateway_storage_catalog": "stg",
             "gateway_storage_schema": "cdc", "gateway_storage_name": "gw_vol", "id": gw_id}}}}
    return {"object_name": name, "object_type": "lfc_pipeline",
            "metadata_json": json.dumps({"definition": d})}


def test_cdc_recreates_gateway_then_ingestion_and_validates():
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.create_gateway.return_value = "tgt-gw-9"
    deps.create_pipeline.return_value = "tgt-ing-9"
    gmap = {}
    results = migrate_pipeline(_cdc_row(), deps=deps, target_connection_name="tgt_sql", gateway_id_map=gmap)
    deps.create_gateway.assert_called_once()                       # gateway recreated
    assert gmap["src-gw-1"] == "tgt-gw-9"                           # mapping cached
    spec = deps.create_pipeline.call_args.args[0]
    assert spec["ingestion_definition"]["ingestion_gateway_id"] == "tgt-gw-9"   # remapped
    deps.validate_pipeline.assert_called()                          # dry-validate run
    assert any(r["object_type"] == "lfc_gateway" and r["status"] == "lfc_gateway_created" for r in results)
    assert any(r["status"] == "lfc_pipeline_created_fullreload" for r in results)
    # NOT started:
    deps.run_pipeline_and_await.assert_not_called()


def test_cdc_shared_gateway_recreated_once():
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.create_gateway.return_value = "tgt-gw-9"
    deps.create_pipeline.side_effect = ["i1", "i2"]
    gmap = {}
    migrate_pipeline(_cdc_row("cdc_a"), deps=deps, target_connection_name="t", gateway_id_map=gmap)
    migrate_pipeline(_cdc_row("cdc_b"), deps=deps, target_connection_name="t", gateway_id_map=gmap)
    deps.create_gateway.assert_called_once()    # same source gateway → recreated once
    assert deps.create_pipeline.call_count == 2  # both ingestion pipelines recreated
