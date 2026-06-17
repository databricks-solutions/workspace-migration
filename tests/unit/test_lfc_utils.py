from common.tracking import _TERMINAL_STATUSES
from migrate.lfc_utils import (
    build_query_based_create_spec,
    build_unified_view_sql,
    classify_pipeline,
    extract_table_configs,
)


def test_lfc_stage1_statuses_are_terminal():
    for s in ("lfc_pipeline_created_incremental", "lfc_view_created"):
        assert s in _TERMINAL_STATUSES


# cursor lives in the NESTED query_based_connector_config.cursor_columns (DOC-26044); flat cursor_column is dropped
_QB_TC = {
    "scd_type": "SCD_TYPE_1",
    "primary_keys": ["order_id"],
    "query_based_connector_config": {"cursor_columns": ["updated_at"]},
}
_QB_TABLE = {
    "source_catalog": "pg", "source_schema": "public", "source_table": "orders",
    "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "orders",
    "table_configuration": _QB_TC,
}
QB_DEF = {"spec": {"catalog": "bronze", "schema": "pg", "ingestion_definition": {
    "connection_name": "src_pg", "source_type": "POSTGRESQL", "objects": [{"table": _QB_TABLE}]}}}
CDC_DEF = {"spec": {"ingestion_definition": {
    "source_type": "SQLSERVER", "ingestion_gateway_id": "gw-123", "objects": []}}}
SAAS_DEF = {"spec": {"ingestion_definition": {
    "connection_name": "wd", "source_type": "WORKDAY", "objects": []}}}


def test_classify_query_based():
    assert classify_pipeline(QB_DEF) == ("query_based", "tier1")


def test_classify_query_based_keys_off_source_type_not_cursor():
    # cursor_columns may be absent (connector auto-selects) — source_type still classifies it
    no_cursor = {"spec": {"ingestion_definition": {"source_type": "MYSQL", "objects": [
        {"table": {"table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["id"]}}}]}}}
    assert classify_pipeline(no_cursor) == ("query_based", "tier1")


def test_classify_saas_via_source_type():
    assert classify_pipeline(SAAS_DEF) == ("saas", "tier1")


def test_classify_cdc_is_tier2_not_this_stage():
    assert classify_pipeline(CDC_DEF) == ("cdc", "tier2")


def test_extract_table_configs():
    cfgs = extract_table_configs(QB_DEF)
    assert cfgs == [{
        "source_catalog": "pg", "source_schema": "public", "source_table": "orders",
        "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "orders",
        "scd_type": "SCD_TYPE_1", "primary_keys": ["order_id"], "cursor_column": "updated_at",
    }]


def test_view_scd1_dedups_by_pk_latest_cursor():
    sql = build_unified_view_sql(
        canonical="`bronze`.`pg`.`orders`",
        history="`bronze`.`pg`.`orders_history`",
        incr="`bronze`.`pg`.`orders_incr`",
        scd_type="SCD_TYPE_1", primary_keys=["order_id"], cursor_column="updated_at",
    )
    assert "CREATE OR REPLACE VIEW `bronze`.`pg`.`orders`" in sql
    assert "PARTITION BY order_id" in sql
    assert "ORDER BY updated_at DESC" in sql
    assert "rn = 1" in sql


def test_view_scd2_is_union_all():
    sql = build_unified_view_sql(
        canonical="`bronze`.`pg`.`orders`",
        history="`bronze`.`pg`.`orders_history`",
        incr="`bronze`.`pg`.`orders_incr`",
        scd_type="SCD_TYPE_2", primary_keys=["order_id"], cursor_column="updated_at",
    )
    assert "UNION ALL" in sql
    assert "ROW_NUMBER" not in sql


def test_build_create_spec_sets_incr_dest_and_row_filter():
    spec = build_query_based_create_spec(
        QB_DEF, target_connection_name="tgt_pg",
        boundaries={"orders": "2026-06-10T00:00:00"}, name="lfc_orders_incr",
    )
    assert spec["channel"] == "PREVIEW"
    # top-level catalog/schema are REQUIRED on create (direct publishing mode)
    assert spec["catalog"] == "bronze"
    assert spec["schema"] == "pg"
    idef = spec["ingestion_definition"]
    assert idef["connection_name"] == "tgt_pg"
    tbl = idef["objects"][0]["table"]
    assert tbl["destination_table"] == "orders_incr"
    tc = tbl["table_configuration"]
    assert tc["row_filter"] == "updated_at >= '2026-06-10T00:00:00'"
    # cursor stays in the nested field; it's the boundary source
    assert tc["query_based_connector_config"]["cursor_columns"] == ["updated_at"]
    assert tc["scd_type"] == "SCD_TYPE_1"
