import pytest

from common.tracking import _TERMINAL_STATUSES
from migrate.lfc_utils import (
    build_query_based_create_spec,
    build_recreate_spec,
    build_unified_view_sql,
    classify_pipeline,
    extract_table_configs,
    parse_describe_columns,
    resolve_saas_cursor,
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
    # WORKDAY is a non-row_filter SaaS connector → tier2
    assert classify_pipeline(SAAS_DEF) == ("saas", "tier2")


def _saas_def(source_type):
    return {"spec": {"ingestion_definition": {"source_type": source_type, "connection_name": "c", "objects": []}}}


@pytest.mark.parametrize("st", ["SALESFORCE", "GA4_RAW_DATA", "SERVICENOW"])
def test_row_filter_saas_is_tier1(st):
    assert classify_pipeline(_saas_def(st)) == ("saas", "tier1")


@pytest.mark.parametrize("st", ["WORKDAY_RAAS", "SHAREPOINT", "NETSUITE", "DYNAMICS365"])
def test_non_row_filter_saas_is_tier2(st):
    assert classify_pipeline(_saas_def(st)) == ("saas", "tier2")


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


def test_salesforce_cursor_priority_picks_first_present():
    cols = ["Id", "Name", "CreatedDate", "LastModifiedDate", "SystemModstamp"]
    assert resolve_saas_cursor("SALESFORCE", cols) == "SystemModstamp"


def test_salesforce_cursor_falls_through_to_createddate():
    assert resolve_saas_cursor("SALESFORCE", ["Id", "Name", "CreatedDate"]) == "CreatedDate"


def test_salesforce_cursor_none_when_absent():
    assert resolve_saas_cursor("SALESFORCE", ["Id", "Name"]) is None


def test_servicenow_cursor():
    assert resolve_saas_cursor("SERVICENOW", ["sys_id", "sys_updated_on"]) == "sys_updated_on"


def test_ga4_cursor():
    assert resolve_saas_cursor("GA4_RAW_DATA", ["event_date", "event_name"]) == "event_date"


def test_resolver_case_insensitive_on_columns():
    assert resolve_saas_cursor("SALESFORCE", ["systemmodstamp"]) == "systemmodstamp"


def test_unknown_source_type_returns_none():
    assert resolve_saas_cursor("WORKDAY_RAAS", ["anything"]) is None


def test_parse_describe_columns_basic():
    rows = [["Id", "string", None], ["Name", "string", None], ["SystemModstamp", "timestamp", None]]
    assert parse_describe_columns(rows) == ["Id", "Name", "SystemModstamp"]


def test_parse_describe_columns_stops_at_partition_metadata():
    # DESCRIBE TABLE re-lists partition cols under '# Partition Information';
    # collection must stop there so they aren't double-counted.
    rows = [
        ["Id", "string", None], ["region", "string", None],
        ["", "", ""], ["# Partition Information", "", ""],
        ["# col_name", "data_type", "comment"], ["region", "string", None],
    ]
    assert parse_describe_columns(rows) == ["Id", "region"]


def test_parse_describe_columns_dict_rows_and_empty():
    assert parse_describe_columns([]) == []
    assert parse_describe_columns([{"col_name": "Id"}, {"col_name": "SystemModstamp"}]) \
        == ["Id", "SystemModstamp"]


def _one_table_def(source_type, dest="orders", extra_tc=None):
    tc = {"scd_type": "SCD_TYPE_1", "primary_keys": ["Id"]}
    if extra_tc:
        tc.update(extra_tc)
    return {"spec": {"catalog": "bronze", "schema": "sf", "ingestion_definition": {
        "source_type": source_type, "connection_name": "src_sf", "objects": [
        {"table": {"source_table": dest, "destination_catalog": "bronze",
                   "destination_schema": "sf", "destination_table": dest,
                   "table_configuration": tc}}]}}}


def test_saas_spec_sets_row_filter_and_incr_from_cursor_map():
    spec = build_recreate_spec(_one_table_def("SALESFORCE"),
                               target_connection_name="tgt_sf", name="p_migrated",
                               row_filter_by_src={"orders": "SystemModstamp >= '2026-06-10T00:00:00.000Z'"})
    t = spec["ingestion_definition"]["objects"][0]["table"]
    assert t["destination_table"] == "orders_incr"
    assert t["table_configuration"]["row_filter"] == "SystemModstamp >= '2026-06-10T00:00:00.000Z'"
    assert "query_based_connector_config" not in t["table_configuration"]
    assert spec["catalog"] == "bronze" and spec["schema"] == "sf"
    assert spec["ingestion_definition"]["connection_name"] == "tgt_sf"


def test_table_absent_from_map_keeps_canonical_name_no_filter():
    spec = build_recreate_spec(_one_table_def("SALESFORCE"),
                               target_connection_name="tgt_sf", name="p_migrated",
                               row_filter_by_src={})
    t = spec["ingestion_definition"]["objects"][0]["table"]
    assert t["destination_table"] == "orders"
    assert "row_filter" not in t["table_configuration"]
