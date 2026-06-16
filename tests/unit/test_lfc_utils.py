from common.tracking import _TERMINAL_STATUSES
from migrate.lfc_utils import classify_pipeline, extract_table_configs


def test_lfc_stage1_statuses_are_terminal():
    for s in ("lfc_pipeline_created_incremental", "lfc_view_created"):
        assert s in _TERMINAL_STATUSES


QB_DEF = {
    "spec": {
        "ingestion_definition": {
            "connection_name": "src_pg",
            "objects": [
                {"table": {"source_catalog": "pg", "source_schema": "public",
                           "source_table": "orders",
                           "destination_catalog": "bronze", "destination_schema": "pg",
                           "destination_table": "orders",
                           "table_configuration": {"scd_type": "SCD_TYPE_1",
                                                   "primary_keys": ["order_id"],
                                                   "cursor_column": "updated_at"}}},
            ],
        }
    }
}
CDC_DEF = {"spec": {"ingestion_definition": {"ingestion_gateway_id": "gw-123", "objects": []}}}


def test_classify_query_based():
    assert classify_pipeline(QB_DEF) == ("query_based", "tier1")


def test_classify_cdc_is_tier2_not_this_stage():
    assert classify_pipeline(CDC_DEF) == ("cdc", "tier2")


def test_extract_table_configs():
    cfgs = extract_table_configs(QB_DEF)
    assert cfgs == [{
        "source_catalog": "pg", "source_schema": "public", "source_table": "orders",
        "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "orders",
        "scd_type": "SCD_TYPE_1", "primary_keys": ["order_id"], "cursor_column": "updated_at",
    }]
