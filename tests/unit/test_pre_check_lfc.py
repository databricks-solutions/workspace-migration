import json
from unittest.mock import MagicMock
from pre_check.pre_check_lfc import find_blockers

def _row(defn):
    return {"object_name": "p1", "metadata_json": json.dumps({"definition": defn})}

QB = {"spec": {"ingestion_definition": {"connection_name": "src_pg", "objects": [
    {"table": {"destination_catalog": "bronze", "destination_schema": "pg",
               "destination_table": "orders",
               "table_configuration": {"cursor_column": "updated_at"}}}]}}}

def test_blocker_when_connection_missing():
    tc = MagicMock()
    tc.connections.get.side_effect = Exception("no conn")
    tc.schemas.get.return_value = object()
    blockers = find_blockers(tc, [_row(QB)], target_connection_name="src_pg")
    assert any("connection" in b.lower() for b in blockers)

def test_no_blocker_when_present():
    tc = MagicMock()
    tc.connections.get.return_value = object()
    tc.schemas.get.return_value = object()
    assert find_blockers(tc, [_row(QB)], target_connection_name="src_pg") == []
