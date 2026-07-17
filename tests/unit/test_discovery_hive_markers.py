"""Unit tests for /mnt mount-prerequisite markers emitted by discovery."""

from __future__ import annotations

from unittest.mock import MagicMock

from discovery.discovery import _discover_hive, mount_name_from_location


class TestMountNameFromLocation:
    def test_extracts_mount_name(self):
        assert mount_name_from_location("dbfs:/mnt/salesraw/tbl") == "salesraw"
        assert mount_name_from_location("dbfs:/mnt/salesraw") == "salesraw"

    def test_none_for_non_mount(self):
        for loc in (None, "", "abfss://c@a.dfs.core.windows.net/x", "dbfs:/user/hive/warehouse/t"):
            assert mount_name_from_location(loc) is None


class TestDiscoverHiveMountMarkers:
    def _explorer(self):
        explorer = MagicMock()
        explorer.list_hive_databases.return_value = ["db1"]
        explorer.classify_hive_tables.return_value = [
            {
                "fqn": "`hive_metastore`.`db1`.`mnt_tbl`",
                "object_type": "hive_table",
                "table_type": "EXTERNAL",
                "storage_location": "dbfs:/mnt/salesraw/mnt_tbl",
                "provider": "delta",
                "data_category": "hive_external",
            },
        ]
        explorer.list_hive_functions.return_value = []
        explorer.get_table_row_count.return_value = 0
        explorer.get_table_size_bytes.return_value = 0
        return explorer

    def test_emits_mount_prerequisite_marker(self):
        rows = _discover_hive(config=MagicMock(), explorer=self._explorer(), now="2026-07-16")
        markers = [r for r in rows if r["object_type"] == "mount_prerequisite"]
        assert len(markers) == 1
        import json
        meta = json.loads(markers[0]["metadata_json"])
        assert meta["mount"] == "salesraw"
        assert "`hive_metastore`.`db1`.`mnt_tbl`" in meta["tables"]

    def test_no_marker_when_no_mount_tables(self):
        explorer = self._explorer()
        explorer.classify_hive_tables.return_value[0]["storage_location"] = (
            "abfss://c@a.dfs.core.windows.net/x"
        )
        rows = _discover_hive(config=MagicMock(), explorer=explorer, now="2026-07-16")
        assert not any(r["object_type"] == "mount_prerequisite" for r in rows)
