from __future__ import annotations

from typing import Any


class Validator:
    """Validates migrated objects between source and target workspaces."""

    def __init__(self, source_explorer: Any, target_explorer: Any) -> None:
        self.source_explorer = source_explorer
        self.target_explorer = target_explorer

    def validate_row_count(self, source_fqn: str, target_fqn: str) -> dict[str, Any]:
        """Compare row counts between source and target tables."""
        source_count = self.source_explorer.get_table_row_count(source_fqn)
        target_count = self.target_explorer.get_table_row_count(target_fqn)
        return {
            "source_count": source_count,
            "target_count": target_count,
            "match": source_count == target_count,
        }

    @staticmethod
    def _real_columns(rows: list[Any]) -> list[dict[str, Any]]:
        """Filter raw DESCRIBE TABLE rows down to real (col_name, data_type) columns.

        ``DESCRIBE TABLE`` appends non-column metadata after the column list:
        a blank separator row, a ``# Partition Information`` header, a
        ``# col_name`` sub-header, and the partition columns repeated. Those
        rows are not real columns. The column list ends at the first blank or
        ``#``-prefixed ``col_name``, so we stop there. Only ``col_name`` and
        ``data_type`` are kept — ``comment`` is deliberately excluded so a
        comment-only difference is never flagged as a schema mismatch.
        """
        columns: list[dict[str, Any]] = []
        for row in rows:
            col = row.asDict()
            name = (col.get("col_name") or "").strip()
            if name == "" or name.startswith("#"):
                break
            columns.append({"col_name": name, "data_type": col.get("data_type")})
        return columns

    def validate_schema_match(self, source_fqn: str, target_fqn: str) -> dict[str, Any]:
        """Compare column schemas between source and target tables.

        Compares only the real ``(col_name, data_type)`` pairs — metadata
        rows and the ``comment`` column are ignored so the gate neither
        false-passes a divergent schema nor false-fails an identical one.
        """
        source_columns = self._real_columns(
            self.source_explorer.spark.sql(f"DESCRIBE TABLE {source_fqn}").collect()
        )
        target_columns = self._real_columns(
            self.target_explorer.spark.sql(f"DESCRIBE TABLE {target_fqn}").collect()
        )

        mismatches: list[dict[str, Any]] = []
        source_map = {col["col_name"]: col["data_type"] for col in source_columns}
        target_map = {col["col_name"]: col["data_type"] for col in target_columns}

        all_names = set(source_map.keys()) | set(target_map.keys())
        for name in sorted(all_names):
            if name not in source_map:
                mismatches.append({"column": name, "issue": "missing_in_source"})
            elif name not in target_map:
                mismatches.append({"column": name, "issue": "missing_in_target"})
            elif source_map[name] != target_map[name]:
                mismatches.append(
                    {
                        "column": name,
                        "issue": "type_mismatch",
                        "source": source_map[name],
                        "target": target_map[name],
                    }
                )

        return {
            "source_columns": source_columns,
            "target_columns": target_columns,
            "match": len(mismatches) == 0,
            "mismatches": mismatches,
        }

    def validate_object_exists(self, target_fqn: str) -> bool:
        """Check whether an object exists in the target workspace."""
        try:
            self.target_explorer.spark.sql(f"DESCRIBE TABLE {target_fqn}").collect()
        except Exception:
            return False
        return True
