from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from migrate.batching import MAX_BATCH_BYTES, build_batches


def test_orchestrator_does_not_emit_comment_list():
    """``comment`` must not be in ``LIST_TYPES``.

    ``comments_worker.run()`` reads ``discovery_inventory`` directly
    because comments span table / view / column / volume / schema /
    catalog and a flat ``comment_list`` was never a natural fit. The
    historical entry in ``LIST_TYPES`` was dead — the worker never
    consumed the task-value. Regression test.
    """
    src_path = Path(__file__).resolve().parents[2] / "src/migrate/orchestrator.py"
    src = src_path.read_text()
    m = re.search(r"LIST_TYPES\s*=\s*\(([^)]*)\)", src, re.DOTALL)
    assert m, "LIST_TYPES tuple not found in orchestrator.py"
    body = m.group(1)
    assert '"comment"' not in body and "'comment'" not in body, (
        "``comment`` reintroduced in LIST_TYPES — comments_worker reads "
        "discovery_inventory directly, so the emit is dead. See review H7."
    )


class TestOrchestrator:
    """Tests for the orchestrator batching logic."""

    def test_batch_building(self):
        """Verify objects are split into correct batch sizes."""
        objects = [{"object_name": f"table_{i}", "object_type": "managed_table"} for i in range(7)]
        batches = build_batches(objects, batch_size=3)

        assert len(batches) == 3
        batch_0 = json.loads(batches[0])
        batch_1 = json.loads(batches[1])
        batch_2 = json.loads(batches[2])

        assert len(batch_0) == 3
        assert len(batch_1) == 3
        assert len(batch_2) == 1

        # Verify content
        assert batch_0[0]["object_name"] == "table_0"
        assert batch_0[2]["object_name"] == "table_2"
        assert batch_1[0]["object_name"] == "table_3"
        assert batch_2[0]["object_name"] == "table_6"

    def test_batch_building_exact_fit(self):
        """Verify exact multiples produce correct number of batches."""
        objects = [{"object_name": f"t_{i}"} for i in range(6)]
        batches = build_batches(objects, batch_size=3)

        assert len(batches) == 2
        assert len(json.loads(batches[0])) == 3
        assert len(json.loads(batches[1])) == 3

    def test_batch_building_single_batch(self):
        """Verify objects smaller than batch size produce one batch."""
        objects = [{"object_name": f"t_{i}"} for i in range(2)]
        batches = build_batches(objects, batch_size=50)

        assert len(batches) == 1
        assert len(json.loads(batches[0])) == 2

    def test_empty_inventory(self):
        """Verify no batches are created for empty input."""
        batches = build_batches([], batch_size=50)
        assert batches == []

    def test_batch_json_roundtrip(self):
        """Verify batches are valid JSON that can be round-tripped."""
        objects = [
            {"object_name": "`cat`.`sch`.`tbl`", "object_type": "managed_table", "row_count": 100},
            {"object_name": "`cat`.`sch`.`tbl2`", "object_type": "managed_table", "row_count": 200},
        ]
        batches = build_batches(objects, batch_size=10)

        assert len(batches) == 1
        parsed = json.loads(batches[0])
        assert parsed == objects

    def test_batch_strips_create_statement(self):
        """create_statement is stripped to keep task-value payloads under
        Jobs' 3000-byte for_each limit; workers re-query the full row."""
        objects = [
            {
                "object_name": "`cat`.`sch`.`tbl`",
                "object_type": "managed_table",
                "create_statement": "CREATE TABLE ...  (... very long DDL ...)",
                "row_count": 100,
            },
        ]
        batches = build_batches(objects, batch_size=10)

        parsed = json.loads(batches[0])
        assert "create_statement" not in parsed[0]
        # other fields preserved
        assert parsed[0]["object_name"] == "`cat`.`sch`.`tbl`"
        assert parsed[0]["row_count"] == 100


class TestBuildBatchesByteCap:
    """Byte-size ceiling is enforced alongside ``batch_size`` so the
    Databricks Jobs for_each 3000-byte per-parameter limit is never hit."""

    @staticmethod
    def _obj(i: int, storage_location_len: int = 200) -> dict:
        return {
            "object_name": f"`catalog_long_name`.`schema_long_name`.`object_name_{i:04d}`",
            "object_type": "hive_managed_dbfs_root",
            "catalog_name": "catalog_long_name",
            "schema_name": "schema_long_name",
            "data_category": "hive_managed_dbfs_root",
            "table_type": "MANAGED",
            "provider": "delta",
            "storage_location": "x" * storage_location_len,
        }

    def test_byte_cap_splits_before_count_cap(self):
        """Each object ≈ 300 bytes. With ``batch_size=50`` the count cap
        wouldn't trigger, but 10+ such objects would blow past
        ``MAX_BATCH_BYTES``; the orchestrator must close the batch early."""
        objects = [self._obj(i) for i in range(20)]
        batches = build_batches(objects, batch_size=50)

        assert len(batches) > 1, "Byte cap should have forced multiple batches."
        for b in batches:
            assert len(b.encode("utf-8")) <= MAX_BATCH_BYTES, (
                f"batch size {len(b.encode('utf-8'))} exceeds MAX_BATCH_BYTES={MAX_BATCH_BYTES}"
            )

        # Round-trip: every object must appear exactly once across all batches.
        flattened = [o for b in batches for o in json.loads(b)]
        assert len(flattened) == len(objects)
        assert {o["object_name"] for o in flattened} == {o["object_name"] for o in objects}

    def test_count_cap_still_wins_when_under_byte_cap(self):
        """If objects are tiny, count cap is the binding constraint."""
        objects = [{"object_name": f"t_{i}"} for i in range(10)]
        batches = build_batches(objects, batch_size=3)

        assert len(batches) == 4  # 3 + 3 + 3 + 1
        for b in batches:
            assert len(json.loads(b)) <= 3

    def test_single_huge_object_emits_warning_but_is_batched(self, caplog):
        """An object whose own JSON is > ``MAX_BATCH_BYTES`` is still emitted
        (alone) — dropping it silently would be worse than an operator-
        visible for_each failure with the warning pointing at the culprit."""
        huge = {
            "object_name": "`cat`.`sch`.`huge_object`",
            "storage_location": "x" * (MAX_BATCH_BYTES + 1000),
        }
        with caplog.at_level(logging.WARNING, logger="orchestrator"):
            batches = build_batches([huge], batch_size=50)

        assert len(batches) == 1
        parsed = json.loads(batches[0])
        assert parsed[0]["object_name"] == "`cat`.`sch`.`huge_object`"
        assert any("Single object encoded to" in r.message for r in caplog.records)
        assert any("huge_object" in r.message for r in caplog.records)

    def test_huge_object_is_flushed_into_its_own_batch(self):
        """A huge object after normal ones must close the prior batch and
        land alone in a new batch — not be appended past the byte cap."""
        tiny = [{"object_name": f"t_{i}"} for i in range(3)]
        huge = {"object_name": "huge", "payload": "x" * (MAX_BATCH_BYTES + 500)}
        objects = tiny + [huge]
        batches = build_batches(objects, batch_size=50)

        # 1 batch of 3 tiny + 1 batch of huge alone.
        parsed = [json.loads(b) for b in batches]
        assert len(parsed) == 2
        assert {o["object_name"] for o in parsed[0]} == {"t_0", "t_1", "t_2"}
        assert parsed[1][0]["object_name"] == "huge"



class TestOrchestratorCollisionGate:
    """X.4: migrate orchestrator refuses to start when the latest
    pre_check_results has a target_collision FAIL row. Verified against
    the ``check_collision_gate`` helper in ``migrate.orchestrator``.
    """

    def _mock_spark_with_gate_rows(self, rows: list[tuple[str, str]]):
        """Return a spark mock whose sql().collect() yields MagicMocks
        with .status and .message attrs matching the given (status, msg)
        tuples."""
        from unittest.mock import MagicMock

        result_rows = []
        for status, msg in rows:
            m = MagicMock()
            m.status = status
            m.message = msg
            result_rows.append(m)
        result = MagicMock()
        result.collect.return_value = result_rows
        spark = MagicMock()
        spark.sql.return_value = result
        return spark

    def _mock_config(self):
        from unittest.mock import MagicMock

        c = MagicMock()
        c.tracking_catalog = "migration_tracking"
        c.tracking_schema = "cp_migration"
        return c

    def test_no_pre_check_rows_passes(self):
        """When pre_check_results has no target_collision row, the gate is
        a no-op — typical when pre_check hasn't been rerun after
        discovery."""
        from migrate.orchestrator import check_collision_gate

        spark = self._mock_spark_with_gate_rows([])
        check_collision_gate(spark, self._mock_config())  # should not raise

    def test_pass_row_does_not_raise(self):
        """A PASS row means collision detection ran and found nothing."""
        from migrate.orchestrator import check_collision_gate

        spark = self._mock_spark_with_gate_rows([("PASS", "all clean")])
        check_collision_gate(spark, self._mock_config())  # should not raise

    def test_warn_row_does_not_raise(self):
        """A WARN row means on_target_collision=skip was active —
        migration proceeds with skipped_target_exists rows pre-seeded."""
        from migrate.orchestrator import check_collision_gate

        spark = self._mock_spark_with_gate_rows([("WARN", "3 collisions skipped")])
        check_collision_gate(spark, self._mock_config())  # should not raise

    def test_fail_row_raises(self):
        """A FAIL row means on_target_collision=fail and the operator
        hasn't resolved the collisions. Migrate must refuse."""
        import pytest

        from migrate.orchestrator import check_collision_gate

        spark = self._mock_spark_with_gate_rows([("FAIL", "2 collisions detected")])
        with pytest.raises(RuntimeError, match="Migrate refused to start"):
            check_collision_gate(spark, self._mock_config())

    def test_fail_row_message_surfaced_in_raise(self):
        """Helpful errors: operator should see which collisions blocked
        them without having to tail the pre_check_results table."""
        import pytest

        from migrate.orchestrator import check_collision_gate

        spark = self._mock_spark_with_gate_rows(
            [("FAIL", "collision in catalog retail and schema orders")]
        )
        with pytest.raises(RuntimeError) as excinfo:
            check_collision_gate(spark, self._mock_config())
        assert "collision in catalog retail" in str(excinfo.value)

    def test_pre_check_results_missing_is_warning_not_raise(self):
        """Fresh install: pre_check_results table doesn't exist yet (or
        some other sql exception). Gate should log and return, not raise —
        that would block EVERY first migrate run."""
        from unittest.mock import MagicMock

        from migrate.orchestrator import check_collision_gate

        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        check_collision_gate(spark, self._mock_config())  # should not raise

    def test_sql_query_filters_to_target_collisions_only(self):
        """Sanity: the gate should only look at target_collision rows,
        not every FAIL in pre_check_results."""
        from unittest.mock import MagicMock

        from migrate.orchestrator import check_collision_gate

        spark = MagicMock()
        result = MagicMock()
        result.collect.return_value = []
        spark.sql.return_value = result
        check_collision_gate(spark, self._mock_config())

        sql = spark.sql.call_args[0][0]
        assert "check_target_collisions" in sql
        # Partitions by check_name so multiple pre_check runs don't
        # confuse the gate — only the LATEST row per check_name is read.
        assert "ROW_NUMBER()" in sql
        assert "ORDER BY checked_at DESC" in sql

    def test_multiple_rows_any_fail_triggers_raise(self):
        """If the pre_check batch writes multiple collision checks (e.g.
        one per type in a future expansion), any FAIL triggers the gate.
        Safety > strictness."""
        import pytest

        from migrate.orchestrator import check_collision_gate

        spark = self._mock_spark_with_gate_rows(
            [("PASS", "catalogs clean"), ("FAIL", "schemas have collisions")]
        )
        with pytest.raises(RuntimeError):
            check_collision_gate(spark, self._mock_config())
