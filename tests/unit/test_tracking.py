from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from common.tracking import TrackingManager, discovery_row, discovery_schema


class TestTrackingManager:
    """Tests for the TrackingManager class."""

    def test_init_tracking_tables_creates_schema(self, mock_spark, mock_config):
        mgr = TrackingManager(mock_spark, mock_config)
        mgr.init_tracking_tables()

        sql_calls = [c.args[0] for c in mock_spark.sql.call_args_list]

        assert any("CREATE CATALOG IF NOT EXISTS migration_tracking" in s for s in sql_calls)
        assert any("CREATE SCHEMA IF NOT EXISTS migration_tracking.cp_migration" in s for s in sql_calls)
        fqn = "migration_tracking.cp_migration"
        assert any(f"CREATE TABLE IF NOT EXISTS {fqn}.discovery_inventory" in s for s in sql_calls)
        assert any(f"CREATE TABLE IF NOT EXISTS {fqn}.migration_status" in s for s in sql_calls)
        assert any(f"CREATE TABLE IF NOT EXISTS {fqn}.pre_check_results" in s for s in sql_calls)

        # Unified discovery_inventory includes source_type and metadata_json columns
        discovery_ddl = next(s for s in sql_calls if "discovery_inventory" in s)
        assert "source_type STRING" in discovery_ddl
        assert "metadata_json STRING" in discovery_ddl
        assert "format STRING" in discovery_ddl

    def test_write_discovery_inventory_uses_merge(self, mock_spark, mock_config):
        """Concurrent discovery must not collide via mode('overwrite').
        write_discovery_inventory uses MERGE INTO keyed on the object
        triple. Verify the SQL shape — no full-table overwrite, and
        all three key columns are on the ON clause."""
        mgr = TrackingManager(mock_spark, mock_config)
        mock_df = MagicMock()
        mgr.write_discovery_inventory(mock_df)

        # Must stage via temp view.
        mock_df.createOrReplaceTempView.assert_called_once()

        # Must issue a MERGE statement — NOT an overwrite write.
        sql_calls = [c.args[0] for c in mock_spark.sql.call_args_list]
        merge_sql = next((s for s in sql_calls if "MERGE INTO" in s), None)
        assert merge_sql is not None, (
            "write_discovery_inventory must use MERGE INTO — overwrite "
            "causes DELTA_CONCURRENT_APPEND on concurrent discovery runs."
        )
        # All three key columns must appear in the ON clause.
        for key in ("object_name", "object_type", "source_type"):
            assert f"t.{key}  = s.{key}" in merge_sql or f"t.{key} = s.{key}" in merge_sql, (
                f"MERGE ON clause missing {key} — concurrent runs from different source_types would collide."
            )
        # Must NOT call the old overwrite write path.
        mock_df.write.mode.assert_not_called()

    def test_append_migration_status(self, mock_spark, mock_config):
        mgr = TrackingManager(mock_spark, mock_config)

        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_df.withColumn.return_value = mock_df

        records = [
            {
                "object_name": "catalog.schema.table1",
                "object_type": "managed_table",
                "status": "migrated",
                "error_message": None,
                "job_run_id": "123",
                "task_run_id": "456",
                "source_row_count": 100,
                "target_row_count": 100,
                "duration_seconds": 5.0,
            }
        ]
        mgr.append_migration_status(records)

        # Verify createDataFrame was called, but don't lock in the exact args —
        # the implementation now normalizes records and passes an explicit schema
        # (both needed to avoid the `CANNOT_DETERMINE_TYPE` integration failure).
        mock_spark.createDataFrame.assert_called_once()
        assert "schema" in mock_spark.createDataFrame.call_args.kwargs, (
            "createDataFrame must be called with an explicit schema kwarg to avoid "
            "type inference failures on all-None columns"
        )
        mock_df.withColumn.assert_called_once()
        assert mock_df.withColumn.call_args[0][0] == "migrated_at"
        mock_df.write.mode.assert_called_once_with("append")
        mock_df.write.mode.return_value.saveAsTable.assert_called_once_with(
            "migration_tracking.cp_migration.migration_status"
        )

    def test_get_pending_objects_filters_completed(self, mock_spark, mock_config):
        mgr = TrackingManager(mock_spark, mock_config)

        # Simulate two rows returned by the SQL query: one pending and one validated
        mock_row_pending = MagicMock()
        mock_row_pending.asDict.return_value = {
            "object_name": "catalog.schema.table1",
            "object_type": "managed_table",
            "catalog_name": "catalog",
            "schema_name": "schema",
        }

        mock_result = MagicMock()
        mock_result.collect.return_value = [mock_row_pending]
        mock_spark.sql.return_value = mock_result

        result = mgr.get_pending_objects("managed_table")

        # Verify the SQL query contains the correct filtering logic
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "LEFT JOIN" in sql_arg
        # Filter: ``validated``, ``skipped_by_pipeline_migration``,
        # ``skipped_target_exists`` (X.4), and
        # ``skipped_by_stateful_service_migration`` (streaming tables /
        # future Stateful Services Phase) are terminal. Other skip
        # statuses (skipped_by_config, skipped_by_rls_cm_policy, plain
        # skipped) re-enter pending so operators can flip config flags
        # and re-run.
        assert (
            "status NOT IN ('validated', 'skipped_by_pipeline_migration', "
            "'skipped_target_exists', 'skipped_by_stateful_service_migration')"
            in sql_arg
        )
        assert "managed_table" in sql_arg

        # Verify the result is a list of dicts from collect()
        assert len(result) == 1
        assert result[0]["object_name"] == "catalog.schema.table1"

    def test_get_tables_with_rls_cm(self, mock_spark, mock_config):
        """row_filter.object_name is already the table FQN; column_mask
        rows carry the clean table_fqn in metadata_json — both contribute."""
        import json as _json

        mgr = TrackingManager(mock_spark, mock_config)

        rf = MagicMock()
        rf.object_name = "`cat`.`sch`.`t1`"
        cm = MagicMock()
        cm.metadata_json = _json.dumps(
            {
                "table_fqn": "`cat`.`sch`.`t2`",
                "column_name": "ssn",
            }
        )
        # Also exercise: a malformed metadata_json is tolerated, not fatal.
        cm_bad = MagicMock()
        cm_bad.metadata_json = "not-json"

        def _sql(query: str) -> MagicMock:
            r = MagicMock()
            if "object_type = 'row_filter'" in query:
                r.collect.return_value = [rf]
            elif "object_type = 'column_mask'" in query:
                r.collect.return_value = [cm, cm_bad]
            else:
                r.collect.return_value = []
            return r

        mock_spark.sql.side_effect = _sql
        result = mgr.get_tables_with_rls_cm()
        assert result == {"`cat`.`sch`.`t1`", "`cat`.`sch`.`t2`"}

    def test_get_row_returns_dict_when_found(self, mock_spark, mock_config):
        """get_row(object_type, object_name) returns the row as a dict on hit."""
        mgr = TrackingManager(mock_spark, mock_config)

        mock_row = MagicMock()
        mock_row.asDict.return_value = {
            "object_name": "`cat`.`sch`.`t`",
            "object_type": "managed_table",
            "create_statement": "CREATE TABLE ...",
        }
        mock_result = MagicMock()
        mock_result.collect.return_value = [mock_row]
        mock_spark.sql.return_value = mock_result

        result = mgr.get_row("managed_table", "`cat`.`sch`.`t`")

        sql_arg = mock_spark.sql.call_args[0][0]
        assert "managed_table" in sql_arg
        assert "`cat`.`sch`.`t`" in sql_arg
        assert "LIMIT 1" in sql_arg
        assert result is not None
        assert result["object_name"] == "`cat`.`sch`.`t`"
        assert result["create_statement"] == "CREATE TABLE ..."

    def test_get_row_returns_none_when_not_found(self, mock_spark, mock_config):
        """get_row returns None when no row matches (empty collect)."""
        mgr = TrackingManager(mock_spark, mock_config)

        mock_result = MagicMock()
        mock_result.collect.return_value = []
        mock_spark.sql.return_value = mock_result

        result = mgr.get_row("managed_table", "`cat`.`sch`.`missing`")
        assert result is None

    def test_get_pending_objects_terminal_status_list(self, mock_spark, mock_config):
        """Terminal statuses: ``validated``, ``skipped_by_pipeline_migration``
        (DLT-owned MV), ``skipped_target_exists`` (X.4 collision skip
        policy), and ``skipped_by_stateful_service_migration`` (streaming
        tables — hard-excluded from the core tool; migrated by the
        future Stateful Services Phase). Other skip statuses re-enter
        pending on re-run so flag-gated skips (``skipped_by_config``,
        ``skipped_by_rls_cm_policy``) heal when the operator flips
        ``iceberg_strategy`` / ``rls_cm_strategy``.
        """
        mgr = TrackingManager(mock_spark, mock_config)
        mock_result = MagicMock()
        mock_result.collect.return_value = []
        mock_spark.sql.return_value = mock_result

        mgr.get_pending_objects("managed_table")

        sql = mock_spark.sql.call_args[0][0]
        # Terminal set uses an explicit IN list so future skip statuses
        # default to "re-pickup" unless someone adds them here.
        # ``skipped_target_exists`` (X.4) was added as terminal so the
        # skip policy for pre-existing target objects actually short-
        # circuits the worker on the next run.
        # ``skipped_by_stateful_service_migration`` was added to
        # hard-exclude streaming tables (migrated by the future Stateful
        # Services Phase, separate job).
        assert (
            "status NOT IN ('validated', 'skipped_by_pipeline_migration', "
            "'skipped_target_exists', 'skipped_by_stateful_service_migration')"
            in sql
        )
        # Guard against regression to the old LIKE filter that swept up
        # skipped_by_config + skipped_by_rls_cm_policy as terminal.
        assert "NOT LIKE 'skipped%'" not in sql

    def test_get_pending_objects_reincludes_skipped_by_config(self, mock_spark, mock_config):
        """Regression for the Iceberg-re-run scenario: a table with
        status=``skipped_by_config`` from a prior run with
        ``iceberg_strategy=""`` MUST reappear as pending when the
        operator flips to ``"ddl_replay"`` + re-runs."""
        import datetime

        mgr = TrackingManager(mock_spark, mock_config)

        # Simulate the filtered SQL returning the skipped_by_config row
        # as pending — if the filter excluded it, this row wouldn't come
        # back at all. We assert the SQL itself, so the mock just needs
        # to return SOMETHING that collect() iterates.
        returned = MagicMock()
        returned.asDict.return_value = {
            "object_name": "`cat`.`sch`.`iceberg_t`",
            "object_type": "managed_table",
            "format": "iceberg",
        }
        result = MagicMock()
        result.collect.return_value = [returned]
        mock_spark.sql.return_value = result

        pending = mgr.get_pending_objects("managed_table")

        sql = mock_spark.sql.call_args[0][0]
        # Critical contract: skipped_by_config NOT in the terminal IN list.
        assert "'skipped_by_config'" not in sql.replace("'skipped_by_pipeline_migration'", "")
        assert len(pending) == 1
        _ = datetime  # keep import for future schema assertions


class TestDiscoveryRowHelpers:
    """Tests for the module-level discovery_row() and discovery_schema() helpers."""

    def test_discovery_row_uc(self):
        now = datetime.now(tz=timezone.utc)
        row = discovery_row(
            source_type="uc",
            object_type="managed_table",
            object_name="cat.sch.t1",
            catalog_name="cat",
            schema_name="sch",
            discovered_at=now,
            row_count=10,
            size_bytes=100,
            is_dlt_managed=False,
            pipeline_id=None,
            create_statement="CREATE TABLE ...",
        )
        assert row["source_type"] == "uc"
        assert row["object_type"] == "managed_table"
        assert row["data_category"] is None
        assert row["metadata_json"] is None

    def test_discovery_row_hive(self):
        now = datetime.now(tz=timezone.utc)
        row = discovery_row(
            source_type="hive",
            object_type="hive_table",
            object_name="hive_metastore.db.t",
            catalog_name="hive_metastore",
            schema_name="db",
            discovered_at=now,
            data_category="hive_external",
            table_type="EXTERNAL",
            provider="DELTA",
            storage_location="abfss://...",
        )
        assert row["source_type"] == "hive"
        assert row["is_dlt_managed"] is None
        assert row["data_category"] == "hive_external"
        assert row["storage_location"] == "abfss://..."

    def test_discovery_row_metadata_json_encoded(self):
        now = datetime.now(tz=timezone.utc)
        row = discovery_row(
            source_type="uc",
            object_type="mv",
            object_name="cat.sch.mv1",
            catalog_name="cat",
            schema_name="sch",
            discovered_at=now,
            metadata={"pipeline_id": "abc123", "is_sql_created": True},
        )
        import json

        assert json.loads(row["metadata_json"]) == {"pipeline_id": "abc123", "is_sql_created": True}

    def test_discovery_schema_is_callable(self):
        # pyspark.sql.types is mocked in unit tests (see conftest.py); verify
        # the function executes without error and returns the mocked StructType.
        # Real schema-field coverage is exercised by the DDL assertion in
        # test_init_tracking_tables_creates_schema above.
        assert discovery_schema() is not None


def test_get_unrestored_rls_cm_manifest_handles_malformed_filter_columns_json(monkeypatch, tmp_path):
    """Regression: malformed JSON in filter_columns must NOT raise NameError.

    Bug C1: tracking.py imports json as _json but except clauses reference
    `json.JSONDecodeError`. One bad row → NameError → restore poisoned.
    """
    from common.tracking import TrackingManager
    from unittest.mock import MagicMock

    config = MagicMock()
    config.tracking_catalog = "main"
    config.tracking_schema = "cp_migration_tracking"

    spark = MagicMock()
    bad_row = MagicMock()
    bad_row.table_fqn = "c.s.t"
    bad_row.filter_fn_fqn = None
    bad_row.filter_columns = "{not json"
    bad_row.masks_json = "[]"
    bad_row.stripped_at = None
    bad_row.restore_failed_at = None
    bad_row.restore_error = None
    bad_row.run_id = "r1"
    spark.sql.return_value.collect.return_value = [bad_row]

    tm = TrackingManager(spark, config)
    result = tm.get_unrestored_rls_cm_manifest()

    assert result == [{"table_fqn": "c.s.t", "filter_fn_fqn": None,
                       "filter_columns": [], "masks": [],
                       "stripped_at": None, "restore_failed_at": None,
                       "restore_error": None, "run_id": "r1"}]
