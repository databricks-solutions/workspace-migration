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
        # ``skipped_target_exists`` (X.4),
        # ``skipped_by_stateful_service_migration`` (streaming tables /
        # future Stateful Services Phase), and ``failed_batch_oversize``
        # (H6: object exceeds MAX_BATCH_BYTES — operator must trim) are
        # terminal. Other skip statuses (skipped_by_config,
        # skipped_by_rls_cm_policy, plain skipped) re-enter pending so
        # operators can flip config flags and re-run.
        assert (
            "status NOT IN ('validated', 'skipped_by_pipeline_migration', "
            "'skipped_target_exists', 'skipped_by_stateful_service_migration', "
            "'failed_batch_oversize')"
            in sql_arg
        )
        # object_type is passed via args= (parameterized), not interpolated
        assert "managed_table" not in sql_arg
        assert mock_spark.sql.call_args.kwargs["args"] == {"obj_type": "managed_table"}

        # Verify the result is a list of dicts from collect()
        assert len(result) == 1
        assert result[0]["object_name"] == "catalog.schema.table1"

    def test_get_pending_objects_quote_safe(self, mock_spark, mock_config):
        """object_type with embedded quote must travel via args= (not inlined)."""
        mgr = TrackingManager(mock_spark, mock_config)
        mock_result = MagicMock()
        mock_result.collect.return_value = []
        mock_spark.sql.return_value = mock_result

        mgr.get_pending_objects("manage'd_table")

        sql_arg = mock_spark.sql.call_args[0][0]
        assert "manage'd_table" not in sql_arg
        assert mock_spark.sql.call_args.kwargs["args"]["obj_type"] == "manage'd_table"

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
        # object_type and object_name are parameterized — not inlined
        assert "managed_table" not in sql_arg
        assert "`cat`.`sch`.`t`" not in sql_arg
        assert "LIMIT 1" in sql_arg
        assert mock_spark.sql.call_args.kwargs["args"] == {
            "obj_type": "managed_table",
            "obj_name": "`cat`.`sch`.`t`",
        }
        assert result is not None
        assert result["object_name"] == "`cat`.`sch`.`t`"
        assert result["create_statement"] == "CREATE TABLE ..."

    def test_get_row_quote_safe(self, mock_spark, mock_config):
        """object_name with embedded quote must travel via args=."""
        mgr = TrackingManager(mock_spark, mock_config)
        mock_result = MagicMock()
        mock_result.collect.return_value = []
        mock_spark.sql.return_value = mock_result

        mgr.get_row("table", "cat.sch.O'Reilly")

        sql_arg = mock_spark.sql.call_args[0][0]
        assert "O'Reilly" not in sql_arg
        assert mock_spark.sql.call_args.kwargs["args"] == {
            "obj_type": "table",
            "obj_name": "cat.sch.O'Reilly",
        }

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
        # Services Phase, separate job). ``failed_batch_oversize`` (H6)
        # is terminal because re-picking would just fail again — operator
        # must trim heavy metadata.
        assert (
            "status NOT IN ('validated', 'skipped_by_pipeline_migration', "
            "'skipped_target_exists', 'skipped_by_stateful_service_migration', "
            "'failed_batch_oversize')"
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


def test_init_creates_rls_cm_staging_manifest_table(mock_spark, mock_config):
    """Path A: init_tracking_tables must create rls_cm_staging_manifest
    table in tracking_catalog.tracking_schema with the expected schema."""
    mgr = TrackingManager(mock_spark, mock_config)
    mgr.init_tracking_tables()

    sql_calls = [c.args[0] for c in mock_spark.sql.call_args_list]
    staging_create = next(
        (s for s in sql_calls if "rls_cm_staging_manifest" in s and "CREATE TABLE IF NOT EXISTS" in s),
        None,
    )
    assert staging_create is not None, "rls_cm_staging_manifest CREATE missing"
    assert "original_fqn STRING NOT NULL" in staging_create
    assert "staging_fqn STRING NOT NULL" in staging_create
    assert "created_at TIMESTAMP" in staging_create
    assert "dropped_at TIMESTAMP" in staging_create
    assert "drop_failed_at TIMESTAMP" in staging_create
    assert "drop_error STRING" in staging_create
    assert "run_id STRING" in staging_create


def test_init_creates_cp_migration_staging_schema(mock_spark, mock_config):
    """Path A: staging tables live in tracking_catalog.cp_migration_staging,
    not tracking_schema. Must create the schema."""
    mgr = TrackingManager(mock_spark, mock_config)
    mgr.init_tracking_tables()

    sql_calls = [c.args[0] for c in mock_spark.sql.call_args_list]
    staging_schema_create = next(
        (s for s in sql_calls if "cp_migration_staging" in s and "CREATE SCHEMA IF NOT EXISTS" in s),
        None,
    )
    assert staging_schema_create is not None, "cp_migration_staging schema CREATE missing"
    # Positive: schema is created at tracking_catalog.cp_migration_staging
    # (with or without backticks).
    assert (
        "migration_tracking.cp_migration_staging" in staging_schema_create
        or "`migration_tracking`.`cp_migration_staging`" in staging_schema_create
    )
    # Negative: schema must NOT be nested inside tracking_schema. Catches
    # the bug where the implementation accidentally uses ``self._fqn``
    # (catalog.schema) instead of ``self._catalog`` as the parent.
    assert "migration_tracking.cp_migration.cp_migration_staging" not in staging_schema_create
    assert (
        "`migration_tracking`.`cp_migration`.`cp_migration_staging`"
        not in staging_schema_create
    )


class TestStagingManifest:
    """Tests for the Path A staging-manifest helpers on TrackingManager."""

    def test_record_staging_created_inserts_row(self, mock_spark, mock_config):
        tm = TrackingManager(mock_spark, mock_config)
        mock_spark.sql.reset_mock()  # ignore CREATE statements from init

        tm.record_staging_created(
            original_fqn="`c`.`s`.`t`",
            staging_fqn="`tcat`.`cp_migration_staging`.`stg_abc123`",
            run_id="r-1",
        )
        sql = mock_spark.sql.call_args_list[-1].args[0]
        assert "INSERT INTO" in sql
        assert "rls_cm_staging_manifest" in sql
        assert "`c`.`s`.`t`" in sql
        assert "stg_abc123" in sql
        assert "r-1" in sql

    def test_record_staging_created_escapes_quotes(self, mock_spark, mock_config):
        tm = TrackingManager(mock_spark, mock_config)
        mock_spark.sql.reset_mock()
        tm.record_staging_created(
            original_fqn="o'reilly",
            staging_fqn="stg_xyz",
            run_id="r'1",
        )
        sql = mock_spark.sql.call_args_list[-1].args[0]
        assert "o''reilly" in sql
        assert "r''1" in sql

    def test_mark_staging_dropped_updates_dropped_at(self, mock_spark, mock_config):
        tm = TrackingManager(mock_spark, mock_config)
        mock_spark.sql.reset_mock()
        tm.mark_staging_dropped(staging_fqn="`tcat`.`cp_migration_staging`.`stg_abc`")
        sql = mock_spark.sql.call_args_list[-1].args[0]
        assert "UPDATE" in sql
        assert "rls_cm_staging_manifest" in sql
        assert "dropped_at = current_timestamp()" in sql
        assert "stg_abc" in sql
        assert "dropped_at IS NULL" in sql

    def test_mark_staging_drop_failed_updates_error(self, mock_spark, mock_config):
        tm = TrackingManager(mock_spark, mock_config)
        mock_spark.sql.reset_mock()
        tm.mark_staging_drop_failed(staging_fqn="stg_abc", error_message="boom")
        sql = mock_spark.sql.call_args_list[-1].args[0]
        assert "drop_failed_at = current_timestamp()" in sql
        assert "drop_error = 'boom'" in sql
        assert "stg_abc" in sql

    def test_get_active_stagings_returns_undropped_rows(self, mock_spark, mock_config):
        tm = TrackingManager(mock_spark, mock_config)
        mock_spark.sql.reset_mock()
        row = MagicMock()
        row.original_fqn = "`c`.`s`.`t`"
        row.staging_fqn = "stg_abc"
        row.created_at = None
        row.run_id = "r-1"
        mock_spark.sql.return_value.collect.return_value = [row]
        result = tm.get_active_stagings()
        assert result == [
            {"original_fqn": "`c`.`s`.`t`", "staging_fqn": "stg_abc",
             "created_at": None, "run_id": "r-1"},
        ]
        sql = mock_spark.sql.call_args_list[-1].args[0]
        assert "WHERE dropped_at IS NULL" in sql

    def test_get_staging_for_original_returns_staging_fqn(self, mock_spark, mock_config):
        tm = TrackingManager(mock_spark, mock_config)
        mock_spark.sql.reset_mock()
        row = MagicMock()
        row.staging_fqn = "stg_abc"
        mock_spark.sql.return_value.collect.return_value = [row]
        result = tm.get_staging_for_original("`c`.`s`.`t`")
        assert result == "stg_abc"

    def test_get_staging_for_original_returns_none_when_absent(self, mock_spark, mock_config):
        tm = TrackingManager(mock_spark, mock_config)
        mock_spark.sql.reset_mock()
        mock_spark.sql.return_value.collect.return_value = []
        result = tm.get_staging_for_original("`c`.`s`.`missing`")
        assert result is None
