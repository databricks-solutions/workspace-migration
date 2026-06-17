from __future__ import annotations

from typing import TYPE_CHECKING

from common.config import MigrationConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


def discovery_row(
    *,
    source_type: str,
    object_type: str,
    object_name: str,
    catalog_name: str | None,
    schema_name: str | None,
    discovered_at: object,
    row_count: int = 0,
    size_bytes: int = 0,
    is_dlt_managed: bool | None = None,
    pipeline_id: str | None = None,
    create_statement: str = "",
    data_category: str | None = None,
    table_type: str | None = None,
    provider: str | None = None,
    storage_location: str | None = None,
    format: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """Build a unified discovery_inventory row. metadata is JSON-encoded into metadata_json."""
    import json

    return {
        "object_name": object_name,
        "object_type": object_type,
        "source_type": source_type,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "row_count": row_count,
        "size_bytes": size_bytes,
        "is_dlt_managed": is_dlt_managed,
        "pipeline_id": pipeline_id,
        "create_statement": create_statement,
        "data_category": data_category,
        "table_type": table_type,
        "provider": provider,
        "storage_location": storage_location,
        "format": format,
        "metadata_json": json.dumps(metadata) if metadata else None,
        "discovered_at": discovered_at,
    }


def discovery_schema() -> StructType:
    """StructType used when writing to discovery_inventory."""
    from pyspark.sql.types import (
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("object_name", StringType(), True),
            StructField("object_type", StringType(), True),
            StructField("source_type", StringType(), True),
            StructField("catalog_name", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("row_count", LongType(), True),
            StructField("size_bytes", LongType(), True),
            StructField("is_dlt_managed", BooleanType(), True),
            StructField("pipeline_id", StringType(), True),
            StructField("create_statement", StringType(), True),
            StructField("data_category", StringType(), True),
            StructField("table_type", StringType(), True),
            StructField("provider", StringType(), True),
            StructField("storage_location", StringType(), True),
            StructField("format", StringType(), True),
            StructField("metadata_json", StringType(), True),
            StructField("discovered_at", TimestampType(), True),
        ]
    )


# Statuses that mark a migration row as terminal from the core tool's
# perspective — ``get_pending_objects`` filters these out so later runs do
# not re-pick-up the object. Extend here (not at the call site) to keep
# the terminal-state contract in one place. See also:
#   - docs/idempotency_audit.md (status taxonomy)
#   - docs/retry_resumability.md (reconciliation decision table)
#   - docs/stateful_services_phase.md (the stateful services scope boundary)
_TERMINAL_STATUSES: tuple[str, ...] = (
    "validated",
    "skipped_by_pipeline_migration",
    "skipped_target_exists",
    "skipped_by_stateful_service_migration",
    # H6: object exceeded MAX_BATCH_BYTES and was excluded from for_each
    # batches. Re-picking it would just fail again — operator must trim
    # heavy metadata (or split the object) and clear the status row.
    "failed_batch_oversize",
    # Vector Search (migrate_vector_search): index created on target, async
    # re-embedding still in progress — terminal so re-runs don't recreate.
    "created_resync_pending",
    # Direct Access VS index — vectors are external app state, can't recreate.
    "skipped_direct_access_unsupported",
    # Lakeflow Connect (migrate_lfc, Tier 1): pipeline recreated on target with
    # a per-table cursor row_filter (pulls only post-cutover rows). Terminal so
    # re-runs don't recreate it.
    "lfc_pipeline_created_incremental",
    # Lakeflow Connect: unified view created over <t>_history + <t>_incr.
    "lfc_view_created",
    # Lakeflow Connect (Tier-1 SaaS): the table has no resolvable incremental
    # cursor, so the recreated pipeline full-loads it and no unified view is built.
    # Terminal so re-runs don't reprocess it.
    "lfc_view_skipped_no_cursor",
    # Lakeflow Connect (Tier-2 CDC, gateway-based): the source gateway was
    # recreated on target (new id, target connection, mirrored staging). Terminal
    # so re-runs don't recreate a shared gateway twice.
    "lfc_gateway_created",
    # Lakeflow Connect (Tier-2 CDC): the ingestion pipeline was recreated on
    # target pointing at the new gateway, full-reload (no row_filter, not started).
    # The validate_only result is folded into this row's error_message (None when
    # validated; "created; validate inconclusive" otherwise). A genuine config
    # error instead records the ingestion pipeline as "failed".
    "lfc_pipeline_created_fullreload",
)
_TERMINAL_STATUSES_SQL = ", ".join(f"'{s}'" for s in _TERMINAL_STATUSES)


class TrackingManager:
    """Manages migration tracking tables for discovery, status, and pre-checks."""

    def __init__(self, spark: SparkSession, config: MigrationConfig) -> None:
        self.spark = spark
        self.config = config
        self._catalog = config.tracking_catalog
        self._schema = config.tracking_schema
        # Run-level job run id (review finding #7). Workers set this once in
        # run() via resolve_current_job_run_id(dbutils); append_migration_status
        # then stamps it on any status row that left job_run_id None, so
        # reconciliation's "only reset prior runs" guard can tell a current-run
        # in_progress row from a genuine prior-run orphan. None when not in a
        # Jobs context (interactive / pytest) — never invented.
        self.job_run_id: str | None = None

    @property
    def _fqn(self) -> str:
        return f"{self._catalog}.{self._schema}"

    def init_tracking_tables(self) -> None:
        """Create the tracking catalog, schema, and tables if they do not exist."""
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self._catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._fqn}")

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.discovery_inventory (
                object_name STRING,
                object_type STRING,
                source_type STRING,
                catalog_name STRING,
                schema_name STRING,
                row_count LONG,
                size_bytes LONG,
                is_dlt_managed BOOLEAN,
                pipeline_id STRING,
                create_statement STRING,
                data_category STRING,
                table_type STRING,
                provider STRING,
                storage_location STRING,
                format STRING,
                metadata_json STRING,
                discovered_at TIMESTAMP
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.migration_status (
                object_name STRING,
                object_type STRING,
                status STRING,
                error_message STRING,
                job_run_id STRING,
                task_run_id STRING,
                source_row_count LONG,
                target_row_count LONG,
                duration_seconds DOUBLE,
                migrated_at TIMESTAMP
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.pre_check_results (
                check_name STRING,
                status STRING,
                message STRING,
                action_required STRING,
                checked_at TIMESTAMP
            ) USING DELTA
        """)

        # Path A staging schema — staging table copies live here so the
        # tracking_schema only holds metadata, not user data. One schema
        # for all migrations against this tracking catalog.
        self.spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {self._catalog}.cp_migration_staging
        """)

        # Path A staging manifest — records the source→staging FQN mapping
        # for each table we copy as a substitute for stripping RLS/CM.
        # ``dropped_at`` is NULL until cleanup_staging removes the staging
        # table after migrate completes.
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.rls_cm_staging_manifest (
                original_fqn STRING NOT NULL,
                staging_fqn STRING NOT NULL,
                created_at TIMESTAMP,
                dropped_at TIMESTAMP,
                drop_failed_at TIMESTAMP,
                drop_error STRING,
                run_id STRING
            ) USING DELTA
        """)

    def write_discovery_inventory(self, df: DataFrame) -> None:
        """Upsert the discovery inventory with this run's DataFrame.

        Uses Delta MERGE INTO keyed on (object_name, object_type,
        source_type). Two concurrent migrations against the same
        tracking catalog no longer collide — each one upserts its
        own keys, and overlapping keys (e.g. same source workspace)
        end up with whichever run's scan landed last, not a failed
        DELTA_CONCURRENT_APPEND.

        Prior behaviour was ``mode("overwrite").saveAsTable(...)``
        which replaced the whole table per run. Side-effect: objects
        present in a previous scan but not this one were dropped.
        MERGE preserves them. Operators who want the old "fresh-scan-
        only" behaviour can truncate ``discovery_inventory`` before
        discovery runs.
        """
        # Stage new rows under a session-local temp view so we can
        # write the MERGE as pure SQL (Delta's Python merge API works
        # but leaves less tidy lineage).
        staging_view = "_staging_discovery_inventory"
        df.createOrReplaceTempView(staging_view)
        merge_cols = (
            "object_name",
            "object_type",
            "source_type",
            "catalog_name",
            "schema_name",
            "row_count",
            "size_bytes",
            "is_dlt_managed",
            "pipeline_id",
            "create_statement",
            "data_category",
            "table_type",
            "provider",
            "storage_location",
            "format",
            "metadata_json",
            "discovered_at",
        )
        key_cols = ("object_name", "object_type", "source_type")
        update_set = ", ".join(f"t.{c} = s.{c}" for c in merge_cols if c not in key_cols)
        insert_cols = ", ".join(merge_cols)
        insert_vals = ", ".join(f"s.{c}" for c in merge_cols)
        self.spark.sql(f"""
            MERGE INTO {self._fqn}.discovery_inventory t
            USING {staging_view} s
            ON  t.object_name  = s.object_name
            AND t.object_type  = s.object_type
            AND t.source_type  = s.source_type
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

    def _stamp_job_run_id(self, records: list[dict]) -> list[dict]:
        """Return copies of ``records`` with ``job_run_id`` filled from the
        run-level ``self.job_run_id`` wherever it was left None (review
        finding #7). An explicit per-record value is preserved; if no
        run-level id is set, None stays None (never invented)."""
        stamped: list[dict] = []
        for r in records:
            rr = dict(r)
            if rr.get("job_run_id") is None:
                rr["job_run_id"] = self.job_run_id
            stamped.append(rr)
        return stamped

    def append_migration_status(self, records: list[dict]) -> None:
        """Append migration status records with a current timestamp."""
        from pyspark.sql.functions import current_timestamp
        from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("object_name", StringType(), True),
                StructField("object_type", StringType(), True),
                StructField("status", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("job_run_id", StringType(), True),
                StructField("task_run_id", StringType(), True),
                StructField("source_row_count", LongType(), True),
                StructField("target_row_count", LongType(), True),
                StructField("duration_seconds", DoubleType(), True),
            ]
        )
        # Stamp the run-level job_run_id (#7) on the source records BEFORE
        # field projection — so an explicit value is preserved and a None is
        # filled, independent of the schema field list.
        records = self._stamp_job_run_id(records)
        # Only keep known fields; coerce missing to None
        field_names = [f.name for f in schema.fields]
        normalized = [{k: r.get(k) for k in field_names} for r in records]
        df = self.spark.createDataFrame(normalized, schema=schema)
        df = df.withColumn("migrated_at", current_timestamp())
        df.write.mode("append").saveAsTable(f"{self._fqn}.migration_status")

    def append_pre_check_results(self, records: list[dict]) -> None:
        """Append pre-check result records with a current timestamp."""
        from pyspark.sql.functions import current_timestamp

        df = self.spark.createDataFrame(records)
        df = df.withColumn("checked_at", current_timestamp())
        df.write.mode("append").saveAsTable(f"{self._fqn}.pre_check_results")

    def get_latest_migration_status(self) -> DataFrame:
        """Return the latest migration status per object (by object_name + object_type)."""
        return self.spark.sql(f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY object_name, object_type
                           ORDER BY migrated_at DESC
                       ) AS rn
                FROM {self._fqn}.migration_status
            )
            WHERE rn = 1
        """)

    def get_pending_objects(self, object_type: str) -> list[dict]:
        """Return discovery inventory objects that haven't reached a terminal status.

        Terminal statuses (object won't be reprocessed on re-run):
        - ``validated``: migrated successfully.
        - ``skipped_by_pipeline_migration``: DLT-owned; the pipeline is
          migrated by the pipelines workflow, never by managed_table_worker.
        - ``skipped_target_exists``: pre_check detected a collision with a
          pre-existing target object under the ``on_target_collision:
          skip`` policy (X.4). The target object is left untouched.
        - ``skipped_by_stateful_service_migration``: the object is out of
          scope for the core migration tool; the future Stateful Services
          Phase (separate job) will migrate it. Currently used by
          ``mv_st_worker`` for streaming tables. See
          ``docs/stateful_services_phase.md``.

        All other ``skipped_*`` statuses are **non-terminal by design** —
        they're flag-gated skips that an operator flips via config to
        re-enable:

        - ``skipped_by_config``: ``iceberg_strategy=""``. Flip to
          ``"ddl_replay"`` + re-run → table re-enters pending pool.
        - ``skipped_by_rls_cm_policy``: ``rls_cm_strategy=""``. Flip to
          ``"staging_copy"`` + re-run → same.
        - Plain ``skipped`` (dry_run output): real run must re-evaluate.

        Previously the filter was ``NOT LIKE 'skipped%'`` which treated
        every skip variant as terminal. That broke the Iceberg re-run
        scenario that ``skipped_by_config`` was introduced for.
        """
        rows = self.spark.sql(
            f"""
            WITH latest_status AS (
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY object_name, object_type
                               ORDER BY migrated_at DESC
                           ) AS rn
                    FROM {self._fqn}.migration_status
                )
                WHERE rn = 1
            )
            SELECT d.*
            FROM {self._fqn}.discovery_inventory d
            LEFT JOIN latest_status s
                ON d.object_name = s.object_name AND d.object_type = s.object_type
            WHERE d.object_type = :obj_type
              AND (s.status IS NULL
                   OR s.status NOT IN ({_TERMINAL_STATUSES_SQL}))
            """,
            args={"obj_type": object_type},
        ).collect()
        return [row.asDict() for row in rows]

    def get_row(self, object_type: str, object_name: str) -> dict | None:
        """Return a single discovery_inventory row by (object_type, object_name).

        Used by workers that receive lightweight identifier-only batches from
        the orchestrator and need to re-hydrate the full row (e.g. the
        ``create_statement`` field, which the orchestrator strips to keep
        for_each task-value payloads under Jobs' 3000-byte limit).
        """
        rows = self.spark.sql(
            f"""
            SELECT *
            FROM {self._fqn}.discovery_inventory
            WHERE object_type = :obj_type
              AND object_name = :obj_name
            LIMIT 1
            """,
            args={"obj_type": object_type, "obj_name": object_name},
        ).collect()
        return rows[0].asDict() if rows else None

    # ---------------------------------------------------------------- Staging manifest (Path A)

    def record_staging_created(
        self,
        *,
        original_fqn: str,
        staging_fqn: str,
        run_id: str,
    ) -> None:
        """Insert a staging-manifest row. Call AFTER the CTAS into
        cp_migration_staging succeeds so we never record a non-existent
        staging table."""
        self.spark.sql(
            f"""
            INSERT INTO {self._fqn}.rls_cm_staging_manifest
            SELECT
                '{original_fqn.replace("'", "''")}',
                '{staging_fqn.replace("'", "''")}',
                current_timestamp(),
                CAST(NULL AS TIMESTAMP),
                CAST(NULL AS TIMESTAMP),
                CAST(NULL AS STRING),
                '{run_id.replace("'", "''")}'
            """
        )

    def mark_staging_dropped(self, staging_fqn: str) -> None:
        """Mark a staging table dropped after cleanup_staging succeeded.
        Clears any prior drop_failed_at / drop_error from a previous attempt."""
        self.spark.sql(
            f"""
            UPDATE {self._fqn}.rls_cm_staging_manifest
            SET dropped_at = current_timestamp(),
                drop_failed_at = NULL,
                drop_error = NULL
            WHERE staging_fqn = '{staging_fqn.replace("'", "''")}'
              AND dropped_at IS NULL
            """
        )

    def mark_staging_drop_failed(self, staging_fqn: str, error_message: str) -> None:
        """Stamp drop_failed_at + drop_error so the next cleanup_staging run
        retries and operators can inspect failures."""
        safe = error_message.replace("'", "''")[:4000]
        self.spark.sql(
            f"""
            UPDATE {self._fqn}.rls_cm_staging_manifest
            SET drop_failed_at = current_timestamp(),
                drop_error = '{safe}'
            WHERE staging_fqn = '{staging_fqn.replace("'", "''")}'
              AND dropped_at IS NULL
            """
        )

    def get_active_stagings(self) -> list[dict]:
        """Return all staging rows still awaiting cleanup, oldest first.
        cleanup_staging iterates this list to drop staging tables."""
        rows = self.spark.sql(
            f"""
            SELECT original_fqn, staging_fqn, created_at, run_id
            FROM {self._fqn}.rls_cm_staging_manifest
            WHERE dropped_at IS NULL
            ORDER BY created_at ASC
            """
        ).collect()
        return [
            {
                "original_fqn": r.original_fqn,
                "staging_fqn": r.staging_fqn,
                "created_at": r.created_at,
                "run_id": r.run_id,
            }
            for r in rows
        ]

    def get_staging_for_original(self, original_fqn: str) -> str | None:
        """Look up the staging FQN for an original table FQN. Returns None
        if no active staging exists. managed_table_worker uses this to find
        the staging consumer-side path for DEEP CLONE."""
        rows = self.spark.sql(
            f"""
            SELECT staging_fqn
            FROM {self._fqn}.rls_cm_staging_manifest
            WHERE original_fqn = '{original_fqn.replace("'", "''")}'
              AND dropped_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).collect()
        return rows[0].staging_fqn if rows else None

    def get_tables_with_rls_cm(self) -> set[str]:
        """Return the set of table FQNs that have row filters or column masks
        recorded in discovery_inventory.

        Delta Sharing refuses to share tables with legacy RLS/CM, so
        ``setup_sharing`` uses this to filter the share payload when
        ``config.rls_cm_strategy`` is empty. ``row_filter`` rows have
        ``object_name`` set to the table FQN; ``column_mask`` rows have
        ``object_name`` set to ``<table_fqn>.<col>`` with the clean
        ``table_fqn`` stored in ``metadata_json``.
        """
        import json

        result: set[str] = set()
        rf_rows = self.spark.sql(f"""
            SELECT object_name FROM {self._fqn}.discovery_inventory
            WHERE object_type = 'row_filter'
        """).collect()
        result.update(r.object_name for r in rf_rows if r.object_name)
        cm_rows = self.spark.sql(f"""
            SELECT metadata_json FROM {self._fqn}.discovery_inventory
            WHERE object_type = 'column_mask'
        """).collect()
        for r in cm_rows:
            if r.metadata_json:
                try:
                    meta = json.loads(r.metadata_json)
                except json.JSONDecodeError:
                    continue
                tbl = meta.get("table_fqn")
                if tbl:
                    result.add(tbl)
        return result
