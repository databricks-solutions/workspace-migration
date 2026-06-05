# Databricks notebook source

# COMMAND ----------

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass  # not running under a Databricks notebook (e.g. pytest)

# COMMAND ----------

# Seed UC test data: create a small source catalog with table, view, function, volume.
# Pre-seed hygiene checks (S.14/S.15) were removed after a test run showed
# signs they introduced side-effects — the assertions they offered are
# covered by the unit tests for teardown behavior instead.

spark.sql("CREATE CATALOG IF NOT EXISTS integration_test_src")  # noqa: F821
spark.sql("CREATE SCHEMA IF NOT EXISTS integration_test_src.test_schema")  # noqa: F821
# --- 1.8 Multi-schema fixture ---
# A second schema under the same catalog with a distinct managed table.
# Discovery should enumerate both schemas; managed_table_worker should
# clone both. The assertion in test_uc_end_to_end verifies the target
# sees a validated row for the second-schema table — if discovery or
# the schema-level filter accidentally scopes to one schema, the row
# won't exist.
spark.sql("CREATE SCHEMA IF NOT EXISTS integration_test_src.test_schema_2")  # noqa: F821
# --- 1.8 Multi-catalog fixture ---
# A second source catalog with its own schema + managed table.
# Discovery's catalog enumeration (explorer.list_catalogs, filtered by
# tool_owned_catalogs) must enumerate BOTH test catalogs and discovery's
# for-loop over catalogs must migrate each one. If catalog iteration
# is accidentally scoped to the first match (or some index-based bug),
# the row for integration_test_src_b.extra_schema.extra_orders won't
# exist on target.
spark.sql("CREATE CATALOG IF NOT EXISTS integration_test_src_b")  # noqa: F821
spark.sql("CREATE SCHEMA IF NOT EXISTS integration_test_src_b.extra_schema")  # noqa: F821
spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE TABLE integration_test_src_b.extra_schema.extra_orders (
        order_id INT,
        region STRING
    ) USING DELTA
    """
)
spark.sql(  # noqa: F821
    """
    INSERT INTO integration_test_src_b.extra_schema.extra_orders VALUES
        (100, 'US'),
        (101, 'UK')
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE TABLE integration_test_src.test_schema_2.secondary_orders (
        order_id INT,
        amount DOUBLE
    ) USING DELTA
    """
)
spark.sql(  # noqa: F821
    """
    INSERT INTO integration_test_src.test_schema_2.secondary_orders VALUES
        (10, 10.0),
        (11, 20.0)
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE TABLE integration_test_src.test_schema.managed_orders (
        order_id INT,
        customer_id INT,
        amount DOUBLE,
        order_date DATE
    ) USING DELTA
    """
)
spark.sql(  # noqa: F821
    """
    INSERT INTO integration_test_src.test_schema.managed_orders VALUES
        (1, 100, 250.00, '2024-01-15'),
        (2, 200, 75.50, '2024-01-16')
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE VIEW integration_test_src.test_schema.high_value_orders AS
    SELECT * FROM integration_test_src.test_schema.managed_orders
    WHERE amount > 100
    """
)

# --- View-over-view (Phase 1 integration 1.10) ---
# View dependency ordering: ``recent_high_value`` references
# ``high_value_orders``, which references ``managed_orders``. The
# views worker must migrate them in topological order (leaves first)
# so each view's upstream exists on target when it's CREATE'd.
spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE VIEW integration_test_src.test_schema.recent_high_value AS
    SELECT * FROM integration_test_src.test_schema.high_value_orders
    WHERE order_date > '2024-01-01'
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE FUNCTION integration_test_src.test_schema.double_amount(x DOUBLE)
    RETURNS DOUBLE
    RETURN x * 2
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    "CREATE VOLUME IF NOT EXISTS integration_test_src.test_schema.test_volume"
)

# --- Partitioned external Delta table (Phase 1 integration 1.11) ---
# Source-side PARTITIONED BY must survive DDL replay + arrive on target
# with partition metadata intact. The unit test for the sanitizer +
# rewrite_ddl locks in the DDL shape; this integration verifies the
# round-trip on a live target.
_partitioned_location = "abfss://external-data@stextsourcemig36cd38.dfs.core.windows.net/partitioned_events"
_has_partitioned_external = False
try:
    spark.sql(  # noqa: F821
        f"""
        CREATE OR REPLACE TABLE integration_test_src.test_schema.partitioned_events (
            event_id INT,
            region STRING,
            event_date DATE,
            payload STRING
        )
        USING DELTA
        PARTITIONED BY (region, event_date)
        LOCATION '{_partitioned_location}'
        """
    )
    spark.sql(  # noqa: F821
        "INSERT OVERWRITE TABLE integration_test_src.test_schema.partitioned_events VALUES "
        "(1, 'US', DATE'2024-01-15', 'a'), "
        "(2, 'UK', DATE'2024-01-15', 'b'), "
        "(3, 'US', DATE'2024-01-16', 'c')"
    )
    _has_partitioned_external = True
    print("Created partitioned external table partitioned_events with 3 rows.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped partitioned external seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_partitioned_external",
    value="true" if _has_partitioned_external else "false",
)

# Seed a small file into the managed volume so Phase 2.5.A data-copy can be
# verified end-to-end. Byte count is surfaced as a task value so the assertion
# step knows what to expect on target.
_VOLUME_FILE_CONTENT = b"phase-2.5 integration-test marker file\n"
dbutils.fs.put(  # type: ignore[name-defined]  # noqa: F821
    "/Volumes/integration_test_src/test_schema/test_volume/marker.txt",
    _VOLUME_FILE_CONTENT.decode(),
    overwrite=True,
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="volume_marker_bytes", value=str(len(_VOLUME_FILE_CONTENT))
)

# COMMAND ----------

# --- Phase 2.5.C: Python UDF ---
spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE FUNCTION integration_test_src.test_schema.py_double(x DOUBLE)
    RETURNS DOUBLE
    LANGUAGE PYTHON
    AS $$
def handler(x):
    return x * 2

return handler(x)
    $$
    """
)


# COMMAND ----------

# --- Phase 2.5.B: UC-managed Iceberg table ---
# UC-managed native Iceberg is preview-feature-gated in some workspaces;
# guard so the seed doesn't fail the whole test when unavailable.
_has_iceberg = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE TABLE integration_test_src.test_schema.iceberg_sales (
            sale_id INT,
            customer_id INT,
            amount DOUBLE
        ) USING ICEBERG
        """
    )
    spark.sql(  # noqa: F821
        """
        INSERT INTO integration_test_src.test_schema.iceberg_sales VALUES
            (1, 100, 42.00),
            (2, 200, 19.95),
            (3, 300, 7.50)
        """
    )
    _has_iceberg = True
    print("Created Iceberg table iceberg_sales with 3 rows.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped Iceberg seed (unsupported on this runtime): {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_iceberg", value="true" if _has_iceberg else "false"
)

# COMMAND ----------

# --- Phase 3 governance fixtures ---

# Task 28 — Tags on a table and on a column
_has_tag = False
try:
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_orders SET TAGS ('env' = 'test', 'phase' = '3')"
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_orders ALTER COLUMN customer_id SET TAGS ('pii' = 'true')"
    )
    _has_tag = True
    print("Applied tags to managed_orders (table + column).")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped tag seed (unsupported on this runtime): {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_tag", value="true" if _has_tag else "false"
)

# COMMAND ----------

# Task 29 / 30 — Row filter + column mask applied to an EXTERNAL table.
#
# Delta Sharing refuses tables that carry row-level security or column masks
# (InvalidParameterValue), so applying these to a managed table breaks
# setup_sharing. External tables are migrated via DDL replay (not Delta
# Sharing), so they can carry filters/masks without blocking the share.
# The migration tool's setup_sharing → filter/mask workers chain covers
# external tables fine; discovery reads filter/mask metadata from
# information_schema regardless of table_type.

_external_customers_location = "abfss://external-data@stextsourcemig36cd38.dfs.core.windows.net/external_customers"

_has_row_filter = False
_has_column_mask = False
try:
    spark.sql(  # noqa: F821
        f"""
        CREATE OR REPLACE TABLE integration_test_src.test_schema.external_customers (
            customer_id INT,
            name STRING,
            region STRING
        )
        USING DELTA
        LOCATION '{_external_customers_location}'
        """
    )
    spark.sql(  # noqa: F821
        "INSERT INTO integration_test_src.test_schema.external_customers VALUES (1, 'Alice', 'US'), (2, 'Bob', 'UK')"
    )
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE FUNCTION integration_test_src.test_schema.region_filter(region STRING)
        RETURNS BOOLEAN
        RETURN region = 'US' OR is_account_group_member('admins')
        """
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.external_customers "
        "SET ROW FILTER integration_test_src.test_schema.region_filter ON (region)"
    )
    _has_row_filter = True
    print("Applied row filter region_filter on external_customers.region.")

    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE FUNCTION integration_test_src.test_schema.mask_customer(cid INT)
        RETURNS INT
        RETURN CASE WHEN is_account_group_member('admins') THEN cid ELSE -1 END
        """
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.external_customers "
        "ALTER COLUMN customer_id SET MASK integration_test_src.test_schema.mask_customer"
    )
    _has_column_mask = True
    print("Applied column mask mask_customer on external_customers.customer_id.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped row filter / column mask seed: {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_row_filter", value="true" if _has_row_filter else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_column_mask", value="true" if _has_column_mask else "false"
)

# COMMAND ----------

# --- RLS/CM skip-path fixture ---
# A managed Delta table with row filter AND column mask. Delta Sharing
# refuses to share these, so with rls_cm_strategy="" (default) the tool
# must skip the table and record status=skipped_by_rls_cm_policy. The
# companion assertion in test_uc_end_to_end.py verifies that.

_has_rls_cm_managed = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE TABLE integration_test_src.test_schema.managed_sensitive (
            record_id INT,
            account_id INT,
            region STRING
        ) USING DELTA
        """
    )
    spark.sql(  # noqa: F821
        "INSERT INTO integration_test_src.test_schema.managed_sensitive VALUES "
        "(1, 100, 'US'), (2, 200, 'UK'), (3, 300, 'US')"
    )
    # Reuse region_filter + mask_customer from above (both were created
    # in integration_test_src.test_schema).
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_sensitive "
        "SET ROW FILTER integration_test_src.test_schema.region_filter ON (region)"
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_sensitive "
        "ALTER COLUMN account_id SET MASK integration_test_src.test_schema.mask_customer"
    )
    _has_rls_cm_managed = True
    print("Applied row filter + column mask on managed_sensitive (managed table).")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped managed RLS/CM seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_rls_cm_managed", value="true" if _has_rls_cm_managed else "false"
)

# COMMAND ----------

# Task 32 — Comments on catalog/schema/table
try:
    spark.sql(  # noqa: F821
        "COMMENT ON CATALOG integration_test_src IS 'Phase 3 integration test catalog'"
    )
    spark.sql(  # noqa: F821
        "COMMENT ON SCHEMA integration_test_src.test_schema IS 'Phase 3 test schema'"
    )
    spark.sql(  # noqa: F821
        "COMMENT ON TABLE integration_test_src.test_schema.managed_orders "
        "IS 'Orders fixture — carries tags, row filter, and column mask'"
    )
    print("Applied comments on catalog/schema/table.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped comments seed: {_exc}")

# COMMAND ----------

# Grant the migration SPN permissions to read the source catalog.
from common.config import MigrationConfig  # noqa: E402

config = MigrationConfig.from_workspace_file()
if config.spn_client_id:
    for _cat in ("integration_test_src", "integration_test_src_b"):
        spark.sql(  # noqa: F821
            f"GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE, READ VOLUME ON CATALOG {_cat} TO `{config.spn_client_id}`"
        )
        print(f"Granted migration SPN {config.spn_client_id} perms on {_cat}.")

# Schema-level grant to a well-known principal (``account users``) so
# test_uc_end_to_end can assert a specific grant replays on target.
# ``account users`` exists on every Databricks account, so the grant is
# portable across test environments.
# Note: table-level grants are out of scope for grants_worker today
# (only CATALOG + SCHEMA grants migrate). Seeding at schema level so
# the assertion exercises a path the tool actually supports.
_has_schema_grant = False
try:
    spark.sql(  # noqa: F821
        "GRANT SELECT ON SCHEMA integration_test_src.test_schema TO `account users`"
    )
    _has_schema_grant = True
    print("Granted SELECT on test_schema to `account users`.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped schema-level grant seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_schema_grant", value="true" if _has_schema_grant else "false"
)

# COMMAND ----------

# --- 1.13 Grant to non-existent principal ---
# Seed a grant to a principal that is unlikely to exist on target
# (a deterministic bogus name). The grants_worker should either:
#   (a) migrate it successfully (UC accepts arbitrary string identifiers
#       without validating principal existence at grant time), or
#   (b) record status=failed with an operator-readable error.
# Either is acceptable; the assertion in test_uc_end_to_end verifies the
# row lands in migration_status and the status reflects one of the two,
# never silently disappearing.

_has_missing_principal_grant = False
_missing_principal = "nonexistent-group-integration-test-1a2b3c4d@example.invalid"
try:
    spark.sql(  # noqa: F821
        f"GRANT SELECT ON SCHEMA integration_test_src.test_schema TO `{_missing_principal}`"
    )
    _has_missing_principal_grant = True
    print(f"Seeded grant to missing principal {_missing_principal!r}.")
except Exception as _exc:  # noqa: BLE001
    # Some source metastores reject unknown principals at grant time;
    # leave the task value false so the assertion skips.
    print(f"Skipped missing-principal grant seed (source rejected): {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_missing_principal_grant",
    value="true" if _has_missing_principal_grant else "false",
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="missing_principal", value=_missing_principal
)

# COMMAND ----------
# --- 2.5.10: Managed volume with nested directory tree ---
# A second managed volume with files at /a/b/c/file.txt + /a/b/d/other.txt.
# The volume copy notebook recurses via dbutils.fs.cp, so nested paths must
# land on target with their directory structure preserved. Records the
# expected file count + total byte count as task values so the assertion
# can verify exact parity on target (not just "volume exists").
_NESTED_VOLUME_FILES = {
    "a/b/c/file.txt": b"nested-file-1: phase-2.5.10 marker\n",
    "a/b/d/other.txt": b"nested-file-2: second branch of directory tree\n",
    "a/top_level.txt": b"top-level-file in nested_volume root\n",
}
_has_nested_volume = False
try:
    spark.sql(  # noqa: F821
        "CREATE VOLUME IF NOT EXISTS integration_test_src.test_schema.nested_volume"
    )
    for _rel_path, _content in _NESTED_VOLUME_FILES.items():
        dbutils.fs.put(  # type: ignore[name-defined]  # noqa: F821
            f"/Volumes/integration_test_src/test_schema/nested_volume/{_rel_path}",
            _content.decode(),
            overwrite=True,
        )
    _has_nested_volume = True
    print(f"Seeded nested_volume with {len(_NESTED_VOLUME_FILES)} file(s) across a nested tree.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped nested volume seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_nested_volume", value="true" if _has_nested_volume else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="nested_volume_file_count", value=str(len(_NESTED_VOLUME_FILES))
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="nested_volume_total_bytes",
    value=str(sum(len(_b) for _b in _NESTED_VOLUME_FILES.values())),
)

# COMMAND ----------
# --- 2.5.9: Iceberg re-run fixture (skipped_by_config -> validated) ---
# A separate Iceberg table used exclusively to exercise the re-pickup
# contract after a prior run marked it ``skipped_by_config``. Approach:
#
#   1. Seed creates ``iceberg_replay_target`` (distinct from iceberg_sales
#      so the base Phase 2.5.B assertion is unaffected).
#   2. Seed ensures tracking tables exist, then INSERTs a synthetic
#      ``skipped_by_config`` row for it into migration_status with a
#      deliberately-past migrated_at timestamp. This simulates "a prior
#      run with iceberg_strategy=''".
#   3. The real migrate task runs with iceberg_strategy=``ddl_replay``
#      (set by setup_test_config). get_pending_objects' filter excludes
#      ('validated', 'skipped_by_pipeline_migration') — ``skipped_by_
#      config`` is NOT terminal, so the table re-enters the pending pool.
#   4. managed_table_worker migrates it and writes a new ``validated``
#      row with a later migrated_at.
#   5. Assertion: migration_status history for iceberg_replay_target
#      contains BOTH the seeded skipped_by_config row AND a later
#      validated row. get_latest_migration_status returns validated.

_has_iceberg_replay = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE TABLE integration_test_src.test_schema.iceberg_replay_target (
            record_id INT,
            payload STRING
        ) USING ICEBERG
        """
    )
    spark.sql(  # noqa: F821
        """
        INSERT INTO integration_test_src.test_schema.iceberg_replay_target VALUES
            (1, 'replay-row-1'),
            (2, 'replay-row-2')
        """
    )
    _has_iceberg_replay = True

    # Ensure tracking schema/tables exist before inserting into them.
    # Discovery.init_tracking_tables() runs a bit later in the workflow,
    # but creating the tables here is idempotent (CREATE IF NOT EXISTS).
    from common.tracking import TrackingManager  # noqa: E402

    _tracker_seed = TrackingManager(spark, config)  # noqa: F821
    _tracker_seed.init_tracking_tables()

    # Pre-insert a ``skipped_by_config`` row for the Iceberg table so the
    # subsequent migrate task sees it as a flag-gated skip and re-picks it
    # up. Use append_migration_status so the row shape matches exactly
    # what the worker writes (same schema, same current_timestamp-stamped
    # migrated_at; it'll be earlier than the post-migrate row written next).
    _tracker_seed.append_migration_status(
        [
            {
                "object_name": "`integration_test_src`.`test_schema`.`iceberg_replay_target`",
                "object_type": "managed_table",
                "status": "skipped_by_config",
                "error_message": (
                    "Iceberg migration not enabled. (seeded by integration test "
                    "for 2.5.9 re-run transition)"
                ),
                "job_run_id": None,
                "task_run_id": None,
                "source_row_count": None,
                "target_row_count": None,
                "duration_seconds": 0.0,
            }
        ]
    )
    print("Seeded iceberg_replay_target + synthetic skipped_by_config row for 2.5.9 re-run test.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped Iceberg re-run fixture seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_iceberg_replay", value="true" if _has_iceberg_replay else "false"
)

# COMMAND ----------

# --- 3.15: Tags on catalog / schema / volume ---
# PR #25 added unit coverage for tags_worker at catalog/schema/volume
# level; this integration fixture exercises the same path end-to-end.
# Discovery picks up each tag via list_tags(); tags_worker renders one
# ALTER per (securable_type, fqn, column) group and replays on target.

_has_non_table_tags = False
try:
    spark.sql(  # noqa: F821
        "ALTER CATALOG integration_test_src SET TAGS ('scope' = 'integration', 'tier' = 'gold')"
    )
    spark.sql(  # noqa: F821
        "ALTER SCHEMA integration_test_src.test_schema SET TAGS ('owner' = 'platform', 'pii' = 'false')"
    )
    spark.sql(  # noqa: F821
        "ALTER VOLUME integration_test_src.test_schema.test_volume SET TAGS ('purpose' = 'landing')"
    )
    _has_non_table_tags = True
    print("Applied tags to catalog / schema / volume (3.15 fixture).")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped non-table tag seed (unsupported on this runtime): {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_non_table_tags",
    value="true" if _has_non_table_tags else "false",
)

# COMMAND ----------

# --- 3.17: Column comment on managed_orders.amount + volume comment ---
# PR #25 fixed the COLUMN branch in comments_worker to emit
# ``ALTER TABLE ... ALTER COLUMN ... COMMENT '...'`` instead of the
# invalid ``COMMENT ON COLUMN``. The worker also now iterates volumes.
# This fixture exercises both paths end-to-end.

_has_column_comment = False
_has_volume_comment = False
try:
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_orders ALTER COLUMN amount COMMENT 'order amount in GBP'"
    )
    _has_column_comment = True
    print("Applied column comment on managed_orders.amount.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped column comment seed: {_exc}")
try:
    spark.sql(  # noqa: F821
        "COMMENT ON VOLUME integration_test_src.test_schema.test_volume IS 'Integration test landing volume'"
    )
    _has_volume_comment = True
    print("Applied comment on test_volume.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped volume comment seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_column_comment", value="true" if _has_column_comment else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_volume_comment", value="true" if _has_volume_comment else "false"
)

# COMMAND ----------

# --- 3.19: Registered model with 1 version + 1 alias (no artifacts) ---
# models_worker creates the shell, version metadata, and alias on target.
# No artifact URI is provided, so artifact copy is a no-op — the version
# is pure metadata. 3.20 (artifact copy) is covered in a parallel lane.

import contextlib  # noqa: E402

from databricks.sdk import WorkspaceClient  # noqa: E402

_w_seed = WorkspaceClient()
_has_registered_model = False
_MODEL_CATALOG = "integration_test_src"
_MODEL_SCHEMA = "test_schema"
_MODEL_NAME = "integration_test_model"
_MODEL_ALIAS = "champion"
try:
    # Idempotent: delete any pre-existing model so the seed is deterministic.
    with contextlib.suppress(Exception):
        _w_seed.registered_models.delete(f"{_MODEL_CATALOG}.{_MODEL_SCHEMA}.{_MODEL_NAME}")
    _w_seed.registered_models.create(
        catalog_name=_MODEL_CATALOG,
        schema_name=_MODEL_SCHEMA,
        name=_MODEL_NAME,
        comment="Integration test fixture — scoped metadata only, no artifacts",
    )
    # Create version with empty source (metadata only; artifact copy path
    # is 3.20, out of scope for this item).
    _created_version = _w_seed.model_versions.create(
        catalog_name=_MODEL_CATALOG,
        schema_name=_MODEL_SCHEMA,
        model_name=_MODEL_NAME,
        source="",  # intentionally empty — no artifact bytes to copy
    )
    _w_seed.registered_models.set_alias(
        full_name=f"{_MODEL_CATALOG}.{_MODEL_SCHEMA}.{_MODEL_NAME}",
        alias=_MODEL_ALIAS,
        version_num=int(_created_version.version),
    )
    _has_registered_model = True
    print(f"Created registered model {_MODEL_NAME} v{_created_version.version} with alias '{_MODEL_ALIAS}'.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped registered model seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_registered_model",
    value="true" if _has_registered_model else "false",
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="registered_model_fqn",
    value=f"{_MODEL_CATALOG}.{_MODEL_SCHEMA}.{_MODEL_NAME}",
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="registered_model_alias", value=_MODEL_ALIAS
)

# COMMAND ----------

# --- 3.24: Customer-defined Delta share + recipient ---
# Distinct from the tool's internal ``cp_migration_share``. Discovery's
# list_shares() excludes that one by name; the customer share flows
# through normally. sharing_worker.run() recreates the share shell +
# recipient on target, then ALTER SHARE ADDs each object.

_CUSTOMER_SHARE = "integration_test_customer_share"
_CUSTOMER_RECIPIENT = "integration_test_recipient"
_has_customer_share = False
try:
    # Clean up any leftovers from prior runs so the seed is deterministic.
    with contextlib.suppress(Exception):
        _w_seed.shares.delete(_CUSTOMER_SHARE)
    with contextlib.suppress(Exception):
        _w_seed.recipients.delete(_CUSTOMER_RECIPIENT)

    from databricks.sdk.service.sharing import AuthenticationType  # noqa: E402

    _w_seed.recipients.create(
        name=_CUSTOMER_RECIPIENT,
        authentication_type=AuthenticationType.TOKEN,
        comment="Integration test recipient (3.24)",
    )
    _w_seed.shares.create(
        name=_CUSTOMER_SHARE,
        comment="Integration test customer-defined share (3.24)",
    )
    # Add managed_orders to the share. sharing_worker uses ALTER SHARE ADD
    # TABLE on target, so we mirror that shape on source. Note: tables
    # with row filter / column mask are rejected by Delta Sharing, so we
    # use managed_orders (no RLS/CM) rather than external_customers.
    spark.sql(  # noqa: F821
        f"ALTER SHARE `{_CUSTOMER_SHARE}` ADD TABLE integration_test_src.test_schema.managed_orders"
    )
    # Transfer ownership of the share + recipient to the migration SPN.
    #
    # UC's ``shares.list()`` / ``recipients.list()`` only return objects
    # the caller owns or holds USE SHARE / USE RECIPIENT on. Discovery
    # runs under the migration SPN (via OAuth M2M in ``AuthManager``);
    # the seeder's ``_w_seed = WorkspaceClient()`` uses the notebook's
    # ambient credentials which are *usually* the same SPN (thanks to
    # ``run_as`` on the job) but can drift to the triggering user when
    # the job is kicked off manually. If that drifts, the customer
    # share silently vanishes from ``list_shares()`` output at
    # discovery time and no migration_status row is ever emitted for
    # ``SHARE_integration_test_customer_share`` (root cause of F.1).
    #
    # Ownership transfer is the strongest access bit (it implies USE
    # SHARE / USE RECIPIENT) and makes discovery deterministic
    # regardless of seeder identity. Swallow failures with
    # ``contextlib.suppress`` so a transient ALTER failure doesn't mask
    # the share-create success; if ownership didn't land and the SPN
    # can't see the share, the 3.24 hard assertion in
    # ``test_uc_end_to_end.py`` fails loud with a clear message.
    if config.spn_client_id:
        with contextlib.suppress(Exception):
            spark.sql(  # noqa: F821
                f"ALTER SHARE `{_CUSTOMER_SHARE}` OWNER TO `{config.spn_client_id}`"
            )
        with contextlib.suppress(Exception):
            spark.sql(  # noqa: F821
                f"ALTER RECIPIENT `{_CUSTOMER_RECIPIENT}` OWNER TO `{config.spn_client_id}`"
            )
        print(
            f"Transferred ownership of share '{_CUSTOMER_SHARE}' + recipient "
            f"'{_CUSTOMER_RECIPIENT}' to SPN {config.spn_client_id}."
        )
    _has_customer_share = True
    print(f"Created customer share '{_CUSTOMER_SHARE}' with recipient '{_CUSTOMER_RECIPIENT}'.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped customer share seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_customer_share", value="true" if _has_customer_share else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="customer_share_name", value=_CUSTOMER_SHARE
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="customer_recipient_name", value=_CUSTOMER_RECIPIENT
)

# COMMAND ----------

# COMMAND ----------

# --- 3.16 ABAC policy ---
# Attach a UC ABAC policy to ``managed_orders`` so discovery's policy
# enumeration (explorer.list_policies, PR #20) returns a row, the
# policies_worker POSTs it on target, and both discovery_inventory +
# migration_status land rows we can assert on.
#
# ABAC is a preview feature: the SET ABAC POLICY SQL form is accepted
# only on certain runtimes / workspaces. We attempt it; if the runtime
# rejects it we surface a task-value flag (``has_abac=false``) so the
# downstream assertion skips cleanly rather than failing the whole
# integration run. The filter function itself uses a trivial BOOLEAN
# expression (no external dependencies) so its existence is not the
# variable under test — only the ABAC attach step is.

_has_abac = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE FUNCTION integration_test_src.test_schema.abac_filter(region STRING)
        RETURNS BOOLEAN
        RETURN region IS NOT NULL
        """
    )
    # The SET ABAC POLICY shape is the canonical attach form. Policy
    # name is scoped to the securable so it's fine to reuse a fixed
    # value across re-runs — idempotent via CREATE OR REPLACE shape
    # where supported; otherwise we swallow ``already exists``.
    spark.sql(  # noqa: F821
        """
        ALTER TABLE integration_test_src.test_schema.managed_orders
        SET ABAC POLICY abac_orders_policy
        COMMENT 'integration-test ABAC policy'
        MATCH columns (region)
        ON ROW FILTER integration_test_src.test_schema.abac_filter
        """
    )
    _has_abac = True
    print("Applied ABAC policy abac_orders_policy to managed_orders.")
except Exception as _exc:  # noqa: BLE001
    # Some workspace runtimes reject the SET ABAC POLICY syntax entirely;
    # others only reject specific clauses. Either way, the assertion path
    # is gated — no need to fail the whole seed.
    print(f"Skipped ABAC seed (unsupported on this runtime): {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_abac", value="true" if _has_abac else "false"
)

# COMMAND ----------

# --- 3.20 Model artifacts ---
# Create a registered model + one version with a real artifact file at
# the version's allocated ``storage_location``. models_worker on main
# (PR #21) calls ``run_target_file_copy`` to copy bytes from the
# version's source URI to the target version's storage path and surfaces
# the byte/file counts in ``migration_status.error_message`` as
# ``"N file(s), M byte(s) copied."``.
#
# The source URI MUST be one the target SPN can read. We use the
# version's own storage_location on source (``dbfs:/Volumes/...`` via
# UC-managed model storage), seed a ``requirements.txt`` file there,
# and set ``source`` to that same URI — so the target copy pulls from
# a UC-governed location rather than a run URI.
#
# Gated behind a task value (``has_model_artifacts``) so a workspace
# without UC model support, or one where the registered_models API
# rejects ``storage_location`` overrides, doesn't fail the integration.

_has_model_artifacts = False
_model_fqn = "integration_test_src.test_schema.integration_test_model"
_model_artifact_bytes = b"# integration-test requirements.txt\nrequests==2.31.0\n"
try:
    from databricks.sdk import WorkspaceClient as _WorkspaceClient  # noqa: E402

    _wsc = _WorkspaceClient()
    # Shell model
    try:
        _wsc.registered_models.create(
            catalog_name="integration_test_src",
            schema_name="test_schema",
            name="integration_test_model",
            comment="Phase 3 integration test model",
        )
    except Exception as _exc:  # noqa: BLE001
        if "already" not in str(_exc).lower() or "exists" not in str(_exc).lower():
            raise
    # One version — the source URI must be readable by the target SPN.
    # We create the version FIRST with a placeholder source so UC
    # allocates ``storage_location`` on the source side, seed the
    # artifact bytes there via ``dbutils.fs.put``, then (for clarity
    # in the downstream assertion) capture the actual storage_location
    # and re-publish it as the version's source.
    _created = _wsc.model_versions.create(
        catalog_name="integration_test_src",
        schema_name="test_schema",
        model_name="integration_test_model",
        source="dbfs:/tmp/integration_test_model_placeholder",
    )
    _version_storage = getattr(_created, "storage_location", None) or ""
    if not _version_storage:
        raise RuntimeError(
            "UC did not return storage_location for the new model version — "
            "cannot stage an artifact the target SPN can read."
        )
    # Seed the artifact file.
    dbutils.fs.put(  # type: ignore[name-defined]  # noqa: F821
        _version_storage.rstrip("/") + "/requirements.txt",
        _model_artifact_bytes.decode(),
        overwrite=True,
    )
    # Re-read the version so we have the authoritative source URL for
    # models_worker to copy from. In the `source` field, point at the
    # same storage_location — models_worker prefers ``storage_location``
    # over ``source`` anyway, so either works.
    _has_model_artifacts = True
    print(
        f"Seeded model artifact ({len(_model_artifact_bytes)} bytes) at "
        f"{_version_storage}/requirements.txt."
    )
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped model artifact seed: {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_model_artifacts",
    value="true" if _has_model_artifacts else "false",
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="model_artifact_bytes", value=str(len(_model_artifact_bytes))
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="model_fqn", value=_model_fqn
)

# COMMAND ----------

print("UC seed data created successfully.")
