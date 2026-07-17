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
    pass

# COMMAND ----------

# Seed Hive test data: managed DBFS-root table + view + function under
# hive_metastore.integration_test_hive. Also grants the migration SPN the
# target-side permissions needed to register UC external tables during the
# DBFS-root migration path.

from common.auth import AuthManager  # noqa: E402
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from migrate.hive_common import configure_adls_account_key

config = MigrationConfig.from_workspace_file()




# COMMAND ----------

# Clean any stale fixtures from a prior interrupted run. DBFS-root managed-table
# files can be orphaned when a run is cancelled: teardown_hive runs on serverless
# and can't always delete DBFS-root managed-table data, so a later
# CREATE TABLE fails with "location is not empty and also not a Delta table".
# This seed runs on a classic cluster (hive_adls_classic) that CAN delete those
# files, so drop the database CASCADE here (clears managed locations) and rm the
# warehouse dir for good measure before re-creating the fixtures.
import contextlib  # noqa: E402

spark.sql("DROP DATABASE IF EXISTS hive_metastore.integration_test_hive CASCADE")  # noqa: F821
# The managed-non-DBFS db lives on ADLS; drop it too (this classic cluster can
# reach the abfss data to clean it).
spark.sql("DROP DATABASE IF EXISTS hive_metastore.integration_test_hive_nd CASCADE")  # noqa: F821
with contextlib.suppress(Exception):
    dbutils.fs.rm("dbfs:/user/hive/warehouse/integration_test_hive.db", True)  # type: ignore[name-defined]  # noqa: F821

spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.integration_test_hive")  # noqa: F821

# COMMAND ----------
# Clean-slate the Hive tracking rows at the START of the run (finding H3).
#
# test_hive reads TrackingManager.get_latest_migration_status(), which returns
# the latest row per (object_name, object_type) across ALL history — it is NOT
# scoped to this run's job_run_id. Orphaned rows from a bygone fixture naming
# (e.g. a prior lab seed that used different db/table names) are for objects
# THIS run never re-migrates, so the latest-row window never supersedes them,
# AND teardown's ``LIKE '%integration_test_hive%'`` delete never matches them —
# so they linger forever and poison the coverage assertion with stale
# 'failed' rows. Deleting every source_type='hive' row here makes each run
# start from a clean slate, so the assertion only ever sees this run's rows.
# On a pristine workspace this is a harmless no-op; the try/except tolerates
# the tracking tables not existing yet (discovery creates them after seed).
_hive_tracking_reset = (
    f"DELETE FROM {config.tracking_catalog}.{config.tracking_schema}.migration_status "
    f"WHERE object_type LIKE 'hive_%'"
)
_hive_disc_reset = (
    f"DELETE FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
    f"WHERE source_type = 'hive'"
)
try:
    _reset_auth = AuthManager(config, dbutils)  # noqa: F821
    _reset_wh = find_warehouse(_reset_auth)
    for _sql in (_hive_tracking_reset, _hive_disc_reset):
        _r = execute_and_poll(_reset_auth, _reset_wh, _sql)
        if _r["state"] != "SUCCEEDED":
            # Missing table (first run before discovery ever ran) is expected.
            print(f"Hive tracking clean-slate skipped ({_r.get('error', _r['state'])}).")
    print("Cleared prior hive_* rows from tracking tables (H3 clean-slate).")
except Exception as _reset_exc:  # noqa: BLE001
    print(f"Hive tracking clean-slate skipped: {_reset_exc}")

# Managed Delta table on DBFS root — default for Hive CREATE TABLE
spark.sql(  # noqa: F821
    """
    CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.managed_orders (
        order_id INT,
        amount DOUBLE
    ) USING DELTA
    """
)
spark.sql(  # noqa: F821
    """
    INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.managed_orders VALUES
        (1, 10.0),
        (2, 20.0),
        (3, 30.0)
    """
)

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE VIEW hive_metastore.integration_test_hive.big_orders AS
    SELECT * FROM hive_metastore.integration_test_hive.managed_orders WHERE amount > 15
    """
)

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE FUNCTION hive_metastore.integration_test_hive.triple(x DOUBLE)
    RETURNS DOUBLE
    RETURN x * 3
    """
)

# COMMAND ----------

# --- Hive external + managed non-DBFS fixtures ---
#
# Phase 2 has dedicated workers for these categories:
#   hive_external            — EXTERNAL table on any storage
#   hive_managed_nondbfs     — MANAGED table whose location is off DBFS root
#                              (legacy pattern; migrate preserving MANAGED on target)
#
# Before this fixture, both workers ran with empty input on every
# integration test — zero coverage. We seed one row per category here
# so both workflows exercise at least the happy path.
#
# The ADLS path reuses the external location used by DBFS-root
# migration, gated on the same config fields.

_ND_DB = "integration_test_hive_nd"  # dedicated db with a non-DBFS LOCATION
_hive_external_location: str | None = None
_hive_nondbfs_db_location: str | None = None
if config.migrate_hive_dbfs_root and config.hive_dbfs_staging_path:
    _base = config.hive_dbfs_staging_path.rstrip("/")
    _hive_external_location = f"{_base}/hive_external_invoices"
    _hive_nondbfs_db_location = f"{_base}/hive_nondbfs_db"

_has_hive_external = False
_has_hive_nondbfs = False

# hive_metastore tables at an abfss LOCATION need the legacy account-key Hadoop
# conf (UC vending doesn't cover hive_metastore LOCATION). Set it at runtime
# from secret migration/adls-account-key — only works on classic compute (this
# task runs on the hive_adls_classic cluster); no-op/warns on serverless, in
# which case the CREATEs below fail and has_*=false (guard exempts with reason).
if _hive_external_location:
    configure_adls_account_key(spark, dbutils, _hive_external_location)  # noqa: F821

if _hive_external_location:
    try:
        # EXTERNAL table — CREATE TABLE ... LOCATION makes it external in
        # hive_metastore (the LOCATION clause flips table_type to EXTERNAL).
        spark.sql(  # noqa: F821
            f"""
            CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.external_invoices (
                invoice_id INT,
                amount DOUBLE
            )
            USING DELTA
            LOCATION '{_hive_external_location}'
            """
        )
        spark.sql(  # noqa: F821
            """
            INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.external_invoices VALUES
                (101, 100.0),
                (102, 200.0)
            """
        )
        _has_hive_external = True
        print(f"Created Hive external table at {_hive_external_location}.")
    except Exception as _exc:  # noqa: BLE001
        print(f"Skipped Hive external seed: {_exc}")

if _hive_nondbfs_db_location:
    try:
        # MANAGED non-DBFS: a managed table whose storage is off the DBFS root.
        # In hive_metastore, CREATE TABLE ... LOCATION makes an EXTERNAL table —
        # so the only way to get a MANAGED table at a non-default location is to
        # give the *database* a non-DBFS LOCATION, then create a managed table
        # (no LOCATION) that inherits it. categorize_hive_table then classifies
        # it as hive_managed_nondbfs (MANAGED + abfss location), not external.
        spark.sql(  # noqa: F821
            f"CREATE DATABASE IF NOT EXISTS hive_metastore.{_ND_DB} LOCATION '{_hive_nondbfs_db_location}'"
        )
        spark.sql(  # noqa: F821
            f"""
            CREATE TABLE IF NOT EXISTS hive_metastore.{_ND_DB}.nondbfs_sales (
                sale_id INT,
                amount DOUBLE
            )
            USING DELTA
            """
        )
        spark.sql(  # noqa: F821
            f"""
            INSERT OVERWRITE TABLE hive_metastore.{_ND_DB}.nondbfs_sales VALUES
                (201, 50.0),
                (202, 75.0),
                (203, 90.0)
            """
        )
        _has_hive_nondbfs = True
        print(f"Created Hive MANAGED non-DBFS table hive_metastore.{_ND_DB}.nondbfs_sales (db LOCATION {_hive_nondbfs_db_location}).")
    except Exception as _exc:  # noqa: BLE001
        print(f"!! Skipped Hive managed-nondbfs seed: {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_hive_external", value="true" if _has_hive_external else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_hive_nondbfs", value="true" if _has_hive_nondbfs else "false"
)

# COMMAND ----------

# --- Hive grants fixture ---
# Legacy hive_metastore supports GRANT / REVOKE on tables. Seed a grant on
# managed_orders so hive_grants_worker has something to migrate and the
# assertion in test_hive_end_to_end can verify it replays on target.

_has_hive_grant = False
try:
    spark.sql(  # noqa: F821
        "GRANT SELECT ON SCHEMA hive_metastore.integration_test_hive TO `account users`"
    )
    _has_hive_grant = True
    print("Granted SELECT on Hive schema to `account users`.")
except Exception as _exc:  # noqa: BLE001
    # Some clusters have Hive ACLs disabled (table ACLs on legacy); tolerate.
    print(f"Skipped Hive grant seed (table ACLs may be disabled): {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_hive_grant", value="true" if _has_hive_grant else "false"
)

# COMMAND ----------

# Grant the SPN CREATE EXTERNAL TABLE on the target's external location that
# hosts hive_dbfs_staging_path. Without this, the DBFS-root worker's
# CREATE TABLE ... LOCATION '<hive_dbfs_staging_path>/...' fails with
# PERMISSION_DENIED on the target side.
if config.migrate_hive_dbfs_root and config.hive_dbfs_staging_path and config.spn_client_id:
    auth = AuthManager(config, dbutils)  # noqa: F821
    wh_id = find_warehouse(auth)
    # Find the external location covering hive_dbfs_staging_path on target.
    locs = list(auth.target_client.external_locations.list())
    matching = [loc for loc in locs if config.hive_dbfs_staging_path.startswith(loc.url)]
    if matching:
        loc_name = matching[0].name
        grant_sql = (
            f"GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES "
            f"ON EXTERNAL LOCATION `{loc_name}` TO `{config.spn_client_id}`"
        )
        res = execute_and_poll(auth, wh_id, grant_sql)
        if res["state"] == "SUCCEEDED":
            print(f"Granted CREATE EXTERNAL TABLE on '{loc_name}' to SPN.")
        else:
            print(f"WARNING: grant on external location '{loc_name}' failed: {res.get('error')}")
    else:
        print(
            f"WARNING: No external location on target covers {config.hive_dbfs_staging_path!r}. "
            f"DBFS-root migration will likely fail with PERMISSION_DENIED."
        )

# COMMAND ----------
# --- Phase 2 integration extras (items 2.10 – 2.15) ---
#
# Each fixture is independently gated so individual failures don't sink
# the whole seed; the corresponding assertion in test_hive_end_to_end.py
# reads the task value and either asserts or skips.

# --- 2.10 multi-upstream view ---
# Hive view that JOINs two source tables. Exercises topological
# dependency tracking when a view has > 1 table upstream.

_has_multi_upstream_view = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.managed_products (
            product_id INT,
            category STRING
        ) USING DELTA
        """
    )
    spark.sql(  # noqa: F821
        """
        INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.managed_products VALUES
            (1, 'books'),
            (2, 'electronics'),
            (3, 'grocery')
        """
    )
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE VIEW hive_metastore.integration_test_hive.product_orders AS
        SELECT o.order_id, o.amount, p.category
        FROM hive_metastore.integration_test_hive.managed_orders o
        JOIN hive_metastore.integration_test_hive.managed_products p
          ON o.order_id = p.product_id
        """
    )
    _has_multi_upstream_view = True
    print("2.10 seeded: managed_products + product_orders view (multi-upstream).")
except Exception as _exc:  # noqa: BLE001
    print(f"2.10 skipped: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_multi_upstream_view", value="true" if _has_multi_upstream_view else "false"
)


# COMMAND ----------
# --- 2.12 DBFS-root managed table with nested partition directories ---
# Partitioned Delta-on-DBFS-root — storage layout has `year=.../month=...`
# nested directories. Verifies DEEP CLONE copies all partition files and
# row counts survive.

_has_partitioned_dbfs = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.partitioned_orders (
            order_id INT,
            amount DOUBLE,
            year INT,
            month INT
        ) USING DELTA
        PARTITIONED BY (year, month)
        """
    )
    spark.sql(  # noqa: F821
        """
        INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.partitioned_orders VALUES
            (1, 10.0, 2025, 1),
            (2, 20.0, 2025, 1),
            (3, 30.0, 2025, 2),
            (4, 40.0, 2026, 1),
            (5, 50.0, 2026, 2),
            (6, 60.0, 2026, 2)
        """
    )
    _has_partitioned_dbfs = True
    print("2.12 seeded: partitioned_orders (DBFS-root, PARTITIONED BY year, month).")
except Exception as _exc:  # noqa: BLE001
    print(f"2.12 skipped: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_partitioned_dbfs", value="true" if _has_partitioned_dbfs else "false"
)

# COMMAND ----------
# --- 2.13 external PARTITIONED BY ---
# External Hive table with explicit PARTITIONED BY on an ADLS location.
# Exercises hive_external_worker's partitioned-table replay path.

_has_partitioned_external = False
_hive_external_partitioned_location: str | None = None
if config.migrate_hive_dbfs_root and config.hive_dbfs_staging_path:
    _base = config.hive_dbfs_staging_path.rstrip("/")
    _hive_external_partitioned_location = f"{_base}/hive_external_partitioned_sales"

if _hive_external_partitioned_location:
    try:
        spark.sql(  # noqa: F821
            f"""
            CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.external_partitioned_sales (
                sale_id INT,
                amount DOUBLE,
                region STRING,
                year INT
            )
            USING DELTA
            PARTITIONED BY (region, year)
            LOCATION '{_hive_external_partitioned_location}'
            """
        )
        spark.sql(  # noqa: F821
            """
            INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.external_partitioned_sales VALUES
                (1, 100.0, 'emea', 2025),
                (2, 150.0, 'emea', 2026),
                (3, 200.0, 'amer', 2025),
                (4, 250.0, 'amer', 2026),
                (5, 300.0, 'apac', 2026)
            """
        )
        _has_partitioned_external = True
        print(
            f"2.13 seeded: external_partitioned_sales at {_hive_external_partitioned_location}."
        )
    except Exception as _exc:  # noqa: BLE001
        print(f"2.13 skipped: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_partitioned_external", value="true" if _has_partitioned_external else "false"
)

# COMMAND ----------
# --- 2.14 orchestrator batch sizing (>batch_size managed tables) ---
# Seed enough tables to force the orchestrator to emit multiple batches.
# The Hive integration workflow sets batch_size=10 via setup_test_config;
# 12 tables here guarantees 2 batches (10 + 2), exercising the range loop
# in hive_orchestrator.build_batches.

_BATCH_TABLE_COUNT = 12
_batch_tables_seeded = 0
try:
    for _i in range(_BATCH_TABLE_COUNT):
        _tbl = f"batch_tbl_{_i:02d}"
        spark.sql(  # noqa: F821
            f"""
            CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.{_tbl} (
                id INT,
                val DOUBLE
            ) USING DELTA
            """
        )
        spark.sql(  # noqa: F821
            f"INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.{_tbl} VALUES ({_i}, {_i * 1.5})"
        )
        _batch_tables_seeded += 1
    print(f"2.14 seeded: {_batch_tables_seeded} managed tables for batch-size exercising.")
except Exception as _exc:  # noqa: BLE001
    print(f"2.14 partial: {_batch_tables_seeded}/{_BATCH_TABLE_COUNT} seeded, stopped at: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="batch_tables_seeded", value=str(_batch_tables_seeded)
)

# COMMAND ----------
# --- 2.15 view dependency ordering (3-level chain) ---
# view_level1 (base on managed_orders) → view_level2 (depends on level1)
# → view_level3 (depends on level2). Seed creation order is inverse of
# dependency so the orchestrator's topological sort has real work to do.

_has_view_chain = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE VIEW hive_metastore.integration_test_hive.view_level1 AS
        SELECT order_id, amount FROM hive_metastore.integration_test_hive.managed_orders
        WHERE amount > 5
        """
    )
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE VIEW hive_metastore.integration_test_hive.view_level2 AS
        SELECT order_id, amount * 2 AS doubled_amount
        FROM hive_metastore.integration_test_hive.view_level1
        """
    )
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE VIEW hive_metastore.integration_test_hive.view_level3 AS
        SELECT order_id, doubled_amount * 2 AS quadrupled_amount
        FROM hive_metastore.integration_test_hive.view_level2
        """
    )
    _has_view_chain = True
    print("2.15 seeded: view_level1 → view_level2 → view_level3 (3-level dependency chain).")
except Exception as _exc:  # noqa: BLE001
    print(f"2.15 skipped: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_view_chain", value="true" if _has_view_chain else "false"
)

# COMMAND ----------

print(
    "Hive seed data created successfully: "
    "managed_orders (DBFS-root), big_orders (view), triple (function). "
    f"DBFS-root migration will {'run' if config.migrate_hive_dbfs_root else 'be SKIPPED (migrate_hive_dbfs_root=false)'}."
)
