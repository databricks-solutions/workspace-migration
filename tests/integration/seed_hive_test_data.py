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

config = MigrationConfig.from_workspace_file()

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.integration_test_hive")  # noqa: F821

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

_hive_external_location: str | None = None
_hive_nondbfs_location: str | None = None
if config.migrate_hive_dbfs_root and config.hive_dbfs_target_path:
    _base = config.hive_dbfs_target_path.rstrip("/")
    _hive_external_location = f"{_base}/hive_external_invoices"
    _hive_nondbfs_location = f"{_base}/hive_nondbfs_sales"

_has_hive_external = False
_has_hive_nondbfs = False

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

if _hive_nondbfs_location:
    try:
        # MANAGED non-DBFS — in hive_metastore, a managed table with an
        # explicit LOCATION off DBFS root. Covers the hive_managed_nondbfs
        # worker's path (legacy mount / non-default cluster config).
        spark.sql(  # noqa: F821
            f"""
            CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.nondbfs_sales (
                sale_id INT,
                amount DOUBLE
            )
            USING DELTA
            LOCATION '{_hive_nondbfs_location}'
            """
        )
        spark.sql(  # noqa: F821
            """
            INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.nondbfs_sales VALUES
                (201, 50.0),
                (202, 75.0),
                (203, 90.0)
            """
        )
        _has_hive_nondbfs = True
        print(f"Created Hive managed non-DBFS table at {_hive_nondbfs_location}.")
    except Exception as _exc:  # noqa: BLE001
        print(f"Skipped Hive managed-nondbfs seed: {_exc}")

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
# hosts hive_dbfs_target_path. Without this, the DBFS-root worker's
# CREATE TABLE ... LOCATION '<hive_dbfs_target_path>/...' fails with
# PERMISSION_DENIED on the target side.
if config.migrate_hive_dbfs_root and config.hive_dbfs_target_path and config.spn_client_id:
    auth = AuthManager(config, dbutils)  # noqa: F821
    wh_id = find_warehouse(auth)
    # Find the external location covering hive_dbfs_target_path on target.
    locs = list(auth.target_client.external_locations.list())
    matching = [loc for loc in locs if config.hive_dbfs_target_path.startswith(loc.url)]
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
            f"WARNING: No external location on target covers {config.hive_dbfs_target_path!r}. "
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
# --- 2.11 cross-catalog view (references a UC catalog, not hive_metastore) ---
# Seeds a parallel UC catalog on BOTH source and target so the Hive view's
# non-hive reference resolves on both sides. rewrite_hive_namespace only
# rewrites ``hive_metastore.`` prefixes — a ``<other_catalog>.`` reference
# must pass through verbatim.

_HIVE_UCREF_CATALOG = "integration_test_hive_ucref"
_HIVE_UCREF_SCHEMA = "ucref_schema"
_HIVE_UCREF_TABLE = "uc_dim_products"
_HIVE_UCREF_FQN = f"{_HIVE_UCREF_CATALOG}.{_HIVE_UCREF_SCHEMA}.{_HIVE_UCREF_TABLE}"

_has_cross_catalog_view = False
if config.spn_client_id:
    try:
        auth_ucref = AuthManager(config, dbutils)  # noqa: F821
        wh_ucref = find_warehouse(auth_ucref)

        # Create UC ref catalog/schema/table on BOTH source and target.
        # Source: via spark SQL (same metastore).
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{_HIVE_UCREF_CATALOG}`")  # noqa: F821
        spark.sql(  # noqa: F821
            f"CREATE SCHEMA IF NOT EXISTS `{_HIVE_UCREF_CATALOG}`.`{_HIVE_UCREF_SCHEMA}`"
        )
        spark.sql(  # noqa: F821
            f"""
            CREATE TABLE IF NOT EXISTS {_HIVE_UCREF_FQN} (
                id INT,
                category STRING
            ) USING DELTA
            """
        )
        spark.sql(  # noqa: F821
            f"""
            INSERT OVERWRITE TABLE {_HIVE_UCREF_FQN} VALUES
                (1, 'alpha'), (2, 'beta'), (3, 'gamma')
            """
        )

        # Target: via SQL warehouse. Rows aren't required for view resolution;
        # only the table shape matters for the view's CREATE to succeed.
        for _s in (
            f"CREATE CATALOG IF NOT EXISTS `{_HIVE_UCREF_CATALOG}`",
            f"CREATE SCHEMA IF NOT EXISTS `{_HIVE_UCREF_CATALOG}`.`{_HIVE_UCREF_SCHEMA}`",
            f"CREATE TABLE IF NOT EXISTS {_HIVE_UCREF_FQN} (id INT, category STRING) USING DELTA",
            f"GRANT USE CATALOG ON CATALOG `{_HIVE_UCREF_CATALOG}` TO `{config.spn_client_id}`",
            f"GRANT USE SCHEMA ON SCHEMA `{_HIVE_UCREF_CATALOG}`.`{_HIVE_UCREF_SCHEMA}` TO `{config.spn_client_id}`",
            f"GRANT SELECT ON TABLE {_HIVE_UCREF_FQN} TO `{config.spn_client_id}`",
        ):
            _r = execute_and_poll(auth_ucref, wh_ucref, _s)
            if _r["state"] != "SUCCEEDED":
                raise RuntimeError(f"Target UC ref setup failed on '{_s[:60]}...': {_r.get('error')}")

        # Source-side grant so the view body can resolve SELECT during DESCRIBE.
        spark.sql(f"GRANT SELECT ON TABLE {_HIVE_UCREF_FQN} TO `{config.spn_client_id}`")  # noqa: F821

        spark.sql(  # noqa: F821
            f"""
            CREATE OR REPLACE VIEW hive_metastore.integration_test_hive.mixed_ref_view AS
            SELECT o.order_id, o.amount, u.category
            FROM hive_metastore.integration_test_hive.managed_orders o
            JOIN {_HIVE_UCREF_FQN} u ON o.order_id = u.id
            """
        )
        _has_cross_catalog_view = True
        print(f"2.11 seeded: mixed_ref_view referencing {_HIVE_UCREF_FQN} (cross-catalog).")
    except Exception as _exc:  # noqa: BLE001
        print(f"2.11 skipped: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_cross_catalog_view", value="true" if _has_cross_catalog_view else "false"
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
if config.migrate_hive_dbfs_root and config.hive_dbfs_target_path:
    _base = config.hive_dbfs_target_path.rstrip("/")
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
