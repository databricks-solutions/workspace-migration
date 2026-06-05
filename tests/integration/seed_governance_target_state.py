# Databricks notebook source

# COMMAND ----------

# Pre-seed the target workspace with catalogs/schemas/tables/views/volumes
# that mirror the governance-test fixture (D3). Used by
# governance_integration_test_workflow.yml so the governance test runs
# standalone without depending on uc_integration_test having migrated
# the data plane first.
#
# The tool's normal workflow is "migrate UC data first, then attach
# governance to the migrated objects". For the standalone governance
# test we cheat: pre-create the target shapes so tags_worker /
# comments_worker / sharing_worker have something to attach to. No data
# migration — just CREATE TABLE / CREATE VOLUME with the right shape.
#
# Idempotent: every CREATE uses IF NOT EXISTS / OR REPLACE so the seed
# can re-run safely. Mirrors the fixtures in seed_uc_test_data.py for
# governance items 3.15 (catalog/schema/volume tags), 3.17 (column +
# volume comments), and 3.24 (customer-defined share, which ALTER SHARE
# ADDs managed_orders so that table must exist on target).

import sys

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass

# COMMAND ----------

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

config = MigrationConfig.from_workspace_file()
auth = AuthManager(config, dbutils)  # type: ignore[name-defined]  # noqa: F821
warehouse_id = find_warehouse(auth)


def _sql_on_target(stmt: str) -> None:
    result = execute_and_poll(auth, warehouse_id, stmt)
    if result.get("state") != "SUCCEEDED":
        raise RuntimeError(
            f"target seed failed for: {stmt!r} "
            f"(state={result.get('state')}, error={result.get('error', '')!r})"
        )


# Catalog + schemas. Mirror the source fixture so tags_worker can
# attach CATALOG / SCHEMA tags to the same FQN on target.
_sql_on_target("CREATE CATALOG IF NOT EXISTS integration_test_src")
_sql_on_target("CREATE SCHEMA IF NOT EXISTS integration_test_src.test_schema")

# managed_orders is the target table for 3.17 column comments (.amount)
# and 3.24 customer share (ALTER SHARE ADDs this table). Shape only —
# no data, since governance workers don't touch row data.
_sql_on_target("""
    CREATE TABLE IF NOT EXISTS integration_test_src.test_schema.managed_orders (
        order_id INT,
        customer_id INT,
        amount DOUBLE,
        order_date DATE
    ) USING DELTA
""")

# external_customers is the target table for T29 row filter / T30
# column mask reapply + DDL sanitizer E2E. row_filters_worker and
# column_masks_worker need this target table to exist before they can
# ALTER TABLE ... SET ROW FILTER / SET MASK on it.
_sql_on_target("""
    CREATE TABLE IF NOT EXISTS integration_test_src.test_schema.external_customers (
        customer_id INT,
        name STRING,
        region STRING
    ) USING DELTA
""")

# The row filter / column mask reference UDFs (region_filter / mask_customer).
# In a real migration migrate_uc (migrate_functions) puts these on the target
# before migrate_governance reapplies the filter/mask. The governance test
# pre-seeds the target directly, so create the functions here too — otherwise
# SET ROW FILTER / SET MASK fails with ROUTINE_NOT_FOUND. Definitions mirror
# the source seed (seed_uc_test_data.py).
_sql_on_target("""
    CREATE OR REPLACE FUNCTION integration_test_src.test_schema.region_filter(region STRING)
    RETURNS BOOLEAN
    RETURN region = 'US' OR is_account_group_member('admins')
""")
_sql_on_target("""
    CREATE OR REPLACE FUNCTION integration_test_src.test_schema.mask_customer(cid INT)
    RETURNS INT
    RETURN CASE WHEN is_account_group_member('admins') THEN cid ELSE -1 END
""")

# managed_sensitive carries source RLS+CM in the integration fixture;
# the governance test's RLS/CM reapply path doesn't strictly require
# it on target (Path A staging_copy keeps source intact and the tests
# under migrate_governance look at policy presence on whichever target
# tables governance attaches to). Still seed the shape for safety.
_sql_on_target("""
    CREATE TABLE IF NOT EXISTS integration_test_src.test_schema.managed_sensitive (
        id INT,
        region STRING,
        account_id STRING,
        amount DOUBLE
    ) USING DELTA
""")

# test_volume is the target volume for 3.15 (VOLUME tags) and 3.17
# (volume comment).
_sql_on_target("CREATE VOLUME IF NOT EXISTS integration_test_src.test_schema.test_volume")

print("seed_governance_target_state: target pre-seed complete.")
