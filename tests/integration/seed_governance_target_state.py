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

# test_volume is the target volume for 3.15 (VOLUME tags) and 3.17
# (volume comment).
_sql_on_target("CREATE VOLUME IF NOT EXISTS integration_test_src.test_schema.test_volume")

print("seed_governance_target_state: target pre-seed complete.")
