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

# abac_filter backs the 3.16 ABAC policy (CREATE POLICY ... ROW FILTER
# abac_filter). The policies_worker POSTs the captured policy definition to
# target, which UC validates against the referenced function — so abac_filter
# must exist on target first, exactly like region_filter/mask_customer for
# the classic RLS/CM reapply. 0-arg signature must match the source
# definition (seed_uc_test_data.py) so the policy's function_name resolves.
_sql_on_target("""
    CREATE OR REPLACE FUNCTION integration_test_src.test_schema.abac_filter()
    RETURNS BOOLEAN
    RETURN is_account_group_member('admins')
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

# --- 3.21 / 3.22 connection + foreign catalog target pre-seed ---
# UC's GET never returns the secret options (user + password for SQLSERVER),
# so the migration tool cannot recreate a secret-bearing connection from the
# discovered options alone — in a real migration the operator recreates the
# connection on target WITH credentials, then migrate_foreign_catalogs binds
# the foreign catalog to it. Mirror that runbook step here: pre-create the
# connection on target with full credentials so (a) the connection worker's
# target-existence pre-check returns validated (idempotent) and (b) the
# foreign_catalog worker's real CREATE runs against an existing connection.
#
# MUST run on a SERVERLESS warehouse — the connection egresses to the SQL
# server over the target NCC (ncc-azpng) private endpoint, which is
# serverless-only. Password comes from the ``migration`` secret scope on the
# source workspace where this notebook runs (never committed). Surface any
# failure (do not swallow) so a missing prereq is visible.
_CONN_NAME = "integration_test_sqlserver"
_SQL_HOST = "sqlsrv-wsm-test-ne.database.windows.net"
try:
    import time as _t2  # noqa: E402

    from databricks.sdk.service.sql import StatementState as _SS2  # noqa: E402, N814

    _tc = auth.target_client
    _pwd = dbutils.secrets.get(scope="migration", key="sqltest-password")  # type: ignore[name-defined]  # noqa: F821
    _twhs = [w for w in _tc.warehouses.list() if getattr(w, "enable_serverless_compute", False)]
    if not _twhs:
        raise RuntimeError("no serverless warehouse on target workspace for connection pre-seed")
    _twh = _twhs[0].id

    def _conn_on_target(stmt: str) -> None:
        _r = _tc.statement_execution.execute_statement(warehouse_id=_twh, statement=stmt, wait_timeout="50s")
        _sid = _r.statement_id
        for _ in range(120):
            _s = _tc.statement_execution.get_statement(_sid)
            if _s.status.state == _SS2.SUCCEEDED:
                return
            if _s.status.state in (_SS2.FAILED, _SS2.CANCELED, _SS2.CLOSED):
                raise RuntimeError(_s.status.error.message if _s.status.error else str(_s.status.state))
            _t2.sleep(2)
        raise RuntimeError("target connection pre-seed statement timed out")

    _conn_on_target(
        f"CREATE CONNECTION IF NOT EXISTS {_CONN_NAME} TYPE sqlserver "
        f"OPTIONS (host '{_SQL_HOST}', port '1433', user 'wsmtestadmin', password '{_pwd}')"
    )
    print(f"seed_governance_target_state: pre-seeded target connection '{_CONN_NAME}'.")
except Exception as _conn_exc:  # noqa: BLE001
    print(f"!! seed_governance_target_state: target connection pre-seed FAILED: {_conn_exc}")

print("seed_governance_target_state: target pre-seed complete.")
