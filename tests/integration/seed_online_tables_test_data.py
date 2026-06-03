# Databricks notebook source

# COMMAND ----------

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
# Seed for the live Online Tables integration test.
# SOURCE: a PK'd Delta table + a Triggered online table (the positive case).
# TARGET: the same-named PK'd Delta table only (stands in for migrate_uc, so the
# migrate_online_tables pre-check finds the source table). No online table on target.

import contextlib
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    OnlineTable,
    OnlineTableSpec,
    OnlineTableSpecTriggeredSchedulingPolicy,
)

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_src"
_SCHEMA = "ot_test"
_TABLE = f"{_CATALOG}.{_SCHEMA}.ot_source"
_OT_FQN = f"{_CATALOG}.{_SCHEMA}.ot_online"

# COMMAND ----------
# --- SOURCE: PK'd Delta table ---
spark.sql(f"CREATE CATALOG IF NOT EXISTS {_CATALOG}")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")  # noqa: F821
spark.sql(  # noqa: F821
    f"CREATE OR REPLACE TABLE {_TABLE} (id INT NOT NULL, text STRING, "
    "CONSTRAINT ot_pk PRIMARY KEY(id))"
)
spark.sql(f"INSERT INTO {_TABLE} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")  # noqa: F821
print(f"[seed-ot] source table {_TABLE} created (PK on id)")

# COMMAND ----------
# --- TARGET: same-named PK'd table so the pre-check passes ---
_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_tgt_wh = find_warehouse(_auth)
for _sql in (
    f"CREATE CATALOG IF NOT EXISTS {_CATALOG}",
    f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}",
    f"CREATE OR REPLACE TABLE {_TABLE} (id INT NOT NULL, text STRING, CONSTRAINT ot_pk PRIMARY KEY(id))",
    f"INSERT INTO {_TABLE} VALUES (1, 'alpha'), (2, 'beta')",
):
    _res = execute_and_poll(_auth, _tgt_wh, _sql)
    if _res.get("state") != "SUCCEEDED":
        raise RuntimeError(f"[seed-ot] target setup SQL failed: {_sql} -> {_res}")
print(f"[seed-ot] target table {_TABLE} created (PK on id)")

# COMMAND ----------
# --- SOURCE: Triggered online table (positive case) ---
_w = WorkspaceClient()
_has_online_table = False
try:
    with contextlib.suppress(Exception):
        _w.online_tables.delete(_OT_FQN)
    _w.online_tables.create(
        OnlineTable(
            name=_OT_FQN,
            spec=OnlineTableSpec(
                source_table_full_name=_TABLE,
                primary_key_columns=["id"],
                run_triggered=OnlineTableSpecTriggeredSchedulingPolicy(),
            ),
        )
    )
    _has_online_table = True
    print(f"[seed-ot] source online table {_OT_FQN} created")
except Exception as _exc:  # noqa: BLE001
    print(f"[seed-ot] online table seed failed (preview may be unavailable): {_exc}")

# COMMAND ----------
dbutils.jobs.taskValues.set(key="has_online_table", value="true" if _has_online_table else "false")  # noqa: F821
dbutils.jobs.taskValues.set(key="online_table_fqn", value=_OT_FQN)  # noqa: F821
print(f"[seed-ot] flags: online_table={_has_online_table}")
dbutils.notebook.exit(json.dumps({"has_online_table": _has_online_table}))  # noqa: F821
