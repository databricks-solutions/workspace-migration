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
# Seed for the live Online Tables → Synced Tables integration test.
#
# Legacy online tables cannot be created any more, so this seed:
#   1. Creates a PK'd source table on TARGET (the pre-check requires it there).
#   2. Injects a synthetic `online_table` row into discovery_inventory so the
#      migrate_online_tables orchestrator picks it up and creates a real Lakebase
#      synced table.
# There is NO real online table on source — the integration workflow has no
# `discovery` task; the row is injected directly here.

import json
from datetime import datetime, timezone

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager, discovery_row, discovery_schema

_CATALOG = "integration_test_src"
_SCHEMA = "ot_test"
_TABLE = f"{_CATALOG}.{_SCHEMA}.ot_source"
_OT_FQN = f"{_CATALOG}.{_SCHEMA}.ot_online"

# COMMAND ----------
# --- TARGET: PK'd Delta table so the pre-check finds the source table ---
_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_tgt_wh = find_warehouse(_auth)

for _sql in (
    f"CREATE CATALOG IF NOT EXISTS {_CATALOG}",
    f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}",
    (
        f"CREATE OR REPLACE TABLE {_TABLE} "
        "(id INT NOT NULL, text STRING, CONSTRAINT ot_pk PRIMARY KEY(id))"
    ),
    f"INSERT INTO {_TABLE} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
):
    _res = execute_and_poll(_auth, _tgt_wh, _sql)
    if _res.get("state") != "SUCCEEDED":
        raise RuntimeError(f"[seed-ot] target setup SQL failed: {_sql!r} -> {_res}")
print(f"[seed-ot] target table {_TABLE} created (PK on id)")

# COMMAND ----------
# --- TRACKING: ensure tracking tables exist, then inject a synthetic online_table row ---
TrackingManager(spark, _config).init_tracking_tables()  # noqa: F821

_now = datetime.now(tz=timezone.utc)
_row = discovery_row(
    source_type="stateful",
    object_type="online_table",
    object_name=_OT_FQN,
    catalog_name=None,
    schema_name=None,
    discovered_at=_now,
    metadata={
        "capability": "online_store",
        "online_table_fqn": _OT_FQN,
        "source_table_fqn": _TABLE,
        "definition": {
            "name": _OT_FQN,
            "spec": {
                "source_table_full_name": _TABLE,
                "primary_key_columns": ["id"],
                "run_triggered": {},
            },
        },
    },
)

spark.createDataFrame([_row], schema=discovery_schema()).write.mode("append").saveAsTable(  # noqa: F821
    f"{_config.tracking_catalog}.{_config.tracking_schema}.discovery_inventory"
)
print(f"[seed-ot] synthetic online_table row injected for {_OT_FQN}")

# COMMAND ----------
dbutils.jobs.taskValues.set(key="online_table_fqn", value=_OT_FQN)  # noqa: F821
dbutils.notebook.exit(json.dumps({"seeded": True, "online_table_fqn": _OT_FQN}))  # noqa: F821
