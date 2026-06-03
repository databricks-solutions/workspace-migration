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
# Seed for the live Vector Search integration test.
#
# SOURCE: a CDF-enabled Delta table + a VS endpoint + a Delta Sync index
# (positive case) + a Direct Access index (negative case).
# TARGET: the SAME-named Delta table only (stands in for migrate_uc, so the
# migrate_vector_search pre-check finds the Delta Sync index's source table on
# target). The target has NO VS endpoint/index — the worker creates those cold.
#
# Each object is best-effort; has_* task values gate the downstream assertion.

import contextlib
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    DirectAccessVectorIndexSpec,
    EmbeddingSourceColumn,
    EmbeddingVectorColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_src"
_SCHEMA = "vs_test"
_TABLE = f"{_CATALOG}.{_SCHEMA}.vs_source"
_ENDPOINT = "cp_migration_vs_it"
_DELTA_IDX = f"{_CATALOG}.{_SCHEMA}.vs_delta_idx"
_DIRECT_IDX = f"{_CATALOG}.{_SCHEMA}.vs_direct_idx"
_EMBED_ENDPOINT = "databricks-gte-large-en"
_EMBED_DIM = 1024

# COMMAND ----------
# --- SOURCE: CDF-enabled Delta table with a few rows ---
spark.sql(f"CREATE CATALOG IF NOT EXISTS {_CATALOG}")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")  # noqa: F821
spark.sql(  # noqa: F821
    f"CREATE OR REPLACE TABLE {_TABLE} (id INT, text STRING) "
    "TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)
spark.sql(  # noqa: F821
    f"INSERT INTO {_TABLE} VALUES "
    "(1, 'red running shoes'), (2, 'blue cotton shirt'), (3, 'green wool socks')"
)
print(f"[seed-vs] source table {_TABLE} created (CDF enabled)")

# COMMAND ----------
# --- TARGET: same-named table so migrate_vector_search pre-check passes ---
_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_tgt_wh = find_warehouse(_auth)
for _sql in (
    f"CREATE CATALOG IF NOT EXISTS {_CATALOG}",
    f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}",
    f"CREATE OR REPLACE TABLE {_TABLE} (id INT, text STRING) "
    "TBLPROPERTIES (delta.enableChangeDataFeed = true)",
    f"INSERT INTO {_TABLE} VALUES (1, 'red running shoes'), (2, 'blue cotton shirt')",
):
    _res = execute_and_poll(_auth, _tgt_wh, _sql)
    if _res["state"] != "SUCCEEDED":
        raise RuntimeError(f"[seed-vs] target setup SQL failed: {_sql} -> {_res}")
print(f"[seed-vs] target table {_TABLE} created (CDF enabled)")

# COMMAND ----------
# --- SOURCE: VS endpoint (wait for ONLINE) ---
_w = WorkspaceClient()


def _endpoint_online(name: str) -> bool:
    with contextlib.suppress(Exception):
        ep = _w.vector_search_endpoints.get_endpoint(name)
        return "ONLINE" in str(getattr(ep.endpoint_status, "state", "")).upper()
    return False


_has_delta_index = False
_has_direct_index = False
_endpoint_ready = False
try:
    if not _endpoint_online(_ENDPOINT):
        with contextlib.suppress(Exception):
            _w.vector_search_endpoints.create_endpoint(name=_ENDPOINT, endpoint_type=EndpointType.STANDARD)
        for _ in range(120):
            if _endpoint_online(_ENDPOINT):
                break
            time.sleep(15)
    if not _endpoint_online(_ENDPOINT):
        raise RuntimeError(f"source VS endpoint {_ENDPOINT} did not reach ONLINE")
    print(f"[seed-vs] source endpoint {_ENDPOINT} ONLINE")
    _endpoint_ready = True
except Exception as _exc:  # noqa: BLE001
    print(f"[seed-vs] VS endpoint not ONLINE — skipping both index seeds: {_exc}")

if _endpoint_ready:
    try:
        with contextlib.suppress(Exception):
            _w.vector_search_indexes.delete_index(_DELTA_IDX)
        _w.vector_search_indexes.create_index(
            name=_DELTA_IDX,
            endpoint_name=_ENDPOINT,
            primary_key="id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=_TABLE,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(name="text", embedding_model_endpoint_name=_EMBED_ENDPOINT)
                ],
            ),
        )
        _has_delta_index = True
        print(f"[seed-vs] source Delta Sync index {_DELTA_IDX} created")
    except Exception as _exc:  # noqa: BLE001
        print(f"[seed-vs] Delta Sync index seed failed: {_exc}")

    try:
        with contextlib.suppress(Exception):
            _w.vector_search_indexes.delete_index(_DIRECT_IDX)
        _w.vector_search_indexes.create_index(
            name=_DIRECT_IDX,
            endpoint_name=_ENDPOINT,
            primary_key="id",
            index_type=VectorIndexType.DIRECT_ACCESS,
            direct_access_index_spec=DirectAccessVectorIndexSpec(
                schema_json='{"id": "integer", "text": "string", "text_vector": "array<float>"}',
                embedding_vector_columns=[EmbeddingVectorColumn(name="text_vector", embedding_dimension=_EMBED_DIM)],
            ),
        )
        _has_direct_index = True
        print(f"[seed-vs] source Direct Access index {_DIRECT_IDX} created")
    except Exception as _exc:  # noqa: BLE001
        print(f"[seed-vs] Direct Access index seed failed: {_exc}")

# COMMAND ----------
dbutils.jobs.taskValues.set(key="has_delta_index", value="true" if _has_delta_index else "false")  # noqa: F821
dbutils.jobs.taskValues.set(key="has_direct_index", value="true" if _has_direct_index else "false")  # noqa: F821
dbutils.jobs.taskValues.set(key="delta_index_fqn", value=_DELTA_IDX)  # noqa: F821
dbutils.jobs.taskValues.set(key="direct_index_fqn", value=_DIRECT_IDX)  # noqa: F821
dbutils.jobs.taskValues.set(key="vs_endpoint_name", value=_ENDPOINT)  # noqa: F821
print(f"[seed-vs] flags: delta={_has_delta_index} direct={_has_direct_index}")
