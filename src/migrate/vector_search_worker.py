# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

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
# Vector Search migration worker. Recreates Delta Sync indexes on the target
# (re-syncing from the same-named source table); skips Direct Access indexes.
# See docs/superpowers/specs/2026-06-03-vector-search-migration-design.md.

import json  # noqa: F401
import time  # noqa: F401

from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EndpointType,  # noqa: F401
    VectorIndexType,  # noqa: F401
)

from common.auth import AuthManager  # noqa: F401
from common.config import MigrationConfig  # noqa: F401
from common.tracking import TrackingManager  # noqa: F401

# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def _is_delta_sync(definition: dict) -> bool:
    """True iff this index spec is a Delta Sync index (the only migratable kind)."""
    return str(definition.get("index_type", "")).upper().endswith("DELTA_SYNC")


def _build_delta_sync_spec(definition: dict) -> DeltaSyncVectorIndexSpecRequest:
    """Build a create-request spec from the discovered get_index() dict.

    The discovered ``delta_sync_index_spec`` is the response shape, which
    carries a response-only ``pipeline_id`` not accepted on create — drop it.
    ``from_dict`` parses nested embedding columns and the ``pipeline_type`` enum.
    """
    dss = dict(definition.get("delta_sync_index_spec") or {})
    dss.pop("pipeline_id", None)
    return DeltaSyncVectorIndexSpecRequest.from_dict(dss)
