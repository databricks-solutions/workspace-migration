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

import contextlib
import json
import time

from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EndpointType,
    VectorIndexType,
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


def _endpoint_state_ready(state: object) -> bool:
    """True when an endpoint's status state reads as ONLINE."""
    return "ONLINE" in str(state).upper()


def _ensure_endpoint(
    target_client,
    endpoint_name: str,
    endpoint_type: str,
    *,
    max_attempts: int = 30,
    sleep_seconds: float = 10.0,
    sleep_fn=time.sleep,
) -> bool:
    """Ensure the target VS endpoint exists and is ONLINE.

    If absent, create it (mirroring the source endpoint_type). Poll up to
    ``max_attempts`` for it to reach ONLINE. Returns True if ready, False if it
    is still provisioning when attempts are exhausted (caller defers the index).
    """
    try:
        ep = target_client.vector_search_endpoints.get_endpoint(endpoint_name)
        if _endpoint_state_ready(getattr(ep.endpoint_status, "state", None)):
            return True
    except Exception:  # noqa: BLE001 — absent (or transient); fall through to create/poll
        et = EndpointType(endpoint_type) if endpoint_type else EndpointType.STANDARD
        # another agent may have created it between our get and create — tolerate the race
        with contextlib.suppress(AlreadyExists):
            target_client.vector_search_endpoints.create_endpoint(name=endpoint_name, endpoint_type=et)

    for _ in range(max_attempts):
        try:
            ep = target_client.vector_search_endpoints.get_endpoint(endpoint_name)
            if _endpoint_state_ready(getattr(ep.endpoint_status, "state", None)):
                return True
        except Exception:  # noqa: BLE001 — keep polling
            pass
        sleep_fn(sleep_seconds)
    return False


def migrate_index(
    target_client,
    row: dict,
    *,
    max_attempts: int = 30,
    sleep_seconds: float = 10.0,
    sleep_fn=time.sleep,
) -> dict:
    """Migrate one vector_search_index discovery row. Returns a status dict."""
    start = time.time()
    obj_name = row["object_name"]

    def _result(status: str, error: str | None = None) -> dict:
        return {
            "object_name": obj_name,
            "object_type": "vector_search_index",
            "status": status,
            "error_message": error,
            "duration_seconds": time.time() - start,
        }

    meta = json.loads(row.get("metadata_json") or "{}")
    definition = meta.get("definition") or {}

    if not definition:
        return _result("failed", "discovery row has no 'definition' in metadata_json — cannot migrate index.")

    if not _is_delta_sync(definition):
        return _result(
            "skipped_direct_access_unsupported",
            "Direct Access VS index — vectors are external app-written state the "
            "tool cannot recreate. See docs/user_guide.md (Vector Search limitations).",
        )

    endpoint_name = definition.get("endpoint_name")
    endpoint_type = definition.get("endpoint_type") or "STANDARD"
    ready = _ensure_endpoint(
        target_client, endpoint_name, endpoint_type,
        max_attempts=max_attempts, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn,
    )
    if not ready:
        return _result(
            "skipped_endpoint_not_ready",
            f"Target endpoint '{endpoint_name}' not ONLINE within wait budget; "
            "a re-run will retry this index.",
        )

    try:
        target_client.vector_search_indexes.create_index(
            name=obj_name,
            endpoint_name=endpoint_name,
            primary_key=definition.get("primary_key"),
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=_build_delta_sync_spec(definition),
        )
    except AlreadyExists as exc:
        return _result("skipped_target_exists", f"Index already exists on target: {exc}")
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"create_index failed: {exc}")

    return _result("created_resync_pending", None)
