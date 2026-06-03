# Vector Search Migration (`migrate_vector_search`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone `migrate_vector_search` job that migrates Delta Sync Vector Search indexes from source to target (recreate → re-sync), skips Direct Access indexes, and gates on the source Delta table existing on target.

**Architecture:** A new Databricks-job workflow `migrate_vector_search` with task chain `pre_check_vector_search → orchestrator → migrate_vector_search → summary_vector_search`. The shared `orchestrator.py` gains `vector_search_index` in its `LIST_TYPES` so it publishes a `vector_search_index_list` task value (exactly like `online_table`). A new notebook worker `vector_search_worker.py` consumes that list, ensures the target VS endpoint exists (create-if-missing), recreates each Delta Sync index pointing at the same-named target source table, and records a terminal `created_resync_pending` status. A new `pre_check_vector_search.py` fails the job up-front if any Delta Sync index's source table is absent on target.

**Tech Stack:** Python 3.11, `databricks-sdk` 0.102.0 (`WorkspaceClient.vector_search_endpoints` / `vector_search_indexes` / `tables`), pytest, ruff. Tests mock `auth.target_client`. Spec: `docs/superpowers/specs/2026-06-03-vector-search-migration-design.md`.

**Verified SDK surface (0.102.0):**
- `target_client.vector_search_endpoints.get_endpoint(endpoint_name)` → `EndpointInfo(.endpoint_status.state, .endpoint_type)`; raises if absent.
- `target_client.vector_search_endpoints.create_endpoint(name, endpoint_type: EndpointType)` → `Wait[EndpointInfo]`. `EndpointType.STANDARD` is the only value.
- `target_client.vector_search_indexes.create_index(name, endpoint_name, primary_key, index_type: VectorIndexType, *, delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest, direct_access_index_spec=...)` → `VectorIndex`.
- `VectorIndexType.DELTA_SYNC` / `VectorIndexType.DIRECT_ACCESS`.
- The discovered `metadata_json.definition` is `get_index(...).as_dict()`: keys `index_type` (str "DELTA_SYNC"/"DIRECT_ACCESS"), `endpoint_name`, `primary_key`, `delta_sync_index_spec` (dict: `source_table`, `pipeline_type`, `embedding_source_columns`, `embedding_vector_columns`, `embedding_writeback_table`, `pipeline_id`), `direct_access_index_spec`.
- `DeltaSyncVectorIndexSpecRequest` fields: `columns_to_sync, embedding_source_columns, embedding_vector_columns, embedding_writeback_table, pipeline_type, source_table` (NO `pipeline_id` — that's response-only). `DeltaSyncVectorIndexSpecRequest.from_dict(...)` parses nested columns + the `pipeline_type` enum.

**Design refinement vs spec:** The Q3 source-table gate is implemented by `pre_check_vector_search` **raising** (failing its task), which halts the job before `orchestrator`/worker run — rather than writing a row that the orchestrator re-reads. This is simpler and decoupled (the shared `orchestrator.check_collision_gate` only knows about UC `target_collision` rows, not VS). The pre-check still records a `pre_check_results` row for dashboard visibility before raising.

---

## File Structure

- **Modify** `src/common/tracking.py` — add two terminal statuses.
- **Create** `src/pre_check/pre_check_vector_search.py` — source-table gate (notebook).
- **Create** `src/migrate/vector_search_worker.py` — index migration worker (notebook).
- **Modify** `src/migrate/orchestrator.py` — add `vector_search_index` to `LIST_TYPES`.
- **Create** `resources/production/migrate_vector_search_workflow.yml` — the job.
- **Create** `resources/integration_tests/vector_search_integration_test_workflow.yml` — test job.
- **Create** `tests/unit/test_pre_check_vector_search.py`, `tests/unit/test_vector_search_worker.py`.
- **Modify** `docs/user_guide.md`, `docs/stateful_services_phase.md`.

Statuses, helper names, and task-value keys used across tasks:
- terminal: `created_resync_pending`, `skipped_direct_access_unsupported`
- non-terminal (re-pickable): `skipped_endpoint_not_ready`
- worker helpers: `_is_delta_sync(definition)`, `_build_delta_sync_spec(definition)`, `_ensure_endpoint(target_client, name, endpoint_type, max_attempts, sleep_seconds, sleep_fn)`, `migrate_index(target_client, row)`
- task-value key: `vector_search_index_list` (published by orchestrator, read by worker)

---

## Task 1: Add the two terminal statuses to tracking

**Files:**
- Modify: `src/common/tracking.py` (the `_TERMINAL_STATUSES` tuple)
- Test: `tests/unit/test_tracking.py` (append a test)

- [ ] **Step 1: Write the failing test** (append to `tests/unit/test_tracking.py`)

```python
def test_vector_search_terminal_statuses_present():
    from common.tracking import _TERMINAL_STATUSES

    # created index (re-embedding still running) and direct-access skip are
    # terminal so re-runs don't recreate; endpoint-not-ready is NOT terminal.
    assert "created_resync_pending" in _TERMINAL_STATUSES
    assert "skipped_direct_access_unsupported" in _TERMINAL_STATUSES
    assert "skipped_endpoint_not_ready" not in _TERMINAL_STATUSES
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_tracking.py::test_vector_search_terminal_statuses_present -v`
Expected: FAIL (`created_resync_pending` not in tuple).

- [ ] **Step 3: Add the statuses** — in `src/common/tracking.py`, extend the `_TERMINAL_STATUSES` tuple (currently `("validated", "skipped_by_pipeline_migration", "skipped_target_exists", "skipped_by_stateful_service_migration", "failed_batch_oversize")`) by appending the two new terminal statuses:

```python
_TERMINAL_STATUSES: tuple[str, ...] = (
    "validated",
    "skipped_by_pipeline_migration",
    "skipped_target_exists",
    "skipped_by_stateful_service_migration",
    "failed_batch_oversize",
    # Vector Search (migrate_vector_search): index created on target, async
    # re-embedding still in progress — terminal so re-runs don't recreate.
    "created_resync_pending",
    # Direct Access VS index — vectors are external app state, can't recreate.
    "skipped_direct_access_unsupported",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_tracking.py -v`
Expected: PASS (all tracking tests).

- [ ] **Step 5: Commit**

```bash
git add src/common/tracking.py tests/unit/test_tracking.py
git commit -m "feat(vs): add created_resync_pending + skipped_direct_access_unsupported terminal statuses"
```

---

## Task 2: Worker pure helpers — `_is_delta_sync` + `_build_delta_sync_spec`

**Files:**
- Create: `src/migrate/vector_search_worker.py` (scaffold + the two pure helpers only)
- Test: `tests/unit/test_vector_search_worker.py`

- [ ] **Step 1: Write the failing test**

```python
"""Unit tests for the Vector Search migration worker."""

from __future__ import annotations

from unittest.mock import MagicMock

from migrate.vector_search_worker import _build_delta_sync_spec, _is_delta_sync


def _delta_sync_definition():
    return {
        "index_type": "DELTA_SYNC",
        "endpoint_name": "ep1",
        "primary_key": "id",
        "delta_sync_index_spec": {
            "source_table": "cat.sch.src",
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {"name": "text", "embedding_model_endpoint_name": "databricks-gte-large-en"}
            ],
            "pipeline_id": "pl-123",  # response-only, must be dropped on create
        },
    }


class TestClassify:
    def test_is_delta_sync_true(self):
        assert _is_delta_sync(_delta_sync_definition()) is True

    def test_is_delta_sync_false_for_direct_access(self):
        assert _is_delta_sync({"index_type": "DIRECT_ACCESS"}) is False

    def test_is_delta_sync_false_when_missing(self):
        assert _is_delta_sync({}) is False


class TestBuildSpec:
    def test_builds_request_from_definition_and_drops_pipeline_id(self):
        spec = _build_delta_sync_spec(_delta_sync_definition())
        assert spec.source_table == "cat.sch.src"
        # pipeline_type parsed into the SDK enum
        assert str(spec.pipeline_type).endswith("TRIGGERED")
        assert spec.embedding_source_columns[0].name == "text"
        assert (
            spec.embedding_source_columns[0].embedding_model_endpoint_name
            == "databricks-gte-large-en"
        )
        # round-tripping the request must not carry the response-only pipeline_id
        assert "pipeline_id" not in spec.as_dict()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'migrate.vector_search_worker'`).

- [ ] **Step 3: Write the scaffold + helpers** — create `src/migrate/vector_search_worker.py`:

```python
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

import json
import time

from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EndpointType,
    VectorIndexType,
)

from common.config import MigrationConfig
from common.auth import AuthManager
from common.tracking import TrackingManager


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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py -v`
Expected: PASS (`TestClassify` + `TestBuildSpec`, 5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/migrate/vector_search_worker.py tests/unit/test_vector_search_worker.py
git commit -m "feat(vs): vector_search_worker scaffold + classify/build-spec helpers"
```

---

## Task 3: Endpoint ensure (create-if-missing, bounded wait)

**Files:**
- Modify: `src/migrate/vector_search_worker.py` (add `_ensure_endpoint`)
- Test: `tests/unit/test_vector_search_worker.py` (append `TestEnsureEndpoint`)

- [ ] **Step 1: Write the failing test**

```python
class TestEnsureEndpoint:
    def _ep(self, state):
        ep = MagicMock()
        ep.endpoint_status.state = state
        return ep

    def test_existing_online_endpoint_is_ready_no_create(self):
        from migrate.vector_search_worker import _ensure_endpoint

        client = MagicMock()
        client.vector_search_endpoints.get_endpoint.return_value = self._ep("ONLINE")
        ready = _ensure_endpoint(client, "ep1", "STANDARD", max_attempts=1, sleep_seconds=0, sleep_fn=lambda s: None)
        assert ready is True
        client.vector_search_endpoints.create_endpoint.assert_not_called()

    def test_missing_endpoint_is_created_then_becomes_ready(self):
        from migrate.vector_search_worker import _ensure_endpoint

        client = MagicMock()
        # first get raises (absent); after create, polling returns PROVISIONING then ONLINE
        client.vector_search_endpoints.get_endpoint.side_effect = [
            Exception("not found"),
            self._ep("PROVISIONING"),
            self._ep("ONLINE"),
        ]
        ready = _ensure_endpoint(client, "ep1", "STANDARD", max_attempts=5, sleep_seconds=0, sleep_fn=lambda s: None)
        assert ready is True
        client.vector_search_endpoints.create_endpoint.assert_called_once()

    def test_endpoint_never_ready_returns_false(self):
        from migrate.vector_search_worker import _ensure_endpoint

        client = MagicMock()
        client.vector_search_endpoints.get_endpoint.side_effect = Exception("not found")
        # after create, every poll stays PROVISIONING
        client.vector_search_endpoints.get_endpoint.side_effect = [Exception("nf")] + [self._ep("PROVISIONING")] * 5
        ready = _ensure_endpoint(client, "ep1", "STANDARD", max_attempts=3, sleep_seconds=0, sleep_fn=lambda s: None)
        assert ready is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py::TestEnsureEndpoint -v`
Expected: FAIL (no attribute `_ensure_endpoint`).

- [ ] **Step 3: Implement** — add to `src/migrate/vector_search_worker.py`:

```python
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
```

> Note: the existing-but-not-online case falls through to the `except` branch only on a raised `get_endpoint`. If `get_endpoint` succeeds but the endpoint is not ONLINE, the code proceeds to the poll loop without calling `create_endpoint` (correct — it already exists). The first test (`ONLINE`) returns early; the create path is driven by `get_endpoint` raising.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py -v`
Expected: PASS (8 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/migrate/vector_search_worker.py tests/unit/test_vector_search_worker.py
git commit -m "feat(vs): _ensure_endpoint create-if-missing with bounded ONLINE wait"
```

---

## Task 4: `migrate_index` (per-index status logic)

**Files:**
- Modify: `src/migrate/vector_search_worker.py` (add `migrate_index`)
- Test: `tests/unit/test_vector_search_worker.py` (append `TestMigrateIndex`)

- [ ] **Step 1: Write the failing test**

```python
class TestMigrateIndex:
    def _row(self, definition):
        return {"object_name": "cat.sch.idx", "object_type": "vector_search_index",
                "metadata_json": json.dumps({"definition": definition})}

    def _delta_def(self):
        return {
            "index_type": "DELTA_SYNC", "endpoint_name": "ep1", "primary_key": "id",
            "delta_sync_index_spec": {"source_table": "cat.sch.src", "pipeline_type": "TRIGGERED",
                                      "embedding_source_columns": [{"name": "t", "embedding_model_endpoint_name": "databricks-gte-large-en"}]},
        }

    def test_direct_access_skipped(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        res = migrate_index(client, self._row({"index_type": "DIRECT_ACCESS"}),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_direct_access_unsupported"
        client.vector_search_indexes.create_index.assert_not_called()

    def test_delta_sync_created_resync_pending(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        ep = MagicMock(); ep.endpoint_status.state = "ONLINE"
        client.vector_search_endpoints.get_endpoint.return_value = ep
        res = migrate_index(client, self._row(self._delta_def()),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "created_resync_pending"
        kwargs = client.vector_search_indexes.create_index.call_args.kwargs
        assert kwargs["name"] == "cat.sch.idx"
        assert kwargs["endpoint_name"] == "ep1"
        assert kwargs["primary_key"] == "id"
        assert str(kwargs["index_type"]).endswith("DELTA_SYNC")
        assert kwargs["delta_sync_index_spec"].source_table == "cat.sch.src"

    def test_endpoint_not_ready_defers(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        client.vector_search_endpoints.get_endpoint.side_effect = Exception("nf")
        res = migrate_index(client, self._row(self._delta_def()),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_endpoint_not_ready"
        client.vector_search_indexes.create_index.assert_not_called()

    def test_already_exists_is_skipped_target_exists(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        ep = MagicMock(); ep.endpoint_status.state = "ONLINE"
        client.vector_search_endpoints.get_endpoint.return_value = ep
        client.vector_search_indexes.create_index.side_effect = Exception("RESOURCE_ALREADY_EXISTS: index exists")
        res = migrate_index(client, self._row(self._delta_def()),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_target_exists"

    def test_create_failure_is_failed(self):
        from migrate.vector_search_worker import migrate_index
        client = MagicMock()
        ep = MagicMock(); ep.endpoint_status.state = "ONLINE"
        client.vector_search_endpoints.get_endpoint.return_value = ep
        client.vector_search_indexes.create_index.side_effect = Exception("boom quota exceeded")
        res = migrate_index(client, self._row(self._delta_def()),
                            sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        assert "boom" in res["error_message"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py::TestMigrateIndex -v`
Expected: FAIL (no attribute `migrate_index`).

- [ ] **Step 3: Implement** — add to `src/migrate/vector_search_worker.py`:

```python
def _is_already_exists(exc: Exception) -> bool:
    msg = str(exc).upper()
    return "ALREADY_EXISTS" in msg or "ALREADY EXISTS" in msg


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
    except Exception as exc:  # noqa: BLE001
        if _is_already_exists(exc):
            return _result("skipped_target_exists", f"Index already exists on target: {exc}")
        return _result("failed", f"create_index failed: {exc}")

    return _result("created_resync_pending", None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py -v`
Expected: PASS (13 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/migrate/vector_search_worker.py tests/unit/test_vector_search_worker.py
git commit -m "feat(vs): migrate_index per-index status logic (skip/defer/create/exists/fail)"
```

---

## Task 5: Worker `run()` entry point

**Files:**
- Modify: `src/migrate/vector_search_worker.py` (add `run` + notebook guard)
- Test: `tests/unit/test_vector_search_worker.py` (append `TestRun`)

- [ ] **Step 1: Write the failing test**

```python
class TestRun:
    def test_run_reads_list_migrates_and_records(self, monkeypatch):
        import migrate.vector_search_worker as w

        # stub MigrationConfig / AuthManager / TrackingManager
        monkeypatch.setattr(w.MigrationConfig, "from_workspace_file", staticmethod(lambda: MagicMock()))
        fake_auth = MagicMock()
        monkeypatch.setattr(w, "AuthManager", lambda *a, **k: fake_auth)
        tracker = MagicMock()
        monkeypatch.setattr(w, "TrackingManager", lambda *a, **k: tracker)

        # one delta-sync row published by the orchestrator
        definition = {"index_type": "DELTA_SYNC", "endpoint_name": "ep1", "primary_key": "id",
                      "delta_sync_index_spec": {"source_table": "cat.sch.src", "pipeline_type": "TRIGGERED"}}
        row = {"object_name": "cat.sch.idx", "object_type": "vector_search_index",
               "metadata_json": json.dumps({"definition": definition})}
        dbutils = MagicMock()
        dbutils.jobs.taskValues.get.return_value = json.dumps([row])

        ep = MagicMock(); ep.endpoint_status.state = "ONLINE"
        fake_auth.target_client.vector_search_endpoints.get_endpoint.return_value = ep

        # make the worker's waits instant
        monkeypatch.setattr(w.time, "sleep", lambda s: None)

        w.run(dbutils, MagicMock())

        recorded = tracker.append_migration_status.call_args.args[0]
        assert len(recorded) == 1
        assert recorded[0]["object_name"] == "cat.sch.idx"
        assert recorded[0]["status"] == "created_resync_pending"

    def test_run_empty_list_records_nothing(self, monkeypatch):
        import migrate.vector_search_worker as w
        monkeypatch.setattr(w.MigrationConfig, "from_workspace_file", staticmethod(lambda: MagicMock()))
        monkeypatch.setattr(w, "AuthManager", lambda *a, **k: MagicMock())
        tracker = MagicMock()
        monkeypatch.setattr(w, "TrackingManager", lambda *a, **k: tracker)
        dbutils = MagicMock()
        dbutils.jobs.taskValues.get.return_value = json.dumps([])

        w.run(dbutils, MagicMock())
        tracker.append_migration_status.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py::TestRun -v`
Expected: FAIL (no attribute `run`).

- [ ] **Step 3: Implement** — add to `src/migrate/vector_search_worker.py`, after the helpers (and a final notebook-guard cell):

```python
# COMMAND ----------


def run(dbutils, spark) -> None:  # noqa: ARG001 — spark unused; kept for worker-signature uniformity
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(  # type: ignore[union-attr]
        taskKey="orchestrator", key="vector_search_index_list", debugValue="[]"
    )
    rows = json.loads(rows_json) if rows_json else []
    print(f"[vector_search] {len(rows)} index(es) to migrate")

    results = [migrate_index(auth.target_client, row) for row in rows]

    if results:
        tracker.append_migration_status(results)
    for r in results:
        print(f"  {r['object_name']}: {r['status']}")


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py -v`
Expected: PASS (15 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/migrate/vector_search_worker.py tests/unit/test_vector_search_worker.py
git commit -m "feat(vs): vector_search_worker run() entry point"
```

---

## Task 6: Pre-check — source-table gate

**Files:**
- Create: `src/pre_check/pre_check_vector_search.py`
- Test: `tests/unit/test_pre_check_vector_search.py`

Read `src/pre_check/pre_check_governance.py` first to match the notebook shape and how pre-checks read discovery_inventory + record `pre_check_results`. The function below is the testable core; keep the notebook bootstrap + `run()` + guard consistent with that sibling.

- [ ] **Step 1: Write the failing test**

```python
"""Unit tests for the Vector Search pre-check source-table gate."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pre_check.pre_check_vector_search import find_missing_source_tables


def _delta_row(source_table):
    import json
    return {"object_name": "cat.sch.idx", "object_type": "vector_search_index",
            "metadata_json": json.dumps(
                {"definition": {"index_type": "DELTA_SYNC",
                                "delta_sync_index_spec": {"source_table": source_table}}})}


def _direct_row():
    import json
    return {"object_name": "cat.sch.da", "object_type": "vector_search_index",
            "metadata_json": json.dumps({"definition": {"index_type": "DIRECT_ACCESS"}})}


def test_missing_source_table_is_reported():
    client = MagicMock()
    client.tables.get.side_effect = Exception("TABLE_DOES_NOT_EXIST")
    missing = find_missing_source_tables(client, [_delta_row("cat.sch.src")])
    assert missing == ["cat.sch.src"]


def test_present_source_table_is_ok():
    client = MagicMock()
    client.tables.get.return_value = MagicMock()  # exists
    missing = find_missing_source_tables(client, [_delta_row("cat.sch.src")])
    assert missing == []


def test_direct_access_rows_are_excluded_from_source_check():
    client = MagicMock()
    missing = find_missing_source_tables(client, [_direct_row()])
    assert missing == []
    client.tables.get.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_pre_check_vector_search.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'pre_check.pre_check_vector_search'`).

- [ ] **Step 3: Implement** — create `src/pre_check/pre_check_vector_search.py`:

```python
# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

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
# Vector Search pre-check: a Delta Sync index can only be recreated if its
# source Delta table already exists on target. Fail the job up-front if any are
# missing (Direct Access indexes have no source table and are excluded).

import json

from common.config import MigrationConfig
from common.auth import AuthManager
from common.tracking import TrackingManager


# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def find_missing_source_tables(target_client, rows: list[dict]) -> list[str]:
    """Return source-table FQNs that are absent on target, for Delta Sync rows only."""
    missing: list[str] = []
    for row in rows:
        definition = (json.loads(row.get("metadata_json") or "{}") or {}).get("definition") or {}
        if not str(definition.get("index_type", "")).upper().endswith("DELTA_SYNC"):
            continue  # Direct Access — no source table
        src = (definition.get("delta_sync_index_spec") or {}).get("source_table")
        if not src:
            continue
        try:
            target_client.tables.get(src)
        except Exception:  # noqa: BLE001 — any failure means "treat as absent"
            missing.append(src)
    return missing


# COMMAND ----------


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows = tracker.get_pending_objects("vector_search_index")
    missing = find_missing_source_tables(auth.target_client, rows)

    status = "PASS" if not missing else "FAIL"
    message = "" if not missing else f"Missing source tables on target: {sorted(set(missing))}"
    action = "" if not missing else "Run migrate_uc first so the source tables exist, then re-run."
    # pre_check_results schema: check_name, status, message, action_required
    # (checked_at is auto-added). Writer takes a list[dict]. See
    # src/pre_check/pre_check.py for the canonical usage.
    tracker.append_pre_check_results(
        [
            {
                "check_name": "vector_search_source_tables",
                "status": status,
                "message": message,
                "action_required": action,
            }
        ]
    )

    if missing:
        raise RuntimeError(
            "migrate_vector_search pre-check FAILED — source Delta tables absent on "
            f"target for {len(set(missing))} index(es): {sorted(set(missing))}. "
            "Run migrate_uc first so the source tables exist, then re-run."
        )
    print(f"[vector_search] pre-check PASS — {len(rows)} index row(s), all source tables present.")


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
```

> Signature confirmed against `src/common/tracking.py:278` (`append_pre_check_results(records: list[dict])`) and `src/pre_check/pre_check.py:575`. The `status` values are PASS/WARN/FAIL strings, matching existing checks.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_pre_check_vector_search.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/pre_check/pre_check_vector_search.py tests/unit/test_pre_check_vector_search.py
git commit -m "feat(vs): pre_check_vector_search source-table gate"
```

---

## Task 7: Wire `vector_search_index` into the orchestrator

**Files:**
- Modify: `src/migrate/orchestrator.py` (`LIST_TYPES`)
- Test: `tests/unit/test_orchestrator.py` (append a test — read the file first to match how it tests `LIST_TYPES`/publishing)

- [ ] **Step 1: Write the failing test** — read `tests/unit/test_orchestrator.py` to match its style. Add a test asserting `vector_search_index` is in `LIST_TYPES`:

```python
def test_vector_search_index_is_a_list_type():
    # The shared orchestrator must publish a vector_search_index_list task value
    # for the migrate_vector_search job's worker to consume.
    import inspect
    import migrate.orchestrator as o

    src = inspect.getsource(o.run)
    assert '"vector_search_index"' in src
```

> If `tests/unit/test_orchestrator.py` already imports `LIST_TYPES` as a module constant or exercises `run` with mocks, prefer matching that existing pattern over `inspect.getsource`. Use the source-scan only if `LIST_TYPES` is a local inside `run` (it is, per `orchestrator.py:155`).

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_orchestrator.py::test_vector_search_index_is_a_list_type -v`
Expected: FAIL.

- [ ] **Step 3: Implement** — in `src/migrate/orchestrator.py`, add `"vector_search_index"` to the `LIST_TYPES` tuple (after `"online_table"`), with a comment:

```python
        "online_table",
        # Stateful Services Phase — consumed by the migrate_vector_search job's
        # worker. Harmless for other jobs (they ignore the published list).
        "vector_search_index",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_orchestrator.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/migrate/orchestrator.py tests/unit/test_orchestrator.py
git commit -m "feat(vs): publish vector_search_index_list from orchestrator"
```

---

## Task 8: Production workflow YAML

**Files:**
- Create: `resources/production/migrate_vector_search_workflow.yml`

Read `resources/production/migrate_governance_workflow.yml` first and mirror its structure exactly (the `resources.jobs.<name>`, `run_as.service_principal_name: ${var.migration_spn_id}`, task `notebook_task.notebook_path` relative paths, `depends_on`, and the `summary` task's `run_if: ALL_DONE`).

- [ ] **Step 1: Create the workflow** — `resources/production/migrate_vector_search_workflow.yml`:

```yaml
resources:
  jobs:
    migrate_vector_search:
      name: migrate_vector_search
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: pre_check_vector_search
          notebook_task:
            notebook_path: ../../src/pre_check/pre_check_vector_search.py
        - task_key: orchestrator
          depends_on:
            - task_key: pre_check_vector_search
          notebook_task:
            notebook_path: ../../src/migrate/orchestrator.py
        - task_key: migrate_vector_search
          depends_on:
            - task_key: orchestrator
          notebook_task:
            notebook_path: ../../src/migrate/vector_search_worker.py
        - task_key: summary_vector_search
          depends_on:
            - task_key: migrate_vector_search
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../../src/migrate/summary.py
```

> Match the exact indentation/keys/version of the sibling YAML (e.g. whether jobs declare `tags`, `max_concurrent_runs`, `queue`, or a `parameters`/`environments` block). Copy any job-level scaffolding that `migrate_governance_workflow.yml` has and this one lacks. The task graph above is the intended shape.

- [ ] **Step 2: Validate the bundle parses**

Run: `databricks bundle validate -t dev --profile source-migration 2>&1 | tail -5`
Expected: validation succeeds (the new job is recognized). If `databricks`/profile is unavailable in this environment, instead run the notebook-shape lint to confirm the referenced notebooks are well-formed: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (expect 0 failures).

- [ ] **Step 3: Commit**

```bash
git add resources/production/migrate_vector_search_workflow.yml
git commit -m "feat(vs): migrate_vector_search production workflow"
```

---

## Task 9: Integration test workflow + tolerant assertion

**Files:**
- Create: `resources/integration_tests/vector_search_integration_test_workflow.yml`
- Create: `tests/integration/test_vector_search.py` (notebook-style assertion)

Read `resources/integration_tests/uc_integration_test_workflow.yml` and `tests/integration/seed_uc_test_data.py` first to match the seed→discovery→migrate→test→teardown shape and the `dbutils.jobs.taskValues` flag pattern. VS requires a preview/enabled workspace, so the assertion must be **tolerant** (zero VS rows = pass).

- [ ] **Step 1: Create the tolerant assertion notebook** — `tests/integration/test_vector_search.py`:

```python
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
# Tolerant VS migration assertion: any vector_search_index rows in
# migration_status must carry a known terminal VS status. Zero rows is a pass
# (VS preview may be disabled in the test workspace).

import json  # noqa: E402

_VALID_VS_STATUSES = {
    "created_resync_pending",
    "skipped_direct_access_unsupported",
    "skipped_target_exists",
    "skipped_endpoint_not_ready",
    "failed",
}

_rows = spark.sql(  # noqa: F821
    "SELECT object_name, status "
    "FROM migration_tracking.cp_migration.migration_status "
    "WHERE object_type = 'vector_search_index'"
).collect()

_errors: list[str] = []
if not _rows:
    print("[vector_search] no vector_search_index migration rows — treating as pass (preview likely disabled)")
else:
    for r in _rows:
        if r["status"] not in _VALID_VS_STATUSES:
            _errors.append(f"{r['object_name']}: unexpected status {r['status']}")
        # a healthy migration should not leave 'failed' rows in a green test run
        if r["status"] == "failed":
            _errors.append(f"{r['object_name']}: migration failed")

if _errors:
    raise AssertionError("Vector Search migration assertion failed:\n" + "\n".join(_errors))
print(f"[vector_search] assertion passed ({len(_rows)} row(s))")
```

- [ ] **Step 2: Create the integration test workflow** — `resources/integration_tests/vector_search_integration_test_workflow.yml`, mirroring `uc_integration_test_workflow.yml` (seed → discovery → pre_check → migrate via `run_job_task` on `${resources.jobs.migrate_vector_search.id}` → test → teardown). Seeding a real VS endpoint+index is optional/preview-gated; the workflow may seed best-effort and rely on the tolerant assertion. Match the sibling YAML's exact structure:

```yaml
resources:
  jobs:
    vector_search_integration_test:
      name: vector_search_integration_test
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: discovery
          notebook_task:
            notebook_path: ../../src/discovery/discovery.py
        - task_key: migrate_vector_search
          depends_on:
            - task_key: discovery
          run_job_task:
            job_id: ${resources.jobs.migrate_vector_search.id}
        - task_key: test_vector_search
          depends_on:
            - task_key: migrate_vector_search
          notebook_task:
            notebook_path: ../../tests/integration/test_vector_search.py
```

> Adjust to match the sibling integration YAML's real structure (seed/teardown tasks, `run_job_task` syntax, any parameters). If seeding VS objects is added, follow the `seed_uc_test_data.py` try/except + `taskValues.set(key="has_vs_index", ...)` pattern.

- [ ] **Step 3: Verify notebook shape + bundle parse**

Run: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (expect 0 failures — the new test notebook must have no indented `# COMMAND ----------`).
Run (if available): `databricks bundle validate -t dev --profile source-migration 2>&1 | tail -5`.

- [ ] **Step 4: Commit**

```bash
git add resources/integration_tests/vector_search_integration_test_workflow.yml tests/integration/test_vector_search.py
git commit -m "test(vs): vector_search integration workflow + tolerant assertion"
```

---

## Task 10: Docs — user guide + stateful phase note

**Files:**
- Modify: `docs/user_guide.md`
- Modify: `docs/stateful_services_phase.md`

- [ ] **Step 1: Add a `migrate_vector_search` section to `docs/user_guide.md`** — read the file's structure first and append a section consistent with how other jobs are documented. It MUST cover:
  - What it does: recreates Delta Sync VS indexes on target (re-sync from the same-named source table); endpoint create-if-missing.
  - Opt-in model: running the job is the opt-in (no config flag).
  - Statuses: `created_resync_pending` (index created, re-embedding in progress — not yet queryable), `skipped_endpoint_not_ready` (re-run will retry), `skipped_target_exists`.
  - Precondition: run `migrate_uc` first (the pre-check fails the job if a source table is missing).
  - **Known limitations** (verbatim intent):
    - "Direct Vector Access indexes are not migrated — they are recorded `skipped_direct_access_unsupported`. Their vectors are written directly by your application and cannot be recreated by this tool."
    - "Custom embedding-model serving endpoints are not checked or migrated. If a Delta Sync index uses a custom model serving endpoint for embeddings, ensure that endpoint exists on the target before running. Databricks-hosted embedding models are unaffected."

- [ ] **Step 2: Update `docs/stateful_services_phase.md`** — change the Vector Search row's "Current-tool behaviour" cell from "Not in scope for the core tool." to note the new job, e.g.: "Migrated by the `migrate_vector_search` job (Delta Sync indexes → recreate + re-sync). Direct Access indexes skipped; see user guide."

- [ ] **Step 3: Commit**

```bash
git add docs/user_guide.md docs/stateful_services_phase.md
git commit -m "docs(vs): migrate_vector_search user guide section + stateful phase note"
```

---

## Task 11: Full suite + lint + PR

- [ ] **Step 1: Run the full unit + lint suite**

Run: `.venv/bin/python -m pytest tests/unit tests/lint -q`
Expected: all pass (prior count + the new `test_vector_search_worker.py`, `test_pre_check_vector_search.py`, and the added tracking/orchestrator tests).

- [ ] **Step 2: Lint the changed set**

Run: `.venv/bin/ruff check src/migrate/vector_search_worker.py src/pre_check/pre_check_vector_search.py src/migrate/orchestrator.py src/common/tracking.py tests/unit/test_vector_search_worker.py tests/unit/test_pre_check_vector_search.py`
Expected: clean. Match existing `# noqa` conventions; import `Callable`/abc styles as the repo uses.

- [ ] **Step 3: Commit any lint fixups**

```bash
git add -A && git commit -m "chore(vs): lint fixups" || echo "nothing to fix"
```

- [ ] **Step 4: Push + open PR**

```bash
git push -u databricks-solutions feat/migrate-vector-search
gh pr create --repo databricks-solutions/workspace-migration --base main --head feat/migrate-vector-search \
  --title "feat: migrate_vector_search job (Delta Sync VS index migration)" \
  --body "$(cat <<'EOF'
## Summary

New standalone `migrate_vector_search` job — the first migration capability of the Stateful Services Phase. Migrates **Delta Sync** Vector Search indexes from source to target by recreating them (which re-syncs/re-embeds from the same-named target source table). Consumes the `source_type='stateful'` `vector_search_index` rows produced by the discovery extension (PR #53).

- Task chain: `pre_check_vector_search → orchestrator → migrate_vector_search → summary_vector_search`.
- Pre-check fails the job up-front if any Delta Sync index's source Delta table is absent on target.
- Worker ensures the target VS endpoint exists (create-if-missing), then recreates each index → terminal `created_resync_pending` (honest: index exists, re-embedding still running). Endpoint still provisioning → `skipped_endpoint_not_ready` (re-pickable). Already exists → `skipped_target_exists`.
- New terminal statuses: `created_resync_pending`, `skipped_direct_access_unsupported`.

### Known limitations (deferred — see spec)
- **Direct Access indexes are not migrated** (`skipped_direct_access_unsupported`) — their vectors are external app state.
- **Custom embedding-model serving endpoints are not checked/migrated** — documented operator precondition.

Spec: `docs/superpowers/specs/2026-06-03-vector-search-migration-design.md`
Plan: `docs/superpowers/plans/2026-06-03-vector-search-migration.md`

## Testing
- Full unit + lint suite passes; new worker + pre-check unit tests; notebook-shape lint clean.
- Tolerant integration assertion (zero VS rows = pass; VS preview may be disabled in the test workspace).

This pull request and its description were written by Isaac.
EOF
)"
```

> Ask the user which merge strategy before merging (do not default to squash); pass `--delete-branch`.

---

## Self-Review

**Spec coverage:**
- New standalone `migrate_vector_search` job (Q1) → Tasks 7–9 (orchestrator wire + workflow YAML). ✓
- Delta Sync only; Direct Access terminal skip (Q7) → Task 4 (`migrate_index` direct-access branch) + Task 1 status. ✓
- Pre-check source-table gate (Q3) → Task 6 (`find_missing_source_tables` + raising `run`). ✓
- `created_resync_pending`, no long wait (Q4) → Task 1 status + Task 4 success path. ✓
- Endpoint create-if-missing (Q5) → Task 3 (`_ensure_endpoint`) + Task 4 wiring; not-ready → `skipped_endpoint_not_ready`. ✓
- No config flag (Q6) → none added; opt-in documented in Task 10. ✓
- Docs incl. limitations → Task 10. ✓
- Tests (unit + tolerant integration) → Tasks 2–6 unit + Task 9 integration. ✓

**Placeholder scan:** No TBD/TODO. The "read sibling X first / confirm exact signature" notes (Tasks 6, 8, 9) are real instructions to match existing patterns whose exact byte-shape isn't reproduced here — not deferred work. Every code step contains complete code.

**Type/name consistency:** helper names (`_is_delta_sync`, `_build_delta_sync_spec`, `_endpoint_state_ready`, `_ensure_endpoint`, `_is_already_exists`, `migrate_index`, `run`) are defined once and reused consistently. Statuses match between Task 1 (registration), Task 4 (emission), and Task 9 (assertion set). Task-value key `vector_search_index_list` matches between Task 7 (publish) and Task 5 (`run` reads it). `migrate_index` signature (with `max_attempts`/`sleep_seconds`/`sleep_fn` kwargs) is consistent between Task 4 definition and Task 5 caller (caller uses defaults). The Task 6 pre-check writer is confirmed (`append_pre_check_results(records: list[dict])`, schema `check_name/status/message/action_required`). Remaining implementer-confirm items are byte-exact YAML scaffolding (Tasks 8–9, match the sibling workflow YAMLs) — intentional, not deferred logic.
