# Online Tables Migration (`migrate_online_tables`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Phase-4 online-table hard-exclude with a real migration in a new standalone `migrate_online_tables` job that recreates each online table on the target via the typed SDK (re-sync from the migrated source table), removes online-table handling from `migrate_uc`, and is proven by a real positive-only live integration test.

**Architecture:** New `migrate_online_tables` job (`pre_check_online_tables → orchestrator → migrate_online_tables → summary_online_tables`) reusing the shared orchestrator's `online_table_list`. The rewritten worker reconstructs `OnlineTableSpec` from the discovered definition (dropping response-only `pipeline_id`) and calls `target_client.online_tables.create`, recording `created_resync_pending` / `skipped_target_exists` / `failed` with full per-row isolation, keyed by the online-table FQN. A source-table pre-check gates the job. Online-table handling is removed from `migrate_uc`. Mirrors the live-validated `migrate_vector_search` job (PR #54), minus endpoint + Direct-Access.

**Tech Stack:** Python 3.11, `databricks-sdk` 0.102.0 (`online_tables` typed client, `OnlineTable`/`OnlineTableSpec`), DABs, `common.auth`/`common.sql_utils`, the `migration_tracking.cp_migration` tables. Spec: `docs/superpowers/specs/2026-06-03-online-tables-migration-design.md`.

**Verified SDK surface (0.102.0):**
- `target_client.online_tables.create(OnlineTable) -> Wait[OnlineTable]`; `.get(name) -> OnlineTable`; `.delete(name)`.
- `OnlineTable(name, spec, status, table_serving_url, unity_catalog_provisioning_state)`.
- `OnlineTableSpec(source_table_full_name, primary_key_columns, timeseries_key, run_triggered, run_continuously, perform_full_copy, pipeline_id)`; `OnlineTableSpec.from_dict(d)` parses sync-mode sub-objects and drops nothing — so the caller must `pop("pipeline_id", None)` (response-only). Verified: `from_dict({...,"run_triggered":{}})` → `run_triggered=OnlineTableSpecTriggeredSchedulingPolicy()`, and the rebuilt spec's `as_dict()` has no `pipeline_id`.
- `OnlineTableSpecTriggeredSchedulingPolicy` (used by the seed to create a Triggered table).
- `databricks.sdk.errors.AlreadyExists` / `NotFound`.
- Discovered `metadata_json.definition` (from `list_online_tables`): `{ "name": <fqn>, "spec": {source_table_full_name, primary_key_columns, run_triggered|run_continuously|perform_full_copy, timeseries_key, pipeline_id}, ... }`. The discovery row's `object_name` is the FQN.

---

## File Structure

- **Rewrite** `src/migrate/online_tables_worker.py` — hard-exclude → real migration.
- **Create** `src/pre_check/pre_check_online_tables.py` — source-table gate.
- **Create** `resources/production/migrate_online_tables_workflow.yml` — new job.
- **Modify** `resources/production/migrate_uc_workflow.yml` — remove OT task + summary refs.
- **Create** `resources/integration_tests/online_tables_integration_test_workflow.yml`.
- **Create** `tests/integration/{seed_online_tables_test_data,test_online_tables,teardown_online_tables}.py`.
- **Create** `tests/unit/test_online_tables_worker.py` (worker has no unit test today) + `tests/unit/test_pre_check_online_tables.py`.
- **Modify** any unit test that asserts the old `online_table → skipped_by_stateful_service_migration` (Task 1 keeps the suite green).
- **Modify** `docs/user_guide.md`, `docs/stateful_services_phase.md`.

Shared identifiers / statuses:
- statuses (all already exist): `created_resync_pending`, `skipped_target_exists`, `failed`.
- worker helpers: `_build_online_table_spec(definition)`, `migrate_online_table(target_client, row)`, `run`.
- task-value key (already published by orchestrator): `online_table_list`.
- integration identifiers: catalog `integration_test_src`, schema `ot_test`, source table `integration_test_src.ot_test.ot_source` (PK on `id`), online table `integration_test_src.ot_test.ot_online`.

---

## Task 1: Rewrite the worker (hard-exclude → real migration)

**Files:**
- Modify: `src/migrate/online_tables_worker.py` (full rewrite of the body cells; keep the bootstrap cell)
- Create: `tests/unit/test_online_tables_worker.py`
- Modify: any unit test that breaks because online_table is no longer hard-excluded (keep suite green)

- [ ] **Step 1: Write the failing tests** — create `tests/unit/test_online_tables_worker.py`:

```python
"""Unit tests for the Online Tables migration worker."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from databricks.sdk.errors import AlreadyExists

from migrate.online_tables_worker import _build_online_table_spec, migrate_online_table


def _definition():
    return {
        "name": "cat.sch.ot",
        "spec": {
            "source_table_full_name": "cat.sch.src",
            "primary_key_columns": ["id"],
            "run_triggered": {},
            "pipeline_id": "pl-123",  # response-only, must be dropped on create
        },
    }


def _row(definition):
    return {"object_name": "cat.sch.ot", "object_type": "online_table",
            "metadata_json": json.dumps({"definition": definition})}


class TestBuildSpec:
    def test_builds_spec_and_drops_pipeline_id(self):
        spec = _build_online_table_spec(_definition())
        assert spec.source_table_full_name == "cat.sch.src"
        assert spec.primary_key_columns == ["id"]
        assert spec.run_triggered is not None
        assert "pipeline_id" not in spec.as_dict()


class TestMigrateOnlineTable:
    def test_created_resync_pending_and_object_name_is_fqn(self):
        client = MagicMock()
        res = migrate_online_table(client, _row(_definition()))
        assert res["status"] == "created_resync_pending"
        # object_name must be the plain FQN (matches discovery), not ONLINE_TABLE_*
        assert res["object_name"] == "cat.sch.ot"
        assert res["object_type"] == "online_table"
        ot_arg = client.online_tables.create.call_args.args[0]
        assert ot_arg.name == "cat.sch.ot"
        assert ot_arg.spec.source_table_full_name == "cat.sch.src"

    def test_already_exists_is_skipped_target_exists(self):
        client = MagicMock()
        client.online_tables.create.side_effect = AlreadyExists("exists")
        res = migrate_online_table(client, _row(_definition()))
        assert res["status"] == "skipped_target_exists"

    def test_create_failure_is_failed(self):
        client = MagicMock()
        client.online_tables.create.side_effect = Exception("boom quota")
        res = migrate_online_table(client, _row(_definition()))
        assert res["status"] == "failed"
        assert "boom" in res["error_message"]

    def test_missing_spec_is_failed_not_raised(self):
        client = MagicMock()
        row = {"object_name": "cat.sch.ot", "object_type": "online_table",
               "metadata_json": json.dumps({"definition": {"name": "cat.sch.ot"}})}
        res = migrate_online_table(client, row)
        assert res["status"] == "failed"
        client.online_tables.create.assert_not_called()

    def test_malformed_metadata_is_failed_not_raised(self):
        client = MagicMock()
        row = {"object_name": "cat.sch.ot", "object_type": "online_table", "metadata_json": "{not json"}
        res = migrate_online_table(client, row)
        assert res["status"] == "failed"
        client.online_tables.create.assert_not_called()
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_online_tables_worker.py -v`
Expected: FAIL (`ImportError: cannot import name '_build_online_table_spec'` — the current worker only has `apply_online_table`).

- [ ] **Step 3: Rewrite the worker** — replace the body of `src/migrate/online_tables_worker.py` (keep the existing first bootstrap cell verbatim) with:

```python
# COMMAND ----------
# Online Tables migration worker. Recreates each online table on the target by
# replaying its spec (pointing at the same-named, already-migrated source Delta
# table), which triggers a fresh re-sync. Sync history/freshness is NOT
# transferred (same accepted trade-off as Vector Search re-embed). Consumes the
# orchestrator's online_table_list task value.
# Spec: docs/superpowers/specs/2026-06-03-online-tables-migration-design.md

import json
import logging
import time

from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.catalog import OnlineTable, OnlineTableSpec

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("online_tables_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def _build_online_table_spec(definition: dict) -> OnlineTableSpec:
    """Reconstruct a create-spec from the discovered online-table definition.

    The discovered ``spec`` is the GET response shape, carrying a response-only
    ``pipeline_id`` not accepted on create — drop it. ``from_dict`` parses the
    sync-mode sub-objects (run_triggered / run_continuously / perform_full_copy)
    plus primary_key_columns / timeseries_key.
    """
    spec_dict = dict(definition.get("spec") or {})
    spec_dict.pop("pipeline_id", None)
    return OnlineTableSpec.from_dict(spec_dict)


def migrate_online_table(target_client, row: dict) -> dict:
    """Migrate one online_table discovery row. Returns a status dict. Fully
    exception-safe: any error for a single online table becomes ``failed`` so
    one bad row never aborts the batch."""
    start = time.time()
    obj_name = row.get("object_name") or ""

    def _result(status: str, error: str | None = None) -> dict:
        return {
            "object_name": obj_name,
            "object_type": "online_table",
            "status": status,
            "error_message": error,
            "duration_seconds": time.time() - start,
        }

    try:
        meta = json.loads(row.get("metadata_json") or "{}")
        definition = meta.get("definition") or {}
        if not definition.get("spec"):
            return _result("failed", "discovery row has no spec in metadata_json — cannot migrate online table.")
        fqn = definition.get("name") or obj_name
        spec = _build_online_table_spec(definition)
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"online table spec rebuild failed: {exc}")

    try:
        target_client.online_tables.create(OnlineTable(name=fqn, spec=spec))
    except AlreadyExists as exc:
        return _result("skipped_target_exists", f"Online table already exists on target: {exc}")
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"online_tables.create failed: {exc}")

    return _result("created_resync_pending", None)


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(  # type: ignore[union-attr]
        taskKey="orchestrator", key="online_table_list", debugValue="[]"
    )
    rows = json.loads(rows_json) if rows_json else []
    logger.info("Received %d online_table record(s).", len(rows))

    results = [migrate_online_table(auth.target_client, row) for row in rows]
    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Online tables worker complete: %d created_resync_pending, %d skipped_target_exists, %d failed.",
        sum(1 for r in results if r["status"] == "created_resync_pending"),
        sum(1 for r in results if r["status"] == "skipped_target_exists"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
```

- [ ] **Step 4: Run the new test, then the whole unit suite**

Run: `.venv/bin/python -m pytest tests/unit/test_online_tables_worker.py -v` — expect PASS (6 tests).
Run: `.venv/bin/python -m pytest tests/unit tests/lint -q` — some PRE-EXISTING tests may now FAIL because they asserted the old `online_table → skipped_by_stateful_service_migration` behavior. Known candidates: `tests/unit/test_phase3_workers.py`, `tests/unit/test_idempotency_audit.py`. For EACH failing test:
  - If it asserts `apply_online_table(...)` returns `skipped_by_stateful_service_migration` → that function no longer exists; rewrite the test to assert the new `migrate_online_table(...)` real-migration behavior (use a `MagicMock` target_client, assert `created_resync_pending` + `online_tables.create` called), OR delete it if it duplicates the new `test_online_tables_worker.py`.
  - If it asserts online-table idempotency via the skip → update to the new contract (`AlreadyExists` → `skipped_target_exists`).
  Do NOT touch `mv_st_worker` / MV / ST assertions — those stay hard-excluded.

- [ ] **Step 5: Lint + confirm whole suite green + commit**

Run: `.venv/bin/ruff check src/migrate/online_tables_worker.py tests/unit/test_online_tables_worker.py` (clean)
Run: `.venv/bin/python -m pytest tests/unit tests/lint -q` (all pass)
```bash
git add src/migrate/online_tables_worker.py tests/unit/test_online_tables_worker.py
# also add any pre-existing unit test you updated for the behavior change
git commit -m "feat(ot): real online-table migration worker (recreate spec via SDK) + object_name FQN fix"
```

---

## Task 2: Pre-check — source-table gate

**Files:**
- Create: `src/pre_check/pre_check_online_tables.py`
- Test: `tests/unit/test_pre_check_online_tables.py`

Read `src/pre_check/pre_check_vector_search.py` first — this mirrors it (same bootstrap, `append_pre_check_results` shape `{check_name,status,message,action_required}`, raise-on-missing, broad-except → warn-then-treat-as-absent).

- [ ] **Step 1: Write the failing test** — `tests/unit/test_pre_check_online_tables.py`:

```python
"""Unit tests for the Online Tables pre-check source-table gate."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from pre_check.pre_check_online_tables import find_missing_source_tables


def _row(source_table):
    return {"object_name": "cat.sch.ot", "object_type": "online_table",
            "metadata_json": json.dumps({"definition": {"spec": {"source_table_full_name": source_table}}})}


def test_missing_source_table_is_reported():
    client = MagicMock()
    client.tables.get.side_effect = Exception("TABLE_DOES_NOT_EXIST")
    assert find_missing_source_tables(client, [_row("cat.sch.src")]) == ["cat.sch.src"]


def test_present_source_table_is_ok():
    client = MagicMock()
    client.tables.get.return_value = MagicMock()
    assert find_missing_source_tables(client, [_row("cat.sch.src")]) == []


def test_row_without_source_table_is_skipped():
    client = MagicMock()
    row = {"object_name": "x", "object_type": "online_table",
           "metadata_json": json.dumps({"definition": {"spec": {}}})}
    assert find_missing_source_tables(client, [row]) == []
    client.tables.get.assert_not_called()
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_pre_check_online_tables.py -v`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement** — create `src/pre_check/pre_check_online_tables.py`:

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
# Online Tables pre-check: an online table can only be recreated if its source
# Delta table already exists on target. Fail the job up-front if any are missing.

import json
import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pre_check_online_tables")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def find_missing_source_tables(target_client, rows: list[dict]) -> list[str]:
    """Return source-table FQNs absent on target for the given online_table rows."""
    missing: list[str] = []
    for row in rows:
        definition = (json.loads(row.get("metadata_json") or "{}") or {}).get("definition") or {}
        src = (definition.get("spec") or {}).get("source_table_full_name")
        if not src:
            continue
        try:
            target_client.tables.get(src)
        except Exception as exc:  # noqa: BLE001 — any failure (absent / transient / permission) blocks the gate
            logger.warning("tables.get(%r) failed — treating source table as absent: %s", src, exc)
            missing.append(src)
    return missing


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows = tracker.get_pending_objects("online_table")
    missing = find_missing_source_tables(auth.target_client, rows)

    status = "PASS" if not missing else "FAIL"
    message = "" if not missing else f"Missing source tables on target: {sorted(set(missing))}"
    action = "" if not missing else "Run migrate_uc first so the source tables exist, then re-run."
    tracker.append_pre_check_results(
        [
            {
                "check_name": "online_table_source_tables",
                "status": status,
                "message": message,
                "action_required": action,
            }
        ]
    )

    if missing:
        raise RuntimeError(
            "migrate_online_tables pre-check FAILED — source Delta tables absent on "
            f"target for {len(set(missing))} online table(s): {sorted(set(missing))}. "
            "Run migrate_uc first so the source tables exist, then re-run."
        )
    logger.info("[online_tables] pre-check PASS — %d online table row(s), all source tables present.", len(rows))


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
```

- [ ] **Step 4: Run tests + lint**

Run: `.venv/bin/python -m pytest tests/unit/test_pre_check_online_tables.py -v` (3 pass)
Run: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (0 failures)
Run: `.venv/bin/ruff check src/pre_check/pre_check_online_tables.py tests/unit/test_pre_check_online_tables.py` (clean)

- [ ] **Step 5: Commit**

```bash
git add src/pre_check/pre_check_online_tables.py tests/unit/test_pre_check_online_tables.py
git commit -m "feat(ot): pre_check_online_tables source-table gate"
```

---

## Task 3: Production workflow + remove OT from migrate_uc

**Files:**
- Create: `resources/production/migrate_online_tables_workflow.yml`
- Modify: `resources/production/migrate_uc_workflow.yml`

Read `resources/production/migrate_vector_search_workflow.yml` (mirror it exactly) and `resources/production/migrate_uc_workflow.yml`.

- [ ] **Step 1: Create `resources/production/migrate_online_tables_workflow.yml`** mirroring the VS workflow's structure (job name pattern, run_as, summary base_parameters):

```yaml
resources:
  jobs:
    migrate_online_tables:
      name: "${var.job_prefix}-migrate-online-tables"
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: pre_check_online_tables
          notebook_task:
            notebook_path: ../../src/pre_check/pre_check_online_tables.py
        - task_key: orchestrator
          depends_on:
            - task_key: pre_check_online_tables
          notebook_task:
            notebook_path: ../../src/migrate/orchestrator.py
        - task_key: migrate_online_tables
          depends_on:
            - task_key: orchestrator
          notebook_task:
            notebook_path: ../../src/migrate/online_tables_worker.py
        - task_key: summary_online_tables
          depends_on:
            - task_key: migrate_online_tables
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../../src/migrate/summary.py
            base_parameters:
              object_types: "online_table"
```
> Match the VS workflow's exact key spellings/indentation. If the VS summary task passes additional base_parameters keys, mirror them with OT-appropriate values.

- [ ] **Step 2: Modify `resources/production/migrate_uc_workflow.yml`** — three edits:
  1. Delete the `migrate_online_tables` task block (the `- task_key: migrate_online_tables` ... `notebook_path: ../../src/migrate/online_tables_worker.py`).
  2. In `summary_uc`'s `depends_on`, remove the `- task_key: migrate_online_tables` line.
  3. In `summary_uc`'s `base_parameters.object_types`, remove `online_table` from the comma list (result: `managed_table,external_table,view,function,volume,mv,st,registered_model,grant`).

- [ ] **Step 3: Validate**

Run: `.venv/bin/python -c "import yaml; yaml.safe_load(open('resources/production/migrate_online_tables_workflow.yml')); yaml.safe_load(open('resources/production/migrate_uc_workflow.yml')); print('YAML OK')"`
Run (if available): `BUNDLE_VAR_migration_spn_id=x databricks bundle validate -t dev --profile source-migration 2>&1 | tail -8` (report; the migration_spn_id var error is expected if unset).
Run: `.venv/bin/python -m pytest tests/unit tests/lint -q` (still all pass — removing the migrate_uc task shouldn't break unit tests; if a test asserts the migrate_uc task list contains migrate_online_tables, update it).

- [ ] **Step 4: Commit**

```bash
git add resources/production/migrate_online_tables_workflow.yml resources/production/migrate_uc_workflow.yml
git commit -m "feat(ot): migrate_online_tables production job + remove OT from migrate_uc"
```

---

## Task 4: Integration test (seed + assertion + teardown + workflow)

**Files:**
- Create: `tests/integration/seed_online_tables_test_data.py`
- Create: `tests/integration/test_online_tables.py`
- Create: `tests/integration/teardown_online_tables.py`
- Create: `resources/integration_tests/online_tables_integration_test_workflow.yml`

Read the VS integration files (`seed_vector_search_test_data.py`, `test_vector_search.py`, `teardown_vector_search.py`, `vector_search_integration_test_workflow.yml`) as templates. All `# COMMAND ----------` markers at column 0; these run live, not unit-tested. Seed + assertion MUST `dbutils.notebook.exit(json.dumps(...))` so outcomes are retrievable via the Jobs API (the VS lesson — notebook stdout is not captured).

- [ ] **Step 1: Create `tests/integration/seed_online_tables_test_data.py`:**

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
```

- [ ] **Step 2: Create `tests/integration/test_online_tables.py`:**

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
# Live Online Tables migration assertion (positive case).
#   migration_status == created_resync_pending AND the online table exists on
#   the TARGET (online_tables.get succeeds). Skipped (not failed) if the seed
#   could not create the source online table (preview unavailable).

import json

from databricks.sdk.errors import NotFound

from common.auth import AuthManager
from common.config import MigrationConfig

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_target = _auth.target_client

_has_ot = dbutils.jobs.taskValues.get(taskKey="seed_online_tables", key="has_online_table", debugValue="false")  # noqa: F821
_ot_fqn = dbutils.jobs.taskValues.get(taskKey="seed_online_tables", key="online_table_fqn", debugValue="")  # noqa: F821

errors: list[str] = []
summary: dict = {}


def _latest_status(fqn: str):
    _safe = fqn.replace("'", "''")
    rows = spark.sql(  # noqa: F821
        "SELECT status FROM migration_tracking.cp_migration.migration_status "
        f"WHERE object_type = 'online_table' AND object_name = '{_safe}' "
        "ORDER BY migrated_at DESC LIMIT 1"
    ).collect()
    return rows[0]["status"] if rows else None


def _exists_on_target(fqn: str) -> bool:
    try:
        _target.online_tables.get(fqn)
        return True
    except NotFound:
        return False


# COMMAND ----------
if _has_ot == "true":
    _n = len(errors)
    _status = _latest_status(_ot_fqn)
    if _status != "created_resync_pending":
        errors.append(f"POSITIVE: {_ot_fqn} status={_status!r}, expected 'created_resync_pending'")
    if not _exists_on_target(_ot_fqn):
        errors.append(f"POSITIVE: {_ot_fqn} not found on target — migration did not create the online table")
    if len(errors) == _n:
        summary["online_table"] = "asserted_ok"
        print(f"[test-ot] POSITIVE ok: {_ot_fqn} created_resync_pending + present on target")
    else:
        summary["online_table"] = "FAILED"
else:
    summary["online_table"] = "skipped_no_seed"
    print("[test-ot] skipped — seed did not create the online table (preview unavailable?)")

# COMMAND ----------
_result = json.dumps({"summary": summary, "errors": errors})
if errors:
    raise AssertionError("Online Tables live integration assertion FAILED: " + _result)
print("[test-ot] passed: " + _result)
dbutils.notebook.exit(_result)  # noqa: F821
```

- [ ] **Step 3: Create `tests/integration/teardown_online_tables.py`** (best-effort, independent per-side, mirroring `teardown_vector_search.py`):

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
# Best-effort teardown for the live Online Tables integration test. Deletes the
# online table on BOTH source and target, drops the test catalog on both sides,
# clears tracking rows. Every step try/excepted — nothing here raises.

import contextlib

from databricks.sdk import WorkspaceClient

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_src"
_OT_FQN = f"{_CATALOG}.ot_test.ot_online"


def _source_client():
    return WorkspaceClient()


def _target_client():
    return AuthManager(MigrationConfig.from_workspace_file(), dbutils).target_client  # noqa: F821


# COMMAND ----------
for _make_client, _label in ((_source_client, "source"), (_target_client, "target")):
    try:
        _client = _make_client()
    except Exception as _exc:  # noqa: BLE001
        print(f"[teardown-ot] could not build {_label} client — skipping {_label}: {_exc}")
        continue
    with contextlib.suppress(Exception):
        _client.online_tables.delete(_OT_FQN)
        print(f"[teardown-ot] deleted online table {_OT_FQN} on {_label}")

# COMMAND ----------
with contextlib.suppress(Exception):
    spark.sql(f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")  # noqa: F821
    print(f"[teardown-ot] dropped source catalog {_CATALOG}")

with contextlib.suppress(Exception):
    _auth = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _wh = find_warehouse(_auth)
    execute_and_poll(_auth, _wh, f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")
    print(f"[teardown-ot] dropped target catalog {_CATALOG}")

# COMMAND ----------
with contextlib.suppress(Exception):
    spark.sql(  # noqa: F821
        f"DELETE FROM migration_tracking.cp_migration.migration_status WHERE object_name = '{_OT_FQN}'"
    )
with contextlib.suppress(Exception):
    spark.sql(  # noqa: F821
        f"DELETE FROM migration_tracking.cp_migration.discovery_inventory WHERE object_name = '{_OT_FQN}'"
    )
print("[teardown-ot] tracking rows cleared")
```

- [ ] **Step 4: Create `resources/integration_tests/online_tables_integration_test_workflow.yml`** mirroring `vector_search_integration_test_workflow.yml` (same setup_test_config base_parameters incl. `catalog_filter: "integration_test_src"`, run_job_task on `${resources.jobs.migrate_online_tables.id}`, teardown ALL_DONE depending on all prior):

```yaml
resources:
  jobs:
    online_tables_integration_test:
      name: "${var.job_prefix}-online-tables-integration-test"
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: setup_test_config
          notebook_task:
            notebook_path: ../../tests/integration/setup_test_config.py
            base_parameters:
              iceberg_strategy: ""
              rls_cm_strategy: ""
              migrate_hive_dbfs_root: "false"
              hive_dbfs_target_path: ""
              catalog_filter: "integration_test_src"
        - task_key: seed_online_tables
          depends_on:
            - task_key: setup_test_config
          notebook_task:
            notebook_path: ../../tests/integration/seed_online_tables_test_data.py
        - task_key: discovery
          depends_on:
            - task_key: seed_online_tables
          notebook_task:
            notebook_path: ../../src/discovery/discovery.py
        - task_key: migrate_online_tables
          depends_on:
            - task_key: discovery
          run_job_task:
            job_id: ${resources.jobs.migrate_online_tables.id}
        - task_key: test_online_tables
          depends_on:
            - task_key: migrate_online_tables
          notebook_task:
            notebook_path: ../../tests/integration/test_online_tables.py
        - task_key: teardown_online_tables
          depends_on:
            - task_key: setup_test_config
            - task_key: seed_online_tables
            - task_key: discovery
            - task_key: migrate_online_tables
            - task_key: test_online_tables
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../../tests/integration/teardown_online_tables.py
```
> Match `vector_search_integration_test_workflow.yml`'s exact base_parameters keys; if it omits/adds any, mirror that.

- [ ] **Step 5: Validate + commit**

Run: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (0 failures)
Run: `.venv/bin/ruff check tests/integration/seed_online_tables_test_data.py tests/integration/test_online_tables.py tests/integration/teardown_online_tables.py` (clean)
Run: `.venv/bin/python -c "import yaml; yaml.safe_load(open('resources/integration_tests/online_tables_integration_test_workflow.yml')); print('YAML OK')"`
```bash
git add tests/integration/seed_online_tables_test_data.py tests/integration/test_online_tables.py tests/integration/teardown_online_tables.py resources/integration_tests/online_tables_integration_test_workflow.yml
git commit -m "test(ot): real online-tables integration workflow (seed/assert/teardown, retrievable evidence)"
```

---

## Task 5: Docs

**Files:**
- Modify: `docs/user_guide.md`, `docs/stateful_services_phase.md`

- [ ] **Step 1:** Add a `migrate_online_tables` section to `docs/user_guide.md` (mirror the VS section's structure) covering: what it does (recreate online table on target → re-sync from the migrated source table; **sync history lost**); opt-in = running the job; precondition (run `migrate_uc` first; pre-check fails if a source table is absent); statuses (`created_resync_pending`, `skipped_target_exists`, `failed`); run command. Note that online tables migrated by this job moved OUT of `migrate_uc`.

- [ ] **Step 2:** Update the **Online Tables** row in `docs/stateful_services_phase.md`'s object-types table — change "Current-tool behaviour" from the hard-exclude / POST-with-warning text to: "Migrated by the `migrate_online_tables` job — recreate the spec on target (re-sync from the migrated source Delta table); sync history not transferred."

- [ ] **Step 3: Commit**

```bash
git add docs/user_guide.md docs/stateful_services_phase.md
git commit -m "docs(ot): migrate_online_tables user guide section + stateful phase note"
```

---

## Task 6: Full suite + lint + push + PR

- [ ] **Step 1:** `.venv/bin/python -m pytest tests/unit tests/lint -q` — all pass.
- [ ] **Step 2:** `.venv/bin/ruff check src/migrate/online_tables_worker.py src/pre_check/pre_check_online_tables.py tests/unit/test_online_tables_worker.py tests/unit/test_pre_check_online_tables.py tests/integration/seed_online_tables_test_data.py tests/integration/test_online_tables.py tests/integration/teardown_online_tables.py` — clean.
- [ ] **Step 3:** Commit any lint fixups (`git add -A && git commit -m "chore(ot): lint fixups" || echo nothing`).
- [ ] **Step 4: Push + open PR**

```bash
git push -u databricks-solutions feat/migrate-online-tables
gh pr create --repo databricks-solutions/workspace-migration --base main --head feat/migrate-online-tables \
  --title "feat: migrate_online_tables job (Online Table migration)" \
  --body "$(cat <<'EOF'
## Summary

New standalone `migrate_online_tables` job — recreates UC Online Tables on the target via the typed SDK (`online_tables.create`, replaying the discovered `OnlineTableSpec` minus the response-only `pipeline_id`), pointing at the already-migrated source table so it re-syncs. Replaces the Phase-4 hard-exclude and moves online-table handling OUT of `migrate_uc`.

- Task chain: `pre_check_online_tables → orchestrator → migrate_online_tables → summary_online_tables` (reuses the shared orchestrator's `online_table_list`).
- Statuses: `created_resync_pending` (on accept), `skipped_target_exists`, `failed`; per-row exception isolation.
- **Bug fix:** worker now records `object_name = <FQN>` (matching discovery) instead of the legacy `ONLINE_TABLE_<fqn>` that never reconciled.
- Removed the `migrate_online_tables` task from `migrate_uc` + dropped `online_table` from `summary_uc` object_types.
- Source-table pre-check gate.

Mirrors the live-validated `migrate_vector_search` job (PR #54), minus endpoint + Direct-Access.

Spec: docs/superpowers/specs/2026-06-03-online-tables-migration-design.md
Plan: docs/superpowers/plans/2026-06-03-online-tables-migration.md

## Testing
- Unit + lint suite passes (new worker + pre-check tests; flipped the old hard-exclude assertions).
- Real positive-only live integration test (Triggered online table) with retrievable `notebook.exit` evidence + teardown. [Live-run result appended after Task 7.]

This pull request and its description were written by Isaac.
EOF
)"
```
> Do NOT merge — ask the user for merge strategy later; pass `--delete-branch` when merging.

---

## Task 7: Probe + deploy + run live + report

Runs against the live workspaces. Not a code change.

- [ ] **Step 1: Probe creatability** — confirm an online table can be created on the source before committing to a full run:

```bash
cd ~/uksouth_migration/workspace-migration
.venv/bin/python - <<'PY'
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import OnlineTable, OnlineTableSpec, OnlineTableSpecTriggeredSchedulingPolicy
import contextlib
w = WorkspaceClient(profile="source-migration")
cat, sch = "integration_test_src", "ot_probe"
tbl = f"{cat}.{sch}.probe_src"
ot = f"{cat}.{sch}.probe_ot"
# needs a SQL warehouse to create the table; use the CLI/SDK statement execution or skip if no warehouse.
print("NOTE: run this probe from a Databricks notebook or ensure a warehouse exists; "
      "the goal is to confirm online_tables.create succeeds on this workspace.")
PY
```
Practically: the probe is cleanest run as a one-off Databricks notebook/job, OR just observe the seed task in Step 3 (the seed's `has_online_table` exit value IS the probe — if it's `false`, online tables aren't creatable here). **Decision rule:** if the live run's `seed_online_tables` exits `{"has_online_table": false}`, online tables are not creatable on this pair — report that honestly, keep the test in the repo for a capable workspace, and do NOT claim validation.

- [ ] **Step 2: Deploy**

```bash
cd ~/uksouth_migration/workspace-migration
databricks current-user me --profile source-migration | grep userName
databricks current-user me --profile target-migration | grep userName
BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae \
  DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform DATABRICKS_TF_VERSION=1.15.5 \
  databricks bundle deploy -t dev --profile source-migration
```
Expected: Deployment complete (the local-terraform pin is required — the CLI's own terraform download fails on an expired HashiCorp PGP key).

- [ ] **Step 3: Run the integration job (background) + read evidence**

```bash
BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae \
  databricks bundle run online_tables_integration_test -t dev --profile source-migration
```
When it finishes, read the run output. Confirm:
- `seed_online_tables` exit: `{"has_online_table": true}` (online table really created on source).
- `test_online_tables` exit: `{"summary": {"online_table": "asserted_ok"}, "errors": []}` (migrated → `created_resync_pending` + present on target).
- `teardown_online_tables` ran; verify no leftover online table: `databricks online-tables get integration_test_src.ot_test.ot_online --profile source-migration` and `--profile target-migration` should both 404.

- [ ] **Step 4: Report** the `test_online_tables` outcome + whether it was a real assertion (`asserted_ok`) or a skip (`skipped_no_seed` → online tables not creatable on the pair). Append the result to PR #(this branch). If teardown failed, manually `databricks online-tables delete integration_test_src.ot_test.ot_online` on both profiles.

- [ ] **Step 5:** (no commit) — run report only.

---

## Self-Review

**Spec coverage:**
- New standalone job + reuse orchestrator → Task 3. ✓
- Remove OT from migrate_uc (task + summary deps + object_types) → Task 3 Step 2. ✓
- Worker rewrite, typed SDK create, drop pipeline_id, per-row isolation, `object_name` FQN fix → Task 1. ✓
- Reuse `created_resync_pending`/`skipped_target_exists`, no new statuses → Task 1 (no tracking change). ✓
- Pre-check source-table gate → Task 2. ✓
- Flip existing hard-exclude assertions → Task 1 Step 4. ✓
- Real positive-only integration test w/ retrievable evidence → Task 4. ✓
- Build → probe → run live, honest-on-unavailable → Task 7. ✓
- Docs → Task 5. ✓

**Placeholder scan:** No TBD/TODO. "Read sibling X / match base_parameters" notes are real match-the-pattern instructions (VS files exist as concrete templates). The Task 7 probe is intentionally pragmatic (the seed's `has_online_table` IS the probe) — explicit decision rule given, not a gap.

**Type/name consistency:** `_build_online_table_spec` / `migrate_online_table` / `run` consistent between Task 1 def and its tests; statuses match Task 1 ↔ Task 4 assertion (`created_resync_pending`); object_type `online_table` and `object_name = FQN` consistent across worker, pre-check, assertion, teardown; task-value key `online_table_list` (worker) and seed keys `has_online_table`/`online_table_fqn` (seed ↔ assertion taskKey `seed_online_tables`); integration identifiers (`integration_test_src.ot_test.ot_source` / `ot_online`) identical across seed/assertion/teardown/workflow. No new terminal statuses needed (verified `created_resync_pending` + `skipped_target_exists` already in `_TERMINAL_STATUSES` from the VS work).
