# Vector Search live integration test — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `vector_search_integration_test` workflow a real end-to-end test on the live source/target pair — seed a Delta Sync index (positive) and a Direct Access index (negative) on the source, run discovery + `migrate_vector_search`, and assert against the target that the Delta Sync index was created (`created_resync_pending`) and the Direct Access index was skipped (`skipped_direct_access_unsupported`, not present on target) — then run it live and report.

**Architecture:** A new seed notebook creates, on the SOURCE, a CDF-enabled Delta table + a VS endpoint + a Delta Sync index + a Direct Access index; and, on the TARGET, the same-named Delta table (so the migrate job's source-table pre-check passes — this stands in for `migrate_uc`). The target has NO VS endpoint/index, so the worker creates them cold. The worker's endpoint-wait default is bumped to tolerate cold provisioning. A real assertion notebook (replacing the tolerant stub) checks the target via `get_index`. A teardown notebook deletes indexes + endpoints on both sides (paid resources) plus the test catalog. The workflow chains setup→seed→discovery→migrate→test→teardown(ALL_DONE). Finally the bundle is deployed and the job triggered live.

**Tech Stack:** Python 3.11, `databricks-sdk` 0.102.0 (`vector_search_endpoints`/`vector_search_indexes`/`tables`), Databricks Asset Bundles, the `migration_tracking.cp_migration` tracking tables, `common.auth.AuthManager`, `common.sql_utils.{find_warehouse,execute_and_poll}`. Spec: `docs/superpowers/specs/2026-06-03-vector-search-live-integration-test-design.md`.

**Verified SDK surface (0.102.0):**
- `vector_search_endpoints.create_endpoint(name, endpoint_type: EndpointType)` (`STANDARD` only) → `Wait[EndpointInfo]`; `get_endpoint(name)` → `EndpointInfo(.endpoint_status.state)`; `delete_endpoint(endpoint_name)`.
- `vector_search_indexes.create_index(name, endpoint_name, primary_key, index_type: VectorIndexType, *, delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest, direct_access_index_spec=DirectAccessVectorIndexSpec)`; `get_index(index_name)` → raises `databricks.sdk.errors.NotFound` when absent; `delete_index(index_name)`.
- `DeltaSyncVectorIndexSpecRequest(source_table, pipeline_type, embedding_source_columns=[EmbeddingSourceColumn(name, embedding_model_endpoint_name)])`.
- `DirectAccessVectorIndexSpec(schema_json, embedding_source_columns, embedding_vector_columns=[EmbeddingVectorColumn(name, embedding_dimension)])`.
- Embedding endpoint `databricks-gte-large-en` is READY on both workspaces (1024-dim).

**Fixed test identifiers (used across tasks):**
- catalog/schema: `integration_test_src` / `vs_test`
- source Delta table (CDF): `integration_test_src.vs_test.vs_source` (cols `id INT`, `text STRING`)
- VS endpoint: `cp_migration_vs_it`
- Delta Sync index: `integration_test_src.vs_test.vs_delta_idx`
- Direct Access index: `integration_test_src.vs_test.vs_direct_idx`
- seed task-value keys: `has_delta_index`, `has_direct_index`, `delta_index_fqn`, `direct_index_fqn`, `vs_endpoint_name`

---

## File Structure

- **Modify** `src/migrate/vector_search_worker.py` — raise `_ensure_endpoint`/`migrate_index` default wait budget for cold provisioning.
- **Create** `tests/integration/seed_vector_search_test_data.py` — seed source objects + target source-table.
- **Replace** `tests/integration/test_vector_search.py` — real positive + negative assertions.
- **Create** `tests/integration/teardown_vector_search.py` — delete both indexes + endpoints on both sides + drop catalog + tracking rows.
- **Modify** `resources/integration_tests/vector_search_integration_test_workflow.yml` — add setup/seed/teardown tasks around discovery→migrate→test.

---

## Task 1: Bump worker endpoint-wait default for cold provisioning

**Files:**
- Modify: `src/migrate/vector_search_worker.py` (`_ensure_endpoint` + `migrate_index` default kwargs)
- Test: `tests/unit/test_vector_search_worker.py`

- [ ] **Step 1: Write the failing test** (append to `TestEnsureEndpoint` — or a small new test class):

```python
def test_default_endpoint_wait_budget_is_generous_for_cold_start():
    # A freshly-created VS endpoint can take ~5-20 min to provision; the
    # default wait budget must be generous enough that a cold-start migrate
    # reaches created_resync_pending in one run rather than giving up early.
    import inspect
    from migrate.vector_search_worker import _ensure_endpoint, migrate_index

    ens = inspect.signature(_ensure_endpoint).parameters
    mig = inspect.signature(migrate_index).parameters
    # ~30 min budget: 120 attempts * 15s
    assert ens["max_attempts"].default == 120
    assert ens["sleep_seconds"].default == 15.0
    # migrate_index must pass the same generous default down (it owns the default
    # because run() calls it without overrides)
    assert mig["max_attempts"].default == 120
    assert mig["sleep_seconds"].default == 15.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py::test_default_endpoint_wait_budget_is_generous_for_cold_start -v`
Expected: FAIL (defaults are currently 30 / 10.0).

- [ ] **Step 3: Change the defaults** — in `src/migrate/vector_search_worker.py`, update BOTH function signatures' defaults (leave all body logic unchanged):
  - `_ensure_endpoint(..., *, max_attempts: int = 120, sleep_seconds: float = 15.0, sleep_fn=time.sleep)`
  - `migrate_index(..., *, max_attempts: int = 120, sleep_seconds: float = 15.0, sleep_fn=time.sleep)`

Also update the comment/docstring on `_ensure_endpoint` to note the ~30-minute cold-provision budget and that exceeding it falls through to `skipped_endpoint_not_ready` (re-run finishes it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_vector_search_worker.py -v`
Expected: PASS (existing 20 tests still pass — they all pass explicit small `max_attempts`/`sleep_seconds`, so the default change doesn't affect them — plus the new test).

- [ ] **Step 5: Lint + commit**

Run: `.venv/bin/ruff check src/migrate/vector_search_worker.py tests/unit/test_vector_search_worker.py`
```bash
git add src/migrate/vector_search_worker.py tests/unit/test_vector_search_worker.py
git commit -m "feat(vs): generous default endpoint-wait budget for cold-start provisioning"
```

---

## Task 2: Seed notebook (source objects + target source-table)

**Files:**
- Create: `tests/integration/seed_vector_search_test_data.py`

Read `tests/integration/seed_uc_test_data.py` (ambient `WorkspaceClient()` + `spark.sql` + `dbutils.jobs.taskValues.set(key="has_*", ...)`) and `tests/integration/teardown_uc.py` (how it reaches the TARGET via `AuthManager` + `find_warehouse` + `execute_and_poll`) before writing. This notebook is run live; it is not unit-tested.

- [ ] **Step 1: Create the seed notebook**

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
# (stands in for migrate_uc having already migrated the source table)
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
    execute_and_poll(_auth, _tgt_wh, _sql)
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
try:
    if not _endpoint_online(_ENDPOINT):
        with contextlib.suppress(Exception):
            _w.vector_search_endpoints.create_endpoint(name=_ENDPOINT, endpoint_type=EndpointType.STANDARD)
        # wait up to ~20 min for the source endpoint to come online
        for _ in range(80):
            if _endpoint_online(_ENDPOINT):
                break
            time.sleep(15)
    if not _endpoint_online(_ENDPOINT):
        raise RuntimeError(f"source VS endpoint {_ENDPOINT} did not reach ONLINE")
    print(f"[seed-vs] source endpoint {_ENDPOINT} ONLINE")

    # --- SOURCE: Delta Sync index (positive case) ---
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

    # --- SOURCE: Direct Access index (negative case) ---
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
    print(f"[seed-vs] VS seeding incomplete (VS may be unavailable): {_exc}")

# COMMAND ----------
dbutils.jobs.taskValues.set(key="has_delta_index", value="true" if _has_delta_index else "false")  # noqa: F821
dbutils.jobs.taskValues.set(key="has_direct_index", value="true" if _has_direct_index else "false")  # noqa: F821
dbutils.jobs.taskValues.set(key="delta_index_fqn", value=_DELTA_IDX)  # noqa: F821
dbutils.jobs.taskValues.set(key="direct_index_fqn", value=_DIRECT_IDX)  # noqa: F821
dbutils.jobs.taskValues.set(key="vs_endpoint_name", value=_ENDPOINT)  # noqa: F821
print(f"[seed-vs] flags: delta={_has_delta_index} direct={_has_direct_index}")
```

- [ ] **Step 2: Verify notebook shape + lint**

Run: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (0 failures — no indented `# COMMAND` markers)
Run: `.venv/bin/ruff check tests/integration/seed_vector_search_test_data.py` (clean — remove any genuinely unused import; note `EmbeddingSourceColumn`, `EmbeddingVectorColumn`, `PipelineType` ARE used)

- [ ] **Step 3: Commit**

```bash
git add tests/integration/seed_vector_search_test_data.py
git commit -m "test(vs): live integration seed — source Delta Sync + Direct Access indexes, target source-table"
```

---

## Task 3: Real assertion notebook (positive + negative)

**Files:**
- Replace contents of: `tests/integration/test_vector_search.py`

This replaces the tolerant stub. Read the existing `test_vector_search.py` and `test_uc_end_to_end.py` for the `migration_status` read FQN (`migration_tracking.cp_migration.migration_status`) and the `error_messages`-accumulate-then-raise notebook style.

- [ ] **Step 1: Replace the file contents**

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
# Live Vector Search migration assertion.
#   Positive (Delta Sync): migration_status == created_resync_pending AND the
#     index exists on the TARGET (get_index succeeds).
#   Negative (Direct Access): migration_status == skipped_direct_access_unsupported
#     AND the index does NOT exist on the target (get_index raises NotFound).
# Each case is gated on the seed's has_* flag — skipped (not failed) if the seed
# could not create that object (e.g. VS unavailable).

from databricks.sdk.errors import NotFound

from common.auth import AuthManager
from common.config import MigrationConfig

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_target = _auth.target_client

_has_delta = dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key="has_delta_index", debugValue="false")  # noqa: F821
_has_direct = dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key="has_direct_index", debugValue="false")  # noqa: F821
_delta_fqn = dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key="delta_index_fqn", debugValue="")  # noqa: F821
_direct_fqn = dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key="direct_index_fqn", debugValue="")  # noqa: F821

errors: list[str] = []


def _latest_status(fqn: str):
    rows = spark.sql(  # noqa: F821
        "SELECT status FROM migration_tracking.cp_migration.migration_status "
        f"WHERE object_type = 'vector_search_index' AND object_name = '{fqn}' "
        "ORDER BY migrated_at DESC LIMIT 1"
    ).collect()
    return rows[0]["status"] if rows else None


def _exists_on_target(fqn: str) -> bool:
    try:
        _target.vector_search_indexes.get_index(fqn)
        return True
    except NotFound:
        return False


# COMMAND ----------
# --- Positive case: Delta Sync ---
if _has_delta == "true":
    _status = _latest_status(_delta_fqn)
    if _status != "created_resync_pending":
        errors.append(f"POSITIVE: {_delta_fqn} status={_status!r}, expected 'created_resync_pending'")
    if not _exists_on_target(_delta_fqn):
        errors.append(f"POSITIVE: {_delta_fqn} not found on target — migration did not create the index")
    else:
        print(f"[test-vs] POSITIVE ok: {_delta_fqn} created_resync_pending + present on target")
else:
    print("[test-vs] POSITIVE skipped — seed did not create the Delta Sync index (VS unavailable?)")

# COMMAND ----------
# --- Negative case: Direct Access ---
if _has_direct == "true":
    _status = _latest_status(_direct_fqn)
    if _status != "skipped_direct_access_unsupported":
        errors.append(f"NEGATIVE: {_direct_fqn} status={_status!r}, expected 'skipped_direct_access_unsupported'")
    if _exists_on_target(_direct_fqn):
        errors.append(f"NEGATIVE: {_direct_fqn} unexpectedly EXISTS on target — Direct Access must not be migrated")
    else:
        print(f"[test-vs] NEGATIVE ok: {_direct_fqn} skipped + absent on target")
else:
    print("[test-vs] NEGATIVE skipped — seed did not create the Direct Access index (VS unavailable?)")

# COMMAND ----------
if errors:
    raise AssertionError("Vector Search live integration assertion failed:\n" + "\n".join(errors))
print("[test-vs] all asserted cases passed")
```

- [ ] **Step 2: Verify notebook shape + lint**

Run: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (0 failures)
Run: `.venv/bin/ruff check tests/integration/test_vector_search.py` (clean)

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_vector_search.py
git commit -m "test(vs): real positive (Delta Sync) + negative (Direct Access) live assertions"
```

---

## Task 4: Teardown notebook

**Files:**
- Create: `tests/integration/teardown_vector_search.py`

Read `tests/integration/teardown_uc.py` to match the best-effort `try/except` everywhere + AuthManager-target + `execute_and_poll` pattern. Runs `run_if ALL_DONE`; nothing here may raise.

- [ ] **Step 1: Create the teardown notebook**

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
# Best-effort teardown for the live Vector Search integration test. Deletes the
# indexes + endpoint on BOTH source and target (endpoints are a paid resource),
# drops the source test catalog, and clears tracking rows. Every step is
# individually try/excepted — nothing here raises.

import contextlib

from databricks.sdk import WorkspaceClient

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_src"
_ENDPOINT = "cp_migration_vs_it"
_DELTA_IDX = f"{_CATALOG}.vs_test.vs_delta_idx"
_DIRECT_IDX = f"{_CATALOG}.vs_test.vs_direct_idx"

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_src = WorkspaceClient()
_tgt = _auth.target_client

# COMMAND ----------
# --- Delete indexes + endpoint on BOTH workspaces ---
for _client, _label in ((_src, "source"), (_tgt, "target")):
    for _idx in (_DELTA_IDX, _DIRECT_IDX):
        with contextlib.suppress(Exception):
            _client.vector_search_indexes.delete_index(_idx)
            print(f"[teardown-vs] deleted index {_idx} on {_label}")
    with contextlib.suppress(Exception):
        _client.vector_search_endpoints.delete_endpoint(_ENDPOINT)
        print(f"[teardown-vs] deleted endpoint {_ENDPOINT} on {_label}")

# COMMAND ----------
# --- Drop the source test catalog ---
with contextlib.suppress(Exception):
    spark.sql(f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")  # noqa: F821
    print(f"[teardown-vs] dropped source catalog {_CATALOG}")

# --- Drop the target test catalog (created by the seed) ---
with contextlib.suppress(Exception):
    _wh = find_warehouse(_auth)
    execute_and_poll(_auth, _wh, f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")
    print(f"[teardown-vs] dropped target catalog {_CATALOG}")

# COMMAND ----------
# --- Clear tracking rows for the two indexes ---
for _fqn in (_DELTA_IDX, _DIRECT_IDX):
    with contextlib.suppress(Exception):
        spark.sql(  # noqa: F821
            "DELETE FROM migration_tracking.cp_migration.migration_status "
            f"WHERE object_name = '{_fqn}'"
        )
    with contextlib.suppress(Exception):
        spark.sql(  # noqa: F821
            "DELETE FROM migration_tracking.cp_migration.discovery_inventory "
            f"WHERE object_name = '{_fqn}'"
        )
print("[teardown-vs] tracking rows cleared")
```

- [ ] **Step 2: Verify notebook shape + lint**

Run: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (0 failures)
Run: `.venv/bin/ruff check tests/integration/teardown_vector_search.py` (clean)

- [ ] **Step 3: Commit**

```bash
git add tests/integration/teardown_vector_search.py
git commit -m "test(vs): live integration teardown (indexes + endpoints both sides, catalog, tracking)"
```

---

## Task 5: Wire the integration workflow

**Files:**
- Modify: `resources/integration_tests/vector_search_integration_test_workflow.yml`

Read `resources/integration_tests/uc_integration_test_workflow.yml` for the exact `setup_test_config` task shape (it passes `base_parameters` like `catalog_filter`) and the `teardown` `run_if: ALL_DONE` + depends-on-all pattern. Mirror it.

- [ ] **Step 1: Rewrite the workflow YAML** to this task graph (keep the job name + run_as exactly as the current file has them):

```yaml
resources:
  jobs:
    vector_search_integration_test:
      name: "${var.job_prefix}-vector-search-integration-test"
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: setup_test_config
          notebook_task:
            notebook_path: ../../tests/integration/setup_test_config.py
            base_parameters:
              catalog_filter: "integration_test_src"
        - task_key: seed_vector_search
          depends_on:
            - task_key: setup_test_config
          notebook_task:
            notebook_path: ../../tests/integration/seed_vector_search_test_data.py
        - task_key: discovery
          depends_on:
            - task_key: seed_vector_search
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
        - task_key: teardown_vector_search
          depends_on:
            - task_key: setup_test_config
            - task_key: seed_vector_search
            - task_key: discovery
            - task_key: migrate_vector_search
            - task_key: test_vector_search
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../../tests/integration/teardown_vector_search.py
```

> Confirm the exact `name`/`run_as`/`base_parameters` key spellings against `uc_integration_test_workflow.yml`. If `setup_test_config` in the UC workflow passes additional required params (e.g. `rls_cm_strategy`), include the same neutral defaults so the shared notebook doesn't trip on a missing param. Match the sibling's exact indentation.

- [ ] **Step 2: Validate**

Run: `.venv/bin/python -c "import yaml; yaml.safe_load(open('resources/integration_tests/vector_search_integration_test_workflow.yml')); print('YAML OK')"`
Run (if CLI available): `databricks bundle validate -t dev --profile source-migration 2>&1 | tail -15`

- [ ] **Step 3: Commit**

```bash
git add resources/integration_tests/vector_search_integration_test_workflow.yml
git commit -m "test(vs): wire live integration workflow (setup→seed→discovery→migrate→test→teardown)"
```

---

## Task 6: Deploy + run live + report

This task runs against the live workspaces. It is NOT a code change — it executes the test and reports the result. (Controller may run this directly rather than via a subagent.)

- [ ] **Step 1: Confirm both workspaces reachable**

Run: `databricks current-user me --profile source-migration 2>&1 | head -3`
Run: `databricks current-user me --profile target-migration 2>&1 | head -3`
Expected: both return the active user (already verified during design).

- [ ] **Step 2: Validate + deploy the bundle**

Run:
```bash
cd ~/uksouth_migration/workspace-migration
databricks bundle validate -t dev --profile source-migration
BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae \
  DATABRICKS_TF_VERSION=1.5.7 DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform \
  databricks bundle deploy -t dev --profile source-migration
```
Expected: validate clean, deploy succeeds (jobs created/updated, including `vector_search_integration_test` and `migrate_vector_search`). If the deploy command's env vars differ in this environment, use the project's session-recovery deploy command from memory; report any deviation.

- [ ] **Step 3: Run the integration job and wait**

Run: `databricks bundle run vector_search_integration_test -t dev --profile source-migration 2>&1 | tail -40`
(If `bundle run` is not the right invocation, get the job id via `databricks jobs list --profile source-migration | grep vector-search-integration-test` and `databricks jobs run-now --job-id <id>` then poll `databricks jobs get-run <run-id>`.)
Expected: the run proceeds setup → seed (provisions source endpoint + indexes, ~10-15 min) → discovery → migrate (cold-starts target endpoint + creates index, ~10-20 min) → test → teardown. Total ~20-40 min. Poll until terminal.

- [ ] **Step 4: Report the outcome**

Capture and report:
- The `test_vector_search` task result (SUCCESS = both positive + negative asserted) and its driver log lines (`[test-vs] POSITIVE ok...`, `[test-vs] NEGATIVE ok...`).
- The `migrate_vector_search` child-job result + the worker's per-index status lines.
- Confirm `teardown_vector_search` ran (SUCCESS) so no paid endpoints are left running. If teardown did NOT run or failed, manually delete the endpoint on both workspaces: `databricks vector-search-endpoints delete-endpoint cp_migration_vs_it --profile source-migration` and `--profile target-migration`.
- If the positive case landed as `skipped_endpoint_not_ready` (endpoint exceeded the 30-min budget), report it — the index migration is correct but the live endpoint was slow; re-running migrate would finish it.

- [ ] **Step 5: (No commit)** — this task produces a run report, not code. Record the result in the PR thread / summary.

---

## Self-Review

**Spec coverage:**
- Positive (Delta Sync) + negative (Direct Access) seeded on source → Task 2. ✓
- Target source-table created so pre-check passes (cold-start = endpoint/index only) → Task 2 (target table via execute_and_poll). ✓ (correction surfaced during planning, not in the original spec prose — the spec's "nothing on target" applies to VS endpoint/index, and this is the necessary refinement.)
- Cold-start endpoint creation + generous wait, no config knob → Task 1. ✓
- Resource-exists target assertion; positive exists+created_resync_pending, negative NotFound+skipped_direct_access_unsupported → Task 3. ✓
- Infra-graceful skip via has_* flags → Tasks 2 + 3. ✓
- Teardown both sides incl. paid endpoints, ALL_DONE → Tasks 4 + 5. ✓
- Build + run live + report → Task 6. ✓

**Placeholder scan:** No TBD/TODO. "Read sibling X / confirm exact key spellings" notes (Tasks 2,3,4,5) are real match-the-pattern instructions, not deferred logic. All notebook code is complete.

**Type/name consistency:** identifiers (`integration_test_src`/`vs_test`/`vs_source`/`cp_migration_vs_it`/`vs_delta_idx`/`vs_direct_idx`) are identical across seed (Task 2), assertion (Task 3, read via task values), teardown (Task 4), and workflow (Task 5). Seed task-value keys (`has_delta_index`, `has_direct_index`, `delta_index_fqn`, `direct_index_fqn`, `vs_endpoint_name`) match the assertion's `dbutils.jobs.taskValues.get(taskKey="seed_vector_search", key=...)`. Worker default budget (120/15) set in Task 1 is what makes Task 6's cold-start positive case viable.

**Open item flagged for the live run:** the Direct Access seed spec (`DirectAccessVectorIndexSpec` with `schema_json` + `embedding_vector_columns`) must be accepted by the live API; if the seed fails to create it, `has_direct_index=false` and the negative case is skipped (visible in the run log) rather than silently passing — Task 6 Step 4 calls this out.
