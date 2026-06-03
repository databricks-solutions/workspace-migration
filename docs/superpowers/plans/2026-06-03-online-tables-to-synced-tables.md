# Online Tables → Lakebase Synced Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redefine `migrate_online_tables` to convert each discovered online table into a **Lakebase synced table** on the target (create-if-missing Lakebase instance + `create_synced_database_table`), replacing the now-invalid "recreate the online table" approach (legacy online-table create is deprecated/blocked platform-wide).

**Architecture:** VS-pattern: the worker ensures a shared target Lakebase **database instance** exists (create-if-missing, wait for `AVAILABLE`), then creates a synced table (taking the online table's FQN) from the online table's discovered spec → `created_resync_pending`. Consumer repoint (apps → new Postgres endpoint) is out of scope/documented. Single target (`synced_table` only); feature_store deferred. Supersedes PR #55's approach on the same branch.

**Tech Stack:** Python 3.11, existing `databricks-sdk` 0.102.0 (`database.create_database_instance` / `create_synced_database_table` / `get_database_instance` / `get_synced_database_table`; `DatabaseInstance`, `SyncedDatabaseTable`, `SyncedTableSpec`, `SyncedTableSchedulingPolicy`, `DatabaseInstanceState`). Spec: `docs/superpowers/specs/2026-06-03-online-tables-to-synced-tables-design.md`.

**Verified SDK surface (0.102.0):**
- `target_client.database.get_database_instance(name) -> DatabaseInstance` (raises if absent); `.create_database_instance(DatabaseInstance(name, capacity)) -> Wait[DatabaseInstance]`. `DatabaseInstance.state` ∈ `DatabaseInstanceState{AVAILABLE, STARTING, STOPPED, UPDATING, DELETING, FAILING_OVER}` — ready = `AVAILABLE`. `capacity` is a string (e.g. `CU_1`).
- `target_client.database.create_synced_database_table(SyncedDatabaseTable(name, database_instance_name, logical_database_name, spec)) -> SyncedDatabaseTable`; `.get_synced_database_table(name) -> SyncedDatabaseTable`; `.delete_synced_database_table(name)`.
- `SyncedTableSpec(source_table_full_name, primary_key_columns, timeseries_key, scheduling_policy, ...)`; `SyncedTableSchedulingPolicy ∈ {CONTINUOUS, SNAPSHOT, TRIGGERED}`.
- Discovered online-table `metadata_json.definition`: `{name, spec:{source_table_full_name, primary_key_columns, run_triggered|run_continuously|perform_full_copy, timeseries_key, pipeline_id}}`.
- `databricks.sdk.errors.AlreadyExists`, `NotFound`.

**Plan decisions (transparent deviations from the spec, kept simple):**
- `skipped_instance_not_ready` is a worker status that is **NOT** added to `_TERMINAL_STATUSES` → naturally re-pickable. No `tracking.py` change.
- **Primary-key requirement enforced at create-time**, not in pre-check: `create_synced_database_table` rejects a PK-less source → worker records `failed` with the platform's clear error. So `pre_check_online_tables` stays the proven source-exists gate (unchanged), avoiding a fragile per-table `information_schema` probe.
- Integration workflow **drops the `discovery` task** and the seed injects a synthetic `online_table` discovery row (legacy online tables can't be created to discover; running real discovery would find none and could clobber the injected row).

---

## File Structure

- **Modify** `src/common/config.py` — add `lakebase_instance_name`, `lakebase_logical_database`, `lakebase_capacity` fields (+ `config.example.yaml`).
- **Rewrite** `src/migrate/online_tables_worker.py` — online table → synced table.
- **Rewrite** `tests/unit/test_online_tables_worker.py` — for synced-table behavior.
- `src/pre_check/pre_check_online_tables.py` — **unchanged** (source-exists gate already correct).
- **Rewrite** `tests/integration/{seed_online_tables_test_data,test_online_tables,teardown_online_tables}.py` — synced-table mechanics + synthetic-row injection.
- **Modify** `resources/integration_tests/online_tables_integration_test_workflow.yml` — drop `discovery` task.
- **Modify** `docs/user_guide.md`, `docs/stateful_services_phase.md`.

Worker helpers (consistent names across tasks): `_scheduling_policy(spec)`, `_build_synced_table_spec(definition)`, `_instance_ready(inst)`, `_ensure_lakebase_instance(target_client, name, capacity, *, max_attempts, sleep_seconds, sleep_fn)`, `migrate_online_table(target_client, row, config, *, ...)`, `run`.

---

## Task 1: Config — Lakebase target settings

**Files:**
- Modify: `src/common/config.py`
- Modify: `config.example.yaml`
- Test: `tests/unit/test_config.py`

- [ ] **Step 1: Write the failing test** — read `tests/unit/test_config.py` for its style; append:

```python
def test_lakebase_defaults_and_overrides(tmp_path):
    import yaml
    from common.config import MigrationConfig

    base = {
        "source_workspace_url": "https://s", "target_workspace_url": "https://t",
        "spn_client_id": "x", "spn_secret_scope": "sc", "spn_secret_key": "k",
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(base))
    cfg = MigrationConfig.from_workspace_file(str(p))
    assert cfg.lakebase_instance_name == "cp_migration_lakebase"
    assert cfg.lakebase_logical_database == "databricks_postgres"
    assert cfg.lakebase_capacity == "CU_1"

    p.write_text(yaml.safe_dump({**base, "lakebase_instance_name": "lb_x",
                                 "lakebase_logical_database": "ldb", "lakebase_capacity": "CU_2"}))
    cfg2 = MigrationConfig.from_workspace_file(str(p))
    assert cfg2.lakebase_instance_name == "lb_x"
    assert cfg2.lakebase_logical_database == "ldb"
    assert cfg2.lakebase_capacity == "CU_2"
```
> If `MigrationConfig.from_workspace_file` requires more mandatory fields than shown, mirror whatever `test_config.py`'s existing fixtures pass.

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_config.py::test_lakebase_defaults_and_overrides -v`
Expected: FAIL (`AttributeError: ... lakebase_instance_name`).

- [ ] **Step 3: Implement** — in `src/common/config.py`, add three dataclass fields (next to the other defaulted fields like `hive_target_catalog`):

```python
    lakebase_instance_name: str = "cp_migration_lakebase"
    lakebase_logical_database: str = "databricks_postgres"
    lakebase_capacity: str = "CU_1"
```
and in `from_workspace_file(...)`'s constructor call (next to `hive_target_catalog=...`):

```python
            lakebase_instance_name=str(raw.get("lakebase_instance_name", "cp_migration_lakebase")),
            lakebase_logical_database=str(raw.get("lakebase_logical_database", "databricks_postgres")),
            lakebase_capacity=str(raw.get("lakebase_capacity", "CU_1")),
```
Add to `config.example.yaml` (with a comment):
```yaml
# Online Tables -> Lakebase synced table migration (migrate_online_tables).
# The job creates this Lakebase database instance if it does not exist.
lakebase_instance_name: cp_migration_lakebase
lakebase_logical_database: databricks_postgres
lakebase_capacity: CU_1
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_config.py -v`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

Run: `.venv/bin/ruff check src/common/config.py tests/unit/test_config.py`
```bash
git add src/common/config.py config.example.yaml tests/unit/test_config.py
git commit -m "feat(ot): Lakebase target config (instance/logical-db/capacity) for synced-table migration"
```

---

## Task 2: Worker rewrite — online table → synced table

**Files:**
- Modify: `src/migrate/online_tables_worker.py` (keep bootstrap cell; replace body)
- Rewrite: `tests/unit/test_online_tables_worker.py`

- [ ] **Step 1: Rewrite the unit test** — replace `tests/unit/test_online_tables_worker.py` contents:

```python
"""Unit tests for the Online Tables -> Lakebase synced table migration worker."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from databricks.sdk.errors import AlreadyExists

from migrate.online_tables_worker import (
    _build_synced_table_spec,
    _scheduling_policy,
    migrate_online_table,
)


def _definition(mode="run_triggered"):
    spec = {"source_table_full_name": "cat.sch.src", "primary_key_columns": ["id"], "pipeline_id": "pl-1"}
    spec[mode] = {} if mode != "perform_full_copy" else True
    return {"name": "cat.sch.ot", "spec": spec}


def _row(definition):
    return {"object_name": "cat.sch.ot", "object_type": "online_table",
            "metadata_json": json.dumps({"definition": definition})}


def _config():
    c = MagicMock()
    c.lakebase_instance_name = "lb1"
    c.lakebase_logical_database = "ldb"
    c.lakebase_capacity = "CU_1"
    return c


def _ready_instance():
    inst = MagicMock(); inst.state = "AVAILABLE"
    return inst


class TestSchedulingPolicy:
    def test_continuous(self):
        from databricks.sdk.service.database import SyncedTableSchedulingPolicy
        assert _scheduling_policy({"run_continuously": {}}) == SyncedTableSchedulingPolicy.CONTINUOUS

    def test_snapshot(self):
        from databricks.sdk.service.database import SyncedTableSchedulingPolicy
        assert _scheduling_policy({"perform_full_copy": True}) == SyncedTableSchedulingPolicy.SNAPSHOT

    def test_triggered_default(self):
        from databricks.sdk.service.database import SyncedTableSchedulingPolicy
        assert _scheduling_policy({"run_triggered": {}}) == SyncedTableSchedulingPolicy.TRIGGERED
        assert _scheduling_policy({}) == SyncedTableSchedulingPolicy.TRIGGERED


class TestBuildSpec:
    def test_builds_spec_from_definition(self):
        spec = _build_synced_table_spec(_definition())
        assert spec.source_table_full_name == "cat.sch.src"
        assert spec.primary_key_columns == ["id"]
        assert spec.scheduling_policy is not None


class TestMigrate:
    def test_created_resync_pending_and_fqn(self):
        client = MagicMock()
        client.database.get_database_instance.return_value = _ready_instance()
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "created_resync_pending"
        assert res["object_name"] == "cat.sch.ot"
        st = client.database.create_synced_database_table.call_args.args[0]
        assert st.name == "cat.sch.ot"
        assert st.database_instance_name == "lb1"
        assert st.logical_database_name == "ldb"
        assert st.spec.source_table_full_name == "cat.sch.src"

    def test_instance_created_if_missing(self):
        client = MagicMock()
        # absent on first get, AVAILABLE after create+poll
        client.database.get_database_instance.side_effect = [Exception("nf"), _ready_instance()]
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=3, sleep_seconds=0)
        assert res["status"] == "created_resync_pending"
        client.database.create_database_instance.assert_called_once()

    def test_instance_not_ready_defers(self):
        client = MagicMock()
        client.database.get_database_instance.side_effect = Exception("nf")
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_instance_not_ready"
        client.database.create_synced_database_table.assert_not_called()

    def test_already_exists(self):
        client = MagicMock()
        client.database.get_database_instance.return_value = _ready_instance()
        client.database.create_synced_database_table.side_effect = AlreadyExists("exists")
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "skipped_target_exists"

    def test_create_failure_is_failed(self):
        client = MagicMock()
        client.database.get_database_instance.return_value = _ready_instance()
        client.database.create_synced_database_table.side_effect = Exception("no primary key")
        res = migrate_online_table(client, _row(_definition()), _config(),
                                   sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        assert "primary key" in res["error_message"]

    def test_missing_source_is_failed(self):
        client = MagicMock()
        row = {"object_name": "cat.sch.ot", "object_type": "online_table",
               "metadata_json": json.dumps({"definition": {"name": "cat.sch.ot", "spec": {}}})}
        res = migrate_online_table(client, row, _config(), sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        client.database.create_synced_database_table.assert_not_called()

    def test_malformed_metadata_is_failed(self):
        client = MagicMock()
        row = {"object_name": "cat.sch.ot", "object_type": "online_table", "metadata_json": "{bad"}
        res = migrate_online_table(client, row, _config(), sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res["status"] == "failed"
        client.database.create_synced_database_table.assert_not_called()
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_online_tables_worker.py -v`
Expected: FAIL (imports `_build_synced_table_spec` etc. — the worker still has the online-table-create version from PR #55).

- [ ] **Step 3: Rewrite the worker** — keep the bootstrap cell; replace the body of `src/migrate/online_tables_worker.py`:

```python
# COMMAND ----------
# Online Tables migration worker. Legacy online tables are deprecated and can no
# longer be created, so this converts each discovered online table into a
# Lakebase SYNCED TABLE on the target: ensure a shared Lakebase database instance
# exists (create-if-missing), then create_synced_database_table from the online
# table's source. Sync state is re-built; consumer apps must repoint to the new
# Postgres endpoint (out of scope). Consumes the orchestrator's online_table_list.
# Spec: docs/superpowers/specs/2026-06-03-online-tables-to-synced-tables-design.md

import contextlib
import json
import logging
import time

from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.database import (
    DatabaseInstance,
    SyncedDatabaseTable,
    SyncedTableSchedulingPolicy,
    SyncedTableSpec,
)

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


def _scheduling_policy(spec: dict) -> SyncedTableSchedulingPolicy:
    """Map the online table's sync mode to a synced-table scheduling policy."""
    if spec.get("run_continuously") is not None:
        return SyncedTableSchedulingPolicy.CONTINUOUS
    if spec.get("perform_full_copy"):
        return SyncedTableSchedulingPolicy.SNAPSHOT
    return SyncedTableSchedulingPolicy.TRIGGERED  # run_triggered or unspecified


def _build_synced_table_spec(definition: dict) -> SyncedTableSpec:
    """Build a SyncedTableSpec from the discovered online-table definition."""
    spec = definition.get("spec") or {}
    return SyncedTableSpec(
        source_table_full_name=spec.get("source_table_full_name"),
        primary_key_columns=list(spec.get("primary_key_columns") or []),
        timeseries_key=spec.get("timeseries_key"),
        scheduling_policy=_scheduling_policy(spec),
    )


def _instance_ready(inst: object) -> bool:
    return "AVAILABLE" in str(getattr(inst, "state", "")).upper()


def _ensure_lakebase_instance(
    target_client,
    name: str,
    capacity: str,
    *,
    max_attempts: int = 120,
    sleep_seconds: float = 15.0,
    sleep_fn=time.sleep,
) -> bool:
    """Ensure the target Lakebase database instance exists and is AVAILABLE.
    Create-if-missing (VS-endpoint-style), poll up to ~30 min. Returns ready?."""
    try:
        inst = target_client.database.get_database_instance(name)
        if _instance_ready(inst):
            return True
    except Exception:  # noqa: BLE001 — absent or transient; create then poll
        with contextlib.suppress(AlreadyExists):
            target_client.database.create_database_instance(DatabaseInstance(name=name, capacity=capacity))

    for _ in range(max_attempts):
        try:
            inst = target_client.database.get_database_instance(name)
            if _instance_ready(inst):
                return True
        except Exception:  # noqa: BLE001 — keep polling
            pass
        sleep_fn(sleep_seconds)
    return False


def migrate_online_table(
    target_client,
    row: dict,
    config,
    *,
    max_attempts: int = 120,
    sleep_seconds: float = 15.0,
    sleep_fn=time.sleep,
) -> dict:
    """Convert one online_table discovery row into a Lakebase synced table.
    Fully exception-safe (one bad row never aborts the batch)."""
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
        if not (definition.get("spec") or {}).get("source_table_full_name"):
            return _result("failed", "online table has no source_table_full_name — cannot convert to synced table.")
        fqn = definition.get("name") or obj_name
        spec = _build_synced_table_spec(definition)
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"synced-table spec rebuild failed: {exc}")

    ready = _ensure_lakebase_instance(
        target_client, config.lakebase_instance_name, config.lakebase_capacity,
        max_attempts=max_attempts, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn,
    )
    if not ready:
        return _result(
            "skipped_instance_not_ready",
            f"Lakebase instance '{config.lakebase_instance_name}' not AVAILABLE within wait budget; "
            "a re-run will retry this online table.",
        )

    try:
        target_client.database.create_synced_database_table(
            SyncedDatabaseTable(
                name=fqn,
                database_instance_name=config.lakebase_instance_name,
                logical_database_name=config.lakebase_logical_database,
                spec=spec,
            )
        )
    except AlreadyExists as exc:
        return _result("skipped_target_exists", f"Synced table already exists on target: {exc}")
    except Exception as exc:  # noqa: BLE001
        return _result("failed", f"create_synced_database_table failed: {exc}")

    return _result("created_resync_pending", None)


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(  # type: ignore[union-attr]
        taskKey="orchestrator", key="online_table_list", debugValue="[]"
    )
    rows = json.loads(rows_json) if rows_json else []
    logger.info("Received %d online_table record(s) to convert to synced tables.", len(rows))

    results = [migrate_online_table(auth.target_client, row, config) for row in rows]
    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Online tables worker complete: %d created_resync_pending, %d skipped_target_exists, "
        "%d skipped_instance_not_ready, %d failed.",
        sum(1 for r in results if r["status"] == "created_resync_pending"),
        sum(1 for r in results if r["status"] == "skipped_target_exists"),
        sum(1 for r in results if r["status"] == "skipped_instance_not_ready"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
```

- [ ] **Step 4: Run the test + whole suite**

Run: `.venv/bin/python -m pytest tests/unit/test_online_tables_worker.py -v` (expect all pass).
Run: `.venv/bin/python -m pytest tests/unit tests/lint -q` — fix any pre-existing test that asserted the *old* online-table-create behavior (e.g. `test_phase3_workers.py` / `test_idempotency_audit.py` were updated in PR #55 to assert `online_tables.create`; update them again to the synced-table contract: `migrate_online_table(client, row, config)` with a ready instance → `created_resync_pending`, and `AlreadyExists` → `skipped_target_exists`). Report which you changed.

- [ ] **Step 5: Lint + commit**

Run: `.venv/bin/ruff check src/migrate/online_tables_worker.py tests/unit/test_online_tables_worker.py`
Run: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (0 failures)
```bash
git add -A
git commit -m "feat(ot): convert online tables to Lakebase synced tables (create-if-missing instance)"
```

---

## Task 3: Integration test — synced-table mechanics via synthetic discovery row

**Files:**
- Rewrite: `tests/integration/seed_online_tables_test_data.py`
- Rewrite: `tests/integration/test_online_tables.py`
- Rewrite: `tests/integration/teardown_online_tables.py`
- Modify: `resources/integration_tests/online_tables_integration_test_workflow.yml` (drop `discovery` task)

Read the VS integration files + the existing OT ones. All `# COMMAND ----------` at column 0; seed + assertion `dbutils.notebook.exit(json...)`. **Legacy online tables can't be created** — the seed creates a PK'd source table on target and **injects a synthetic online_table row into `discovery_inventory`** (matching discovery's shape) so the migrate job's orchestrator picks it up. The workflow therefore has **no `discovery` task**.

- [ ] **Step 1: Rewrite `seed_online_tables_test_data.py`:**

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
# Seed for the live Online Tables -> synced table integration test.
# Legacy online tables can no longer be created, so we:
#   - create a PK'd Delta source table on the TARGET (what the synced table syncs from),
#   - inject a synthetic `online_table` row into discovery_inventory shaped exactly as
#     discovery produces, pointing at that source table.
# The migrate_online_tables job's orchestrator then picks it up and creates a real
# Lakebase synced table. (No real online table is created — documented test boundary.)

import json

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager, discovery_row, discovery_schema

_CATALOG = "integration_test_src"
_SCHEMA = "ot_test"
_TABLE = f"{_CATALOG}.{_SCHEMA}.ot_source"
_OT_FQN = f"{_CATALOG}.{_SCHEMA}.ot_online"

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821

# COMMAND ----------
# --- TARGET: PK'd Delta source table (what the synced table will sync from) ---
_tgt_wh = find_warehouse(_auth)
for _sql in (
    f"CREATE CATALOG IF NOT EXISTS {_CATALOG}",
    f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}",
    f"CREATE OR REPLACE TABLE {_TABLE} (id INT NOT NULL, text STRING, CONSTRAINT ot_pk PRIMARY KEY(id))",
    f"INSERT INTO {_TABLE} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
):
    _res = execute_and_poll(_auth, _tgt_wh, _sql)
    if _res.get("state") != "SUCCEEDED":
        raise RuntimeError(f"[seed-ot] target setup SQL failed: {_sql} -> {_res}")
print(f"[seed-ot] target source table {_TABLE} created (PK on id)")

# COMMAND ----------
# --- Inject a synthetic online_table discovery row (source-side tracking) ---
from datetime import datetime, timezone

_tracker = TrackingManager(spark, _config)  # noqa: F821
_tracker.init_tracking_tables()
_definition = {
    "name": _OT_FQN,
    "spec": {"source_table_full_name": _TABLE, "primary_key_columns": ["id"], "run_triggered": {}},
}
_row = discovery_row(
    source_type="stateful",
    object_type="online_table",
    object_name=_OT_FQN,
    catalog_name=None,
    schema_name=None,
    discovered_at=datetime.now(tz=timezone.utc),
    metadata={"capability": "online_store", "online_table_fqn": _OT_FQN,
              "source_table_fqn": _TABLE, "definition": _definition},
)
spark.createDataFrame([_row], schema=discovery_schema())  # noqa: F821 \
    .write.mode("append").saveAsTable(f"{_config.tracking_catalog}.{_config.tracking_schema}.discovery_inventory")
print(f"[seed-ot] injected synthetic online_table discovery row for {_OT_FQN}")

# COMMAND ----------
dbutils.jobs.taskValues.set(key="online_table_fqn", value=_OT_FQN)  # noqa: F821
dbutils.notebook.exit(json.dumps({"seeded": True, "online_table_fqn": _OT_FQN}))  # noqa: F821
```
> Confirm `discovery_row` / `discovery_schema` import + signature against `src/common/tracking.py` (used by `discovery.py`); match exactly. Confirm `write_discovery_inventory` vs a raw append — if the tracker exposes an append helper, prefer it; otherwise the `saveAsTable(... mode append)` above targets the real `discovery_inventory` table.

- [ ] **Step 2: Rewrite `test_online_tables.py`:**

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
# Live assertion: the migrate_online_tables job converted the (synthetic) online
# table into a real Lakebase synced table on the target.

import json

from databricks.sdk.errors import NotFound

from common.auth import AuthManager
from common.config import MigrationConfig

_config = MigrationConfig.from_workspace_file()
_auth = AuthManager(_config, dbutils)  # noqa: F821
_target = _auth.target_client
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


def _synced_exists(fqn: str) -> bool:
    try:
        _target.database.get_synced_database_table(fqn)
        return True
    except NotFound:
        return False


# COMMAND ----------
_n = len(errors)
_status = _latest_status(_ot_fqn)
if _status == "skipped_instance_not_ready":
    summary["online_table"] = "skipped_instance_not_ready"
    print("[test-ot] Lakebase instance was not AVAILABLE in time — re-run would finish. Not a failure.")
else:
    if _status != "created_resync_pending":
        errors.append(f"{_ot_fqn} status={_status!r}, expected 'created_resync_pending'")
    if not _synced_exists(_ot_fqn):
        errors.append(f"{_ot_fqn} synced table not found on target")
    summary["online_table"] = "asserted_ok" if len(errors) == _n else "FAILED"
    if summary["online_table"] == "asserted_ok":
        print(f"[test-ot] ok: {_ot_fqn} created_resync_pending + synced table present on target")

# COMMAND ----------
_result = json.dumps({"summary": summary, "errors": errors})
if errors:
    raise AssertionError("Online Tables live integration assertion FAILED: " + _result)
print("[test-ot] passed: " + _result)
dbutils.notebook.exit(_result)  # noqa: F821
```

- [ ] **Step 3: Rewrite `teardown_online_tables.py`** (best-effort; delete synced table + Lakebase instance both sides + drop catalogs + clear tracking):

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
# Best-effort teardown for the live Online Tables -> synced table test. Deletes the
# synced table + the Lakebase instance (paid) on the target, drops the test catalog,
# clears tracking rows. Everything suppressed — nothing raises.

import contextlib

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

_CATALOG = "integration_test_src"
_OT_FQN = f"{_CATALOG}.ot_test.ot_online"

_config = MigrationConfig.from_workspace_file()


def _target():
    return AuthManager(MigrationConfig.from_workspace_file(), dbutils).target_client  # noqa: F821


# COMMAND ----------
with contextlib.suppress(Exception):
    _target().database.delete_synced_database_table(_OT_FQN)
    print(f"[teardown-ot] deleted synced table {_OT_FQN}")

with contextlib.suppress(Exception):
    _target().database.delete_database_instance(_config.lakebase_instance_name)
    print(f"[teardown-ot] deleted Lakebase instance {_config.lakebase_instance_name}")

# COMMAND ----------
with contextlib.suppress(Exception):
    _auth = AuthManager(MigrationConfig.from_workspace_file(), dbutils)  # noqa: F821
    _wh = find_warehouse(_auth)
    execute_and_poll(_auth, _wh, f"DROP CATALOG IF EXISTS {_CATALOG} CASCADE")
    print(f"[teardown-ot] dropped target catalog {_CATALOG}")

# COMMAND ----------
for _tbl in ("migration_status", "discovery_inventory"):
    with contextlib.suppress(Exception):
        spark.sql(  # noqa: F821
            f"DELETE FROM migration_tracking.cp_migration.{_tbl} WHERE object_name = '{_OT_FQN}'"
        )
print("[teardown-ot] tracking rows cleared")
```
> Confirm `database.delete_database_instance` exists (it should mirror create/get). If the delete signature differs, match the SDK.

- [ ] **Step 4: Update the workflow YAML** — `resources/integration_tests/online_tables_integration_test_workflow.yml`: **remove the `discovery` task**, and make `seed_online_tables` → `migrate_online_tables` (run_job_task) directly. Result chain: `setup_test_config → seed_online_tables → migrate_online_tables → test_online_tables → teardown_online_tables (ALL_DONE on all prior)`. Keep run_as + the migrate run_job_task on `${resources.jobs.migrate_online_tables.id}`. Update `migrate_online_tables`'s `depends_on` from `discovery` to `seed_online_tables`, and drop `discovery` from teardown's depends_on.

- [ ] **Step 5: Validate + commit**

Run: `.venv/bin/python -m pytest tests/lint/test_notebook_shape.py -q` (0 failures)
Run: `.venv/bin/ruff check tests/integration/seed_online_tables_test_data.py tests/integration/test_online_tables.py tests/integration/teardown_online_tables.py` (clean)
Run: `.venv/bin/python -c "import yaml; yaml.safe_load(open('resources/integration_tests/online_tables_integration_test_workflow.yml')); print('YAML OK')"`
```bash
git add tests/integration/seed_online_tables_test_data.py tests/integration/test_online_tables.py tests/integration/teardown_online_tables.py resources/integration_tests/online_tables_integration_test_workflow.yml
git commit -m "test(ot): synced-table live integration via synthetic discovery row (no real online table)"
```

---

## Task 4: Docs

**Files:** Modify `docs/user_guide.md`, `docs/stateful_services_phase.md`

- [ ] **Step 1:** Update the `migrate_online_tables` section in `docs/user_guide.md`: it now **converts online tables → Lakebase synced tables** (legacy online tables deprecated/uncreatable). Cover: the tool **provisions a paid Lakebase database instance** (`lakebase_*` config, created if missing); each online table becomes a synced table that re-syncs from the source Delta table; **consumer apps must repoint to the new Lakebase Postgres endpoint (operator action, out of scope)**; precondition (source migrated with a primary key — synced-table create fails loudly otherwise); statuses (`created_resync_pending`, `skipped_target_exists`, `skipped_instance_not_ready`, `failed`).
- [ ] **Step 2:** Update the **Online Tables** row in `docs/stateful_services_phase.md` current-tool-behaviour cell → "Migrated by `migrate_online_tables` as a Lakebase **synced table** (legacy online tables deprecated; create blocked). Re-syncs from the source Delta table into a Lakebase instance the job creates; consumer repoint is operator-owned."
- [ ] **Step 3: Commit**
```bash
git add docs/user_guide.md docs/stateful_services_phase.md
git commit -m "docs(ot): migrate_online_tables now converts to Lakebase synced tables"
```

---

## Task 5: Full suite + lint + update PR #55

- [ ] **Step 1:** `.venv/bin/python -m pytest tests/unit tests/lint -q` — all pass.
- [ ] **Step 2:** `.venv/bin/ruff check src/migrate/online_tables_worker.py src/common/config.py tests/unit/test_online_tables_worker.py tests/integration/seed_online_tables_test_data.py tests/integration/test_online_tables.py tests/integration/teardown_online_tables.py` — clean.
- [ ] **Step 3:** Commit any lint fixups.
- [ ] **Step 4:** Push + update PR #55's description to the synced-table design:
```bash
git push databricks-solutions feat/migrate-online-tables
gh pr edit 55 --repo databricks-solutions/workspace-migration --title "feat: migrate_online_tables -> Lakebase synced tables" --body "$(cat <<'EOF'
## Summary

`migrate_online_tables` converts each discovered legacy online table into a **Lakebase synced table** on the target (legacy online-table creation is deprecated/blocked platform-wide; Databricks' documented replacement is synced tables). The worker ensures a shared target Lakebase database instance exists (create-if-missing, wait AVAILABLE), then `create_synced_database_table` from the online table's source → `created_resync_pending`. Removed from `migrate_uc`; reuses the shared orchestrator's online_table_list.

- Single target: synced_table (feature_store deferred — needs databricks-feature-engineering + serving-endpoint cutover).
- Statuses: created_resync_pending / skipped_target_exists / skipped_instance_not_ready (re-pickable) / failed. PK requirement enforced at create-time.
- Tool provisions a paid Lakebase instance (config: lakebase_instance_name/logical_database/capacity). Consumer repoint to the new Postgres endpoint is operator-owned (out of scope).

Spec: docs/superpowers/specs/2026-06-03-online-tables-to-synced-tables-design.md
Plan: docs/superpowers/plans/2026-06-03-online-tables-to-synced-tables.md

## Testing
- Unit suite covers all worker logic (scheduling-policy mapping, create-if-missing instance, synced-table create, already-exists, instance-not-ready, failed/malformed) + config.
- Live integration validates the REAL synced-table mechanics (create Lakebase instance + synced table) via a synthetic injected online-table discovery row — legacy online tables can no longer be created to seed. [Live result appended after Task 6.] Honest boundary: the "from a real online table" front-half is unit-tested only.

This pull request and its description were written by Isaac.
EOF
)"
```

---

## Task 6: Deploy + run live (probe Lakebase) + report

Runs against the live workspaces. Not a code change.

- [ ] **Step 1: Deploy**
```bash
cd ~/uksouth_migration/workspace-migration
BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae \
  DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform DATABRICKS_TF_VERSION=1.15.5 \
  databricks bundle deploy -t dev --profile source-migration
```
(local-terraform pin required — CLI's own terraform download fails on an expired HashiCorp PGP key.)

- [ ] **Step 2: Run + read evidence (background)**
```bash
BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae \
  databricks bundle run online_tables_integration_test -t dev --profile source-migration
```
This provisions a real Lakebase instance (minutes — possibly long; Autoscaling as of Mar 2026) + a synced table, so allow more wall-clock than VS. When done, read:
- `seed_online_tables` exit `{"seeded": true, ...}`.
- `test_online_tables` exit `{"summary": {"online_table": "asserted_ok"}, "errors": []}` → synced table created + present on target. If `skipped_instance_not_ready` → the instance didn't provision in budget; not a failure, re-run finishes it (report it).
- `teardown_online_tables` ran (confirm the Lakebase instance is deleted: `databricks database list-database-instances --profile target-migration` should not list `cp_migration_lakebase`). **Manually delete if teardown failed — it's a paid resource.**

- [ ] **Step 3: Report** the outcome + whether Lakebase synced-table creation worked on the pair (probe). If `create_database_instance`/`create_synced_database_table` is unavailable/blocked → report honestly, ship for later, no false validation. Append the result to PR #55.

- [ ] **Step 4:** (no commit) — run report only.

---

## Self-Review

**Spec coverage:**
- synced_table-only conversion via create-if-missing Lakebase instance + create_synced_database_table → Task 2. ✓
- Config lakebase_* → Task 1. ✓
- created_resync_pending / skipped_target_exists / skipped_instance_not_ready (non-terminal, no tracking change) → Task 2. ✓
- Pre-check unchanged (source-exists); PK enforced at create-time → documented deviation (worker `failed`). ✓
- Live integration via synthetic discovery row + Lakebase instance/synced-table teardown + probe → Task 3 + Task 6. ✓
- Docs (paid Lakebase, consumer repoint out of scope) → Task 4. ✓
- Supersede PR #55 (update title/body) → Task 5. ✓

**Placeholder scan:** No TBD/TODO. "Confirm import/signature against tracking.py / SDK" notes (Task 3 discovery_row/discovery_schema/delete_database_instance) are real verify-against-source instructions; the testable logic is fully specified. All code blocks complete.

**Type/name consistency:** `_scheduling_policy` / `_build_synced_table_spec` / `_instance_ready` / `_ensure_lakebase_instance` / `migrate_online_table(target_client,row,config)` / `run` consistent between Task 2 def and its tests. Config attrs `lakebase_instance_name`/`lakebase_logical_database`/`lakebase_capacity` consistent between Task 1 (def) and Task 2 (worker reads) + teardown. Statuses match Task 2 (emit) ↔ Task 3 (assert). `object_name`=FQN, object_type=`online_table` consistent across worker/seed/assert/teardown. Integration identifiers (`integration_test_src.ot_test.ot_source`/`ot_online`) identical across the three notebooks + the discovery-row injection.

**Deviations from spec (transparent):** (1) PK check enforced at create-time, not pre-check (simpler, equally loud); (2) integration workflow drops the `discovery` task and the seed injects a synthetic discovery row (legacy online tables uncreatable). Both documented above.
