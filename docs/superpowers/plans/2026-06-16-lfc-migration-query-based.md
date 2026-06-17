# LFC Migration — Query-Based Connector (Stage 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate query-based Lakeflow Connect pipelines (scenario C / Tier 1) — clone the landed history, recreate the pipeline on target with a per-table cursor `row_filter` so it pulls only post-cutover rows, and expose a unified view — as the first stage of the `migrate_lfc` job.

**Architecture:** A new standalone job `migrate_lfc` mirrors `migrate_vector_search` (`pre_check_lfc → orchestrator → migrate_lfc → summary_lfc`). Discovery already captures the full pipeline spec under `metadata_json.definition`, so no discovery change. Pure transformation logic (tier classification, boundary/spec/view SQL builders) lives in a new testable `src/migrate/lfc_utils.py`. The data clone reuses the existing Delta-Sharing staging path (`setup_sharing` helpers + `clone_table`). This stage handles **query-based connectors only**; CDC/gateway and SaaS are later stages.

**Tech Stack:** Python 3.10, Databricks SDK (`pipelines`, `connections`, `tables`), Delta Sharing + DEEP CLONE, DAB (Databricks Asset Bundles), pytest.

**Scope boundary (this stage):** query-based DB connectors only (Oracle/Teradata/SQL Server-qb/MySQL-qb/MariaDB-qb/PostgreSQL-qb/federation). Non-query-based LFC rows are left unprocessed (no status written) so a later stage's worker picks them up. No gateway logic, no Tier-2 full-reload, no SaaS.

---

## File structure

| File | Responsibility |
|---|---|
| `src/migrate/lfc_utils.py` (new) | Pure logic: tier/connector classification, per-table config extraction, boundary `T` SQL, recreated-pipeline spec builder, unified-view SQL builder. No side effects → fully unit-testable. |
| `src/migrate/lfc_worker.py` (new) | Notebook worker: orchestrates per-pipeline migration (clone history → recreate pipeline → build view) using `lfc_utils` + reused sharing/clone helpers. |
| `src/pre_check/pre_check_lfc.py` (new) | Pre-check: connection + destination catalog/schema exist on target for each query-based pipeline; classify + record. |
| `src/common/tracking.py` (modify) | Add Stage-1 terminal statuses. |
| `src/migrate/managed_table_worker.py` (modify) | `clone_table`: add `target_fqn` + `object_type` params (default to current behaviour). |
| `src/migrate/orchestrator.py` (modify) | Add `"lfc_pipeline"` to `LIST_TYPES`. |
| `resources/production/migrate_lfc_workflow.yml` (new) | The `migrate_lfc` job. |
| `resources/integration_tests/lfc_integration_test_workflow.yml` (new) | Real-resource integration test (query-based). |
| `tests/integration/seed_lfc_test_data.py`, `test_lfc.py`, `teardown_lfc.py` (new) | Integration seed/assert/teardown. |
| `tests/unit/test_lfc_utils.py`, `test_lfc_worker.py`, `test_pre_check_lfc.py` (new) | Unit tests. |
| `docs/user_guide.md`, `docs/stateful_services_phase.md` (modify) | Doc the `migrate_lfc` query-based path + caveats. |

---

## Task 1: Add Stage-1 LFC terminal statuses

**Files:**
- Modify: `src/common/tracking.py:97-111`
- Test: `tests/unit/test_lfc_utils.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_lfc_utils.py
from common.tracking import _TERMINAL_STATUSES

def test_lfc_stage1_statuses_are_terminal():
    for s in ("lfc_pipeline_created_incremental", "lfc_view_created"):
        assert s in _TERMINAL_STATUSES
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_lfc_utils.py::test_lfc_stage1_statuses_are_terminal -v`
Expected: FAIL — statuses not in tuple.

- [ ] **Step 3: Add the statuses**

In `src/common/tracking.py`, append inside the `_TERMINAL_STATUSES` tuple (before the closing `)` at line 111):

```python
    # Lakeflow Connect (migrate_lfc, Tier 1): pipeline recreated on target with
    # a per-table cursor row_filter (pulls only post-cutover rows). Terminal so
    # re-runs don't recreate it.
    "lfc_pipeline_created_incremental",
    # Lakeflow Connect: unified view created over <t>_history + <t>_incr.
    "lfc_view_created",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_lfc_utils.py::test_lfc_stage1_statuses_are_terminal -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/common/tracking.py tests/unit/test_lfc_utils.py
git commit -m "feat(lfc): add Stage-1 terminal statuses"
```

---

## Task 2: Refactor `clone_table` for explicit target FQN + object_type

**Files:**
- Modify: `src/migrate/managed_table_worker.py:105-354`
- Test: `tests/unit/test_managed_table_worker.py` (existing file; add a test)

The current signature is `clone_table(table_info, *, config, auth, tracker, validator, wh_id, share_name)` and internally sets `target_fqn = obj_name` and hardcodes `object_type="managed_table"`. Add two optional keyword args that default to the current behaviour, so existing callers are unaffected.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_managed_table_worker.py  (add)
from unittest.mock import MagicMock
from migrate.managed_table_worker import clone_table

def test_clone_table_honours_explicit_target_fqn_and_object_type(monkeypatch):
    # Force the simple "target already exists → validate, no re-clone" path.
    import migrate.managed_table_worker as m
    monkeypatch.setattr(m, "_target_table_exists", lambda auth, fqn: True)
    cfg = MagicMock(dry_run=False, overwrite_existing=False, iceberg_strategy="ddl_replay")
    tracker = MagicMock()
    tracker.get_staging_for_original.return_value = None
    validator = MagicMock()
    validator.validate_row_count.return_value = {"match": True, "source_count": 5, "target_count": 5}
    res = clone_table(
        {"object_name": "`c`.`s`.`account`", "format": "delta"},
        config=cfg, auth=MagicMock(), tracker=tracker, validator=validator,
        wh_id="w", share_name="cp_migration_share",
        target_fqn="`c`.`s`.`account_history`", object_type="lfc_table",
    )
    assert res["object_type"] == "lfc_table"
    # validate_row_count compares source object vs the EXPLICIT target fqn
    validator.validate_row_count.assert_called_with("`c`.`s`.`account`", "`c`.`s`.`account_history`")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_managed_table_worker.py::test_clone_table_honours_explicit_target_fqn_and_object_type -v`
Expected: FAIL — `clone_table() got an unexpected keyword argument 'target_fqn'`.

- [ ] **Step 3: Implement the refactor**

In `src/migrate/managed_table_worker.py`, change the signature (line 105-114) to add the two params:

```python
def clone_table(
    table_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    validator: Validator,
    wh_id: str,
    share_name: str,
    target_fqn: str | None = None,
    object_type: str = "managed_table",
) -> dict:
```

Then, immediately after `_catalog, schema, table = parts` (line 139), replace:

```python
    target_fqn = obj_name  # same FQN on target
```

with:

```python
    target_fqn = target_fqn or obj_name  # default: same FQN on target
```

Finally, replace every literal `"object_type": "managed_table",` in this function's return dicts (and the in_progress append) with `"object_type": object_type,`. There are 9 occurrences in `clone_table` (lines ~133, 149, 177, 235, 254, 264, 299, 327, 338, 350) — use find/replace **scoped to the function body only** (do NOT touch other functions). Verify with:

```bash
grep -n '"object_type": "managed_table"' src/migrate/managed_table_worker.py
```
Expected after edit: zero matches inside `clone_table` (other functions may still have their own).

- [ ] **Step 4: Run tests to verify pass (new + existing regression)**

Run: `pytest tests/unit/test_managed_table_worker.py -v`
Expected: PASS — new test passes AND all existing `clone_table` tests still pass (defaults preserve old behaviour).

- [ ] **Step 5: Commit**

```bash
git add src/migrate/managed_table_worker.py tests/unit/test_managed_table_worker.py
git commit -m "refactor(clone_table): explicit target_fqn + parametrized object_type"
```

---

## Task 3: Publish `lfc_pipeline` from the orchestrator

**Files:**
- Modify: `src/migrate/orchestrator.py:50-74`
- Test: `tests/unit/test_orchestrator.py` (existing; add a test)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_orchestrator.py  (add)
from migrate.orchestrator import LIST_TYPES

def test_lfc_pipeline_is_published_as_a_list_type():
    assert "lfc_pipeline" in LIST_TYPES
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_orchestrator.py::test_lfc_pipeline_is_published_as_a_list_type -v`
Expected: FAIL.

- [ ] **Step 3: Add to LIST_TYPES**

In `src/migrate/orchestrator.py`, add to the `LIST_TYPES` tuple (after `"vector_search_index",` at line 73):

```python
    # Stateful Services Phase — consumed by the migrate_lfc job's worker.
    # Harmless for other jobs; they ignore the published list.
    "lfc_pipeline",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_orchestrator.py::test_lfc_pipeline_is_published_as_a_list_type -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/migrate/orchestrator.py tests/unit/test_orchestrator.py
git commit -m "feat(lfc): publish lfc_pipeline list from orchestrator"
```

---

## Task 4: Connector classification + per-table config extraction (`lfc_utils`)

**Files:**
- Create: `src/migrate/lfc_utils.py`
- Test: `tests/unit/test_lfc_utils.py`

Classify a discovered LFC pipeline definition into a tier and connector kind, and pull the per-table config we need. Query-based pipelines have an `ingestion_definition` with table `table_configuration.cursor_column` set and **no** `gateway_definition` / `ingestion_gateway_id`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_lfc_utils.py  (add)
from migrate.lfc_utils import classify_pipeline, extract_table_configs

QB_DEF = {
    "spec": {
        "ingestion_definition": {
            "connection_name": "src_pg",
            "objects": [
                {"table": {"source_catalog": "pg", "source_schema": "public",
                           "source_table": "orders",
                           "destination_catalog": "bronze", "destination_schema": "pg",
                           "destination_table": "orders",
                           "table_configuration": {"scd_type": "SCD_TYPE_1",
                                                   "primary_keys": ["order_id"],
                                                   "cursor_column": "updated_at"}}},
            ],
        }
    }
}
CDC_DEF = {"spec": {"ingestion_definition": {"ingestion_gateway_id": "gw-123", "objects": []}}}

def test_classify_query_based():
    assert classify_pipeline(QB_DEF) == ("query_based", "tier1")

def test_classify_cdc_is_tier2_not_this_stage():
    assert classify_pipeline(CDC_DEF) == ("cdc", "tier2")

def test_extract_table_configs():
    cfgs = extract_table_configs(QB_DEF)
    assert cfgs == [{
        "source_catalog": "pg", "source_schema": "public", "source_table": "orders",
        "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "orders",
        "scd_type": "SCD_TYPE_1", "primary_keys": ["order_id"], "cursor_column": "updated_at",
    }]
```

- [ ] **Step 2: Run to verify fail**

Run: `pytest tests/unit/test_lfc_utils.py -k "classify or extract_table" -v`
Expected: FAIL — module/functions missing.

- [ ] **Step 3: Implement**

```python
# src/migrate/lfc_utils.py
"""Pure transformation helpers for migrate_lfc (no side effects, unit-tested).

Stage 1 supports query-based connectors only. CDC and SaaS classify here but
are handled by later stages.
"""
from __future__ import annotations


def _ingestion_def(definition: dict) -> dict:
    return ((definition or {}).get("spec") or {}).get("ingestion_definition") or {}


def classify_pipeline(definition: dict) -> tuple[str, str]:
    """Return (connector_kind, tier).

    connector_kind ∈ {"query_based", "cdc", "saas", "unknown"}; tier ∈ {"tier1","tier2"}.
    Query-based: ingestion_definition with no gateway and at least one object
    carrying a cursor_column. CDC: an ingestion_gateway_id is present. Otherwise
    SaaS/unknown (later stages).
    """
    idef = _ingestion_def(definition)
    if idef.get("ingestion_gateway_id"):
        return ("cdc", "tier2")
    objs = idef.get("objects") or []
    has_cursor = any(
        (o.get("table") or {}).get("table_configuration", {}).get("cursor_column")
        for o in objs
    )
    if has_cursor:
        return ("query_based", "tier1")
    if idef:
        return ("saas", "tier1")  # row_filter capability decided per-connector later
    return ("unknown", "tier2")


def extract_table_configs(definition: dict) -> list[dict]:
    """Flatten ingestion_definition.objects[].table into plain dicts."""
    out: list[dict] = []
    for o in _ingestion_def(definition).get("objects") or []:
        t = o.get("table") or {}
        tc = t.get("table_configuration") or {}
        out.append({
            "source_catalog": t.get("source_catalog"),
            "source_schema": t.get("source_schema"),
            "source_table": t.get("source_table"),
            "destination_catalog": t.get("destination_catalog"),
            "destination_schema": t.get("destination_schema"),
            "destination_table": t.get("destination_table"),
            "scd_type": tc.get("scd_type"),
            "primary_keys": tc.get("primary_keys") or [],
            "cursor_column": tc.get("cursor_column"),
        })
    return out
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_lfc_utils.py -k "classify or extract_table" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/migrate/lfc_utils.py tests/unit/test_lfc_utils.py
git commit -m "feat(lfc): connector classification + table-config extraction"
```

---

## Task 5: Unified-view SQL builder (`lfc_utils`)

**Files:**
- Modify: `src/migrate/lfc_utils.py`
- Test: `tests/unit/test_lfc_utils.py`

Build the `CREATE OR REPLACE VIEW` SQL at the canonical name over `<t>_history` + `<t>_incr`. SCD1 → PK-dedup merge (latest cursor wins); SCD2/APPEND_ONLY → `UNION ALL`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_lfc_utils.py  (add)
from migrate.lfc_utils import build_unified_view_sql

def test_view_scd1_dedups_by_pk_latest_cursor():
    sql = build_unified_view_sql(
        canonical="`bronze`.`pg`.`orders`",
        history="`bronze`.`pg`.`orders_history`",
        incr="`bronze`.`pg`.`orders_incr`",
        scd_type="SCD_TYPE_1", primary_keys=["order_id"], cursor_column="updated_at",
    )
    assert "CREATE OR REPLACE VIEW `bronze`.`pg`.`orders`" in sql
    assert "PARTITION BY order_id" in sql
    assert "ORDER BY updated_at DESC" in sql
    assert "rn = 1" in sql

def test_view_scd2_is_union_all():
    sql = build_unified_view_sql(
        canonical="`bronze`.`pg`.`orders`",
        history="`bronze`.`pg`.`orders_history`",
        incr="`bronze`.`pg`.`orders_incr`",
        scd_type="SCD_TYPE_2", primary_keys=["order_id"], cursor_column="updated_at",
    )
    assert "UNION ALL" in sql
    assert "ROW_NUMBER" not in sql
```

- [ ] **Step 2: Run to verify fail**

Run: `pytest tests/unit/test_lfc_utils.py -k view -v`
Expected: FAIL.

- [ ] **Step 3: Implement**

```python
# src/migrate/lfc_utils.py  (add)
def build_unified_view_sql(
    *, canonical: str, history: str, incr: str,
    scd_type: str, primary_keys: list[str], cursor_column: str,
) -> str:
    """CREATE OR REPLACE VIEW at `canonical` over history+incr.

    SCD1 → keep one current row per PK (latest cursor wins). SCD2/APPEND_ONLY →
    UNION ALL (history segments are distinct rows by design).
    """
    if str(scd_type).upper() == "SCD_TYPE_1":
        pk = ", ".join(primary_keys)
        return (
            f"CREATE OR REPLACE VIEW {canonical} AS\n"
            f"SELECT * EXCEPT(_rn) FROM (\n"
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk} "
            f"ORDER BY {cursor_column} DESC) AS _rn\n"
            f"  FROM (SELECT * FROM {history} UNION ALL SELECT * FROM {incr})\n"
            f") WHERE _rn = 1"
        )
    return (
        f"CREATE OR REPLACE VIEW {canonical} AS\n"
        f"SELECT * FROM {history} UNION ALL SELECT * FROM {incr}"
    )
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_lfc_utils.py -k view -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/migrate/lfc_utils.py tests/unit/test_lfc_utils.py
git commit -m "feat(lfc): unified-view SQL builder (SCD1 merge / SCD2 union)"
```

---

## Task 6: Recreated-pipeline spec builder (`lfc_utils`)

**Files:**
- Modify: `src/migrate/lfc_utils.py`
- Test: `tests/unit/test_lfc_utils.py`

Transform the discovered query-based spec into the create-spec dict: point each incremental table's destination at `<table>_incr`, set `row_filter = "<cursor> >= '<T>'"`, set `channel="PREVIEW"`, and swap the connection name to the target. `boundaries` maps source_table → `T` string.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_lfc_utils.py  (add)
from migrate.lfc_utils import build_query_based_create_spec

def test_build_create_spec_sets_incr_dest_and_row_filter():
    spec = build_query_based_create_spec(
        QB_DEF, target_connection_name="tgt_pg",
        boundaries={"orders": "2026-06-10T00:00:00"}, name="lfc_orders_incr",
    )
    assert spec["channel"] == "PREVIEW"
    idef = spec["ingestion_definition"]
    assert idef["connection_name"] == "tgt_pg"
    tbl = idef["objects"][0]["table"]
    assert tbl["destination_table"] == "orders_incr"
    assert tbl["table_configuration"]["row_filter"] == "updated_at >= '2026-06-10T00:00:00'"
    # cursor + scd + pk carried over
    assert tbl["table_configuration"]["cursor_column"] == "updated_at"
    assert tbl["table_configuration"]["scd_type"] == "SCD_TYPE_1"
```

- [ ] **Step 2: Run to verify fail**

Run: `pytest tests/unit/test_lfc_utils.py -k create_spec -v`
Expected: FAIL.

- [ ] **Step 3: Implement**

```python
# src/migrate/lfc_utils.py  (add)
import copy


def build_query_based_create_spec(
    definition: dict, *, target_connection_name: str,
    boundaries: dict[str, str], name: str,
) -> dict:
    """Build the pipelines.create spec for the recreated query-based pipeline.

    boundaries maps source_table -> cursor boundary T. A table with a boundary
    gets destination `<table>_incr` + row_filter `<cursor> >= 'T'`; a table
    WITHOUT a boundary (batch/no-cursor) keeps the canonical destination and no
    filter (full-load — its normal behaviour).
    """
    idef = copy.deepcopy(_ingestion_def(definition))
    idef["connection_name"] = target_connection_name
    for o in idef.get("objects") or []:
        t = o.get("table") or {}
        tc = t.setdefault("table_configuration", {})
        src = t.get("source_table")
        cursor = tc.get("cursor_column")
        if src in boundaries and cursor:
            t["destination_table"] = f"{t['destination_table']}_incr"
            tc["row_filter"] = f"{cursor} >= '{boundaries[src]}'"
    return {"name": name, "channel": "PREVIEW", "ingestion_definition": idef}
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_lfc_utils.py -k create_spec -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/migrate/lfc_utils.py tests/unit/test_lfc_utils.py
git commit -m "feat(lfc): query-based recreate-spec builder (row_filter boundary)"
```

---

## Task 7: Pre-check (`pre_check_lfc.py`)

**Files:**
- Create: `src/pre_check/pre_check_lfc.py`
- Test: `tests/unit/test_pre_check_lfc.py`

For each query-based `lfc_pipeline` row: target UC connection exists and each destination catalog/schema exists. FAIL the gate (mirrors `pre_check_vector_search`) on any missing. Non-query-based rows are ignored this stage.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_pre_check_lfc.py
import json
from unittest.mock import MagicMock
from pre_check.pre_check_lfc import find_blockers

def _row(defn):
    return {"object_name": "p1", "metadata_json": json.dumps({"definition": defn})}

QB = {"spec": {"ingestion_definition": {"connection_name": "src_pg", "objects": [
    {"table": {"destination_catalog": "bronze", "destination_schema": "pg",
               "destination_table": "orders",
               "table_configuration": {"cursor_column": "updated_at"}}}]}}}

def test_blocker_when_connection_missing():
    tc = MagicMock()
    tc.connections.get.side_effect = Exception("no conn")
    tc.schemas.get.return_value = object()
    blockers = find_blockers(tc, [_row(QB)], target_connection_name="src_pg")
    assert any("connection" in b.lower() for b in blockers)

def test_no_blocker_when_present():
    tc = MagicMock()
    tc.connections.get.return_value = object()
    tc.schemas.get.return_value = object()
    assert find_blockers(tc, [_row(QB)], target_connection_name="src_pg") == []
```

- [ ] **Step 2: Run to verify fail**

Run: `pytest tests/unit/test_pre_check_lfc.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement** (model on `pre_check_vector_search.py`)

```python
# src/pre_check/pre_check_lfc.py
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
import json
import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.lfc_utils import classify_pipeline, extract_table_configs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pre_check_lfc")
# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def find_blockers(target_client, rows: list[dict], *, target_connection_name: str) -> list[str]:
    """Hard blockers for query-based pipelines: target connection + each
    destination catalog.schema must exist. Non-query-based rows are skipped."""
    blockers: list[str] = []
    checked_conn = False
    for row in rows:
        definition = (json.loads(row.get("metadata_json") or "{}") or {}).get("definition") or {}
        kind, _ = classify_pipeline(definition)
        if kind != "query_based":
            continue
        if not checked_conn:
            try:
                target_client.connections.get(target_connection_name)
            except Exception:  # noqa: BLE001
                blockers.append(f"Target UC connection '{target_connection_name}' not found.")
            checked_conn = True
        for tc in extract_table_configs(definition):
            fqn = f"{tc['destination_catalog']}.{tc['destination_schema']}"
            try:
                target_client.schemas.get(fqn)
            except Exception:  # noqa: BLE001
                blockers.append(f"Destination schema '{fqn}' not found on target.")
    return blockers


# COMMAND ----------
def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    rows = tracker.get_pending_objects("lfc_pipeline")
    conn = config.lfc_target_connection_name
    blockers = find_blockers(auth.target_client, rows, target_connection_name=conn)
    status = "PASS" if not blockers else "FAIL"
    tracker.append_pre_check_results([{
        "check_name": "lfc_query_based_prereqs",
        "status": status,
        "message": "" if not blockers else "; ".join(sorted(set(blockers))),
        "action_required": "" if not blockers else "Run migrate_uc + migrate connection first, then re-run.",
    }])
    if blockers:
        raise RuntimeError(f"migrate_lfc pre-check FAILED: {sorted(set(blockers))}")
    logger.info("[lfc] pre-check PASS — %d pipeline row(s).", len(rows))


# COMMAND ----------
if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
```

Add the config field used above — in `src/common/config.py`, add `lfc_target_connection_name` (default reading from config dict key `lfc_target_connection_name`, fallback `""`). Follow the existing optional-field pattern in that file (mirror how `rls_cm_strategy` is read).

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_pre_check_lfc.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/pre_check/pre_check_lfc.py src/common/config.py tests/unit/test_pre_check_lfc.py
git commit -m "feat(lfc): pre-check connection + destination schema prereqs"
```

---

## Task 8: The worker (`lfc_worker.py`)

**Files:**
- Create: `src/migrate/lfc_worker.py`
- Test: `tests/unit/test_lfc_worker.py`

Per query-based pipeline: (a) per incremental table compute `T = MAX(cursor)` on the source, clone history to `<t>_history` via the reused sharing+`clone_table` path; (b) recreate the pipeline (`build_query_based_create_spec`) via `pipelines.create`; (c) create the unified view at the canonical name. Per-pipeline isolation; idempotent (skip if the target view already exists). The worker composes already-tested helpers, so the unit test mocks the seams and asserts orchestration + statuses.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_lfc_worker.py
import json
from unittest.mock import MagicMock, patch
from migrate.lfc_worker import migrate_pipeline

DEF = {"spec": {"ingestion_definition": {"connection_name": "src_pg", "objects": [
    {"table": {"source_catalog": "pg", "source_schema": "public", "source_table": "orders",
               "destination_catalog": "bronze", "destination_schema": "pg", "destination_table": "orders",
               "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["order_id"],
                                       "cursor_column": "updated_at"}}}]}}}

def _row():
    return {"object_name": "lfc_orders", "object_type": "lfc_pipeline",
            "metadata_json": json.dumps({"definition": DEF})}

def test_migrate_query_based_pipeline_happy_path():
    deps = MagicMock()
    deps.compute_boundary.return_value = "2026-06-10T00:00:00"   # MAX(cursor)
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_view_exists.return_value = False
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="tgt_pg")
    statuses = {r["status"] for r in results}
    assert "lfc_pipeline_created_incremental" in statuses
    assert "lfc_view_created" in statuses
    # recreated pipeline carried the row_filter boundary
    spec = deps.create_pipeline.call_args.args[0]
    assert spec["ingestion_definition"]["objects"][0]["table"]["table_configuration"]["row_filter"] \
        == "updated_at >= '2026-06-10T00:00:00'"

def test_non_query_based_pipeline_is_skipped_this_stage():
    cdc = {"object_name": "p", "object_type": "lfc_pipeline",
           "metadata_json": json.dumps({"definition": {"spec": {"ingestion_definition": {"ingestion_gateway_id": "g"}}}})}
    assert migrate_pipeline(cdc, deps=MagicMock(), target_connection_name="t") == []

def test_idempotent_when_view_exists():
    deps = MagicMock()
    deps.target_view_exists.return_value = True
    results = migrate_pipeline(_row(), deps=deps, target_connection_name="t")
    assert all(r["status"] == "skipped_target_pipeline_exists" for r in results)
    deps.create_pipeline.assert_not_called()
```

- [ ] **Step 2: Run to verify fail**

Run: `pytest tests/unit/test_lfc_worker.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement**

Implement `migrate_pipeline(row, *, deps, target_connection_name)` as pure orchestration over a `deps` object (injected seam, so unit tests mock it and the live `run()` wires the real implementations). `deps` exposes: `compute_boundary(source_warehouse_id, table_fqn, cursor_column) -> str`, `clone_history(table_cfg) -> dict`, `create_pipeline(spec) -> None`, `target_view_exists(canonical_fqn) -> bool`, `create_view(sql) -> None`.

```python
# src/migrate/lfc_worker.py
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
import json
import logging
import time

from migrate.lfc_utils import (
    build_query_based_create_spec,
    build_unified_view_sql,
    classify_pipeline,
    extract_table_configs,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lfc_worker")
# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def _bt(fqn: str) -> str:
    """Backtick a dotted FQN: a.b.c -> `a`.`b`.`c`."""
    return ".".join(f"`{p}`" for p in fqn.split("."))


def migrate_pipeline(row: dict, *, deps, target_connection_name: str) -> list[dict]:
    """Migrate one query-based lfc_pipeline row. Returns status dicts.

    Non-query-based rows return [] (left for a later stage). Per-pipeline
    isolation is the caller's job (it wraps this in try/except per row).
    """
    obj_name = row["object_name"]
    definition = (json.loads(row.get("metadata_json") or "{}") or {}).get("definition") or {}
    kind, _tier = classify_pipeline(definition)
    if kind != "query_based":
        logger.info("[lfc] %s is %s — deferred to a later stage.", obj_name, kind)
        return []

    cfgs = extract_table_configs(definition)
    # Idempotency: if every canonical view already exists, skip.
    canon = [f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}" for c in cfgs]
    if canon and all(deps.target_view_exists(_bt(f)) for f in canon):
        return [{"object_name": obj_name, "object_type": "lfc_pipeline",
                 "status": "skipped_target_pipeline_exists", "error_message": None}]

    results: list[dict] = []
    boundaries: dict[str, str] = {}
    start = time.time()

    # (a) clone history + compute boundary per incremental table
    for c in cfgs:
        if not c.get("cursor_column"):
            continue  # batch table: no clone, recreated pipeline full-loads it
        src_table_fqn = f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}"
        boundaries[c["source_table"]] = deps.compute_boundary(src_table_fqn, c["cursor_column"])
        clone = deps.clone_history(c)
        results.append({"object_name": f"{src_table_fqn}_history", "object_type": "lfc_table",
                        "status": clone["status"], "error_message": clone.get("error_message")})

    # (b) recreate the pipeline
    spec = build_query_based_create_spec(
        definition, target_connection_name=target_connection_name,
        boundaries=boundaries, name=f"{obj_name}_migrated",
    )
    deps.create_pipeline(spec)
    results.append({"object_name": obj_name, "object_type": "lfc_pipeline",
                    "status": "lfc_pipeline_created_incremental", "error_message": None,
                    "duration_seconds": time.time() - start})

    # (c) unified view per incremental table
    for c in cfgs:
        if not c.get("cursor_column"):
            continue
        base = f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}"
        sql = build_unified_view_sql(
            canonical=_bt(base), history=_bt(f"{base}_history"), incr=_bt(f"{base}_incr"),
            scd_type=c["scd_type"], primary_keys=c["primary_keys"], cursor_column=c["cursor_column"],
        )
        deps.create_view(sql)
        results.append({"object_name": base, "object_type": "lfc_view",
                        "status": "lfc_view_created", "error_message": None})
    return results
```

Then add `run(dbutils, spark)` that: builds `config`/`auth`/`tracker`, reads the `lfc_pipeline_list` task value (`taskKey="orchestrator"`), constructs a real `deps` object whose methods wrap: `compute_boundary` → `execute_and_fetch` (source warehouse) `SELECT MAX(<cursor>) FROM <fqn>`; `clone_history` → the sharing helpers + `clone_table(..., target_fqn=<_history>, object_type="lfc_table")`; `create_pipeline` → `auth.target_client.pipelines.create(**spec)` (use `CreatePipeline.from_dict(spec)` if the typed API rejects the raw dict — validate against the SDK at execution); `target_view_exists`/`create_view` → target-warehouse exec. Loop pipelines with per-row try/except, collect results, `tracker.append_migration_status(results)`. Mirror `vector_search_worker.run`. End with the notebook guard.

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_lfc_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/migrate/lfc_worker.py tests/unit/test_lfc_worker.py
git commit -m "feat(lfc): query-based worker (clone history + recreate + view)"
```

---

## Task 9: Production workflow YAML

**Files:**
- Create: `resources/production/migrate_lfc_workflow.yml`
- Test: `tests/unit/test_workflow_shape.py` (existing convention; add an assertion that the job + 4 tasks exist) — if no such test exists, add a minimal YAML-parse test.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_lfc_workflow.py
import yaml
from pathlib import Path

def test_migrate_lfc_workflow_has_expected_tasks():
    doc = yaml.safe_load(Path("resources/production/migrate_lfc_workflow.yml").read_text())
    tasks = {t["task_key"] for t in doc["resources"]["jobs"]["migrate_lfc"]["tasks"]}
    assert {"pre_check_lfc", "orchestrator", "migrate_lfc", "summary_lfc"} <= tasks
```

- [ ] **Step 2: Run to verify fail**

Run: `pytest tests/unit/test_lfc_workflow.py -v`
Expected: FAIL — file missing.

- [ ] **Step 3: Implement** (model on `migrate_vector_search_workflow.yml`)

```yaml
resources:
  jobs:
    migrate_lfc:
      name: "${var.job_prefix}-migrate-lfc"
      description: |
        Lakeflow Connect migration (Stage 1: query-based connectors). Clones
        each landed destination table to <t>_history, recreates the pipeline
        on target with a per-table cursor row_filter (>= the migrated boundary)
        writing to <t>_incr, and creates a unified view at the canonical name.
        Trust-the-operator: target catalogs/schemas + the UC connection must
        already exist (run migrate_uc + connection migration first).
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: pre_check_lfc
          notebook_task:
            notebook_path: ../../src/pre_check/pre_check_lfc.py
        - task_key: orchestrator
          depends_on:
            - task_key: pre_check_lfc
          notebook_task:
            notebook_path: ../../src/migrate/orchestrator.py
        - task_key: migrate_lfc
          depends_on:
            - task_key: orchestrator
          notebook_task:
            notebook_path: ../../src/migrate/lfc_worker.py
        - task_key: summary_lfc
          run_if: ALL_DONE
          depends_on:
            - task_key: migrate_lfc
          notebook_task:
            notebook_path: ../../src/migrate/summary.py
            base_parameters:
              object_types: "lfc_pipeline,lfc_table,lfc_view"
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_lfc_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add resources/production/migrate_lfc_workflow.yml tests/unit/test_lfc_workflow.py
git commit -m "feat(lfc): migrate_lfc production workflow"
```

---

## Task 10: Integration test workflow + seed/assert/teardown (real Azure SQL)

**Files:**
- Create: `resources/integration_tests/lfc_integration_test_workflow.yml`
- Create: `tests/integration/seed_lfc_test_data.py`, `tests/integration/test_lfc.py`, `tests/integration/teardown_lfc.py`

This stage's real source is the existing Azure SQL Server (query-based needs **no** CDC/CT and runs on serverless via the existing NCC PE). Seed creates a query-based LFC pipeline against `dbo.orders`/`dbo.customers` (their `placed_at`/`created_at` are cursor columns), lets it land data, then discovery→pre_check→migrate→assert.

- [ ] **Step 1: Write the workflow** (model on `vector_search_integration_test_workflow.yml`)

```yaml
resources:
  jobs:
    lfc_integration_test:
      name: "${var.job_prefix}-lfc-integration-test"
      description: |
        Lakeflow Connect query-based integration test (real Azure SQL source).
        Seeds a query-based pipeline + landed data, runs discovery, invokes
        migrate_lfc, asserts <t>_history clone + <t>_incr pipeline with a
        cursor row_filter + unified view at the canonical name.
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
              catalog_filter: "integration_test_lfc"
        - task_key: seed_lfc
          depends_on: [{task_key: setup_test_config}]
          notebook_task:
            notebook_path: ../../tests/integration/seed_lfc_test_data.py
        - task_key: discovery
          depends_on: [{task_key: seed_lfc}]
          notebook_task:
            notebook_path: ../../src/discovery/discovery.py
        - task_key: migrate_lfc
          depends_on: [{task_key: discovery}]
          run_job_task:
            job_id: ${resources.jobs.migrate_lfc.id}
        - task_key: test_lfc
          depends_on: [{task_key: migrate_lfc}]
          notebook_task:
            notebook_path: ../../tests/integration/test_lfc.py
        - task_key: teardown_lfc
          depends_on:
            - {task_key: setup_test_config}
            - {task_key: seed_lfc}
            - {task_key: discovery}
            - {task_key: migrate_lfc}
            - {task_key: test_lfc}
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../../tests/integration/teardown_lfc.py
```

- [ ] **Step 2: Write `seed_lfc_test_data.py`** — create the UC connection to the Azure SQL (host/port/user from the secret scope `migration`), create a **query-based** ingestion pipeline (`pipelines.create` with `ingestion_definition` objects for `dbo.orders` + `dbo.customers`, `cursor_column="placed_at"`/`"created_at"`, `scd_type="SCD_TYPE_1"`, destination catalog/schema `integration_test_lfc.sqlsrv`), run it once and wait for the destination STs to land. Emit a `dbutils.notebook.exit(json)` with `{"has_query_based": true, "boundary_orders": "<max placed_at>"}` so the assertion is provable from run output (notebook stdout isn't retrievable via Jobs API — same lesson as VS).

- [ ] **Step 3: Write `test_lfc.py`** (coverage-guard style) — assert, from `migration_status`: an `lfc_table` row `validated` for `orders_history`; an `lfc_pipeline` row `lfc_pipeline_created_incremental`; an `lfc_view` row `lfc_view_created`. Then verify on target via warehouse: `orders_incr` exists and its pipeline spec has `row_filter` referencing `placed_at`; the canonical `orders` view returns `count(history) deduped`. **RED if the query-based scenario isn't exercised** (zero rows). `dbutils.notebook.exit(json summary)`.

- [ ] **Step 4: Write `teardown_lfc.py`** — stop+delete the recreated target pipeline, drop the view + `_history`/`_incr` tables + `integration_test_lfc` catalog (both sides), delete the source seed pipeline, best-effort `ALL_DONE`.

- [ ] **Step 5: Commit**

```bash
git add resources/integration_tests/lfc_integration_test_workflow.yml tests/integration/seed_lfc_test_data.py tests/integration/test_lfc.py tests/integration/teardown_lfc.py
git commit -m "test(lfc): real-resource query-based integration test (Azure SQL)"
```

---

## Task 11: Infra — verify/extend Azure SQL for query-based (no CDC needed)

**Files:**
- Modify: `~/uksouth_migration/infra/azure-sql-test/` (local infra repo — NOT the bundle)

Query-based runs on **serverless via the existing NCC PE** and needs **no** CDC/CT and **no** classic gateway. The existing module already provisions the SQL Server + PE + NCC + cursor-friendly seed (`placed_at`/`created_at`). This stage's infra work is mostly **verification + rebuild** (the sandbox lab has likely expired).

- [ ] **Step 1:** Check lab liveness — `databricks workspaces list` for source/target; `az sql server show -n sqlsrv-wsm-test-ne`. If gone, rebuild per the project's staged terraform flow (workspaces → SQL+PE+NCC) — pause for operator verification between layers (per the staged-infra preference).
- [ ] **Step 2:** Confirm the UC connection to Azure SQL (sqlserver type) + the serverless NCC PE is ESTABLISHED so query-based serverless ingestion can reach the DB.
- [ ] **Step 3:** No Terraform code change needed for query-based (CDC/CT enablement + S3 bump belong to the later CDC stage). Record in the infra README that query-based reuses the existing module as-is.
- [ ] **Step 4:** Commit any infra doc note (infra repo is separate; commit there).

---

## Task 12: Docs

**Files:**
- Modify: `docs/user_guide.md`, `docs/stateful_services_phase.md`

- [ ] **Step 1:** Add a `migrate_lfc` (Stage 1: query-based) section to `user_guide.md`: the clone-history + recreate-with-`row_filter` + unified-view model, Option B layout, the run-the-job-is-opt-in model, and the Stage-1 caveats (deletes after cutover not propagated; SCD1 view dedups by PK; batch/no-cursor tables full-load).
- [ ] **Step 2:** Update `stateful_services_phase.md`: LFC now has a (Stage 1) migration job for query-based connectors; CDC + SaaS are later stages.
- [ ] **Step 3: Commit**

```bash
git add docs/user_guide.md docs/stateful_services_phase.md
git commit -m "docs(lfc): query-based migration + caveats"
```

---

## Self-review notes

- **Spec coverage (query-based slice):** connection (pre-existing) ✓ Task 7; data clone (Option B `_history`) ✓ Tasks 2,8; recreate with per-table `row_filter ≥ T` ✓ Tasks 6,8; unified view SCD1 merge / SCD2 union ✓ Tasks 5,8; per-table strategy (batch tables full-load, no clone/view) ✓ Tasks 6,8; statuses ✓ Task 1; orchestrator wiring ✓ Task 3; pre-check ✓ Task 7; real-resource int test ✓ Tasks 10,11; coverage guard ✓ Task 10 (`test_lfc.py`); docs/caveats ✓ Task 12.
- **Deferred to later stages (explicitly out of this plan):** CDC gateway discovery + recreate + staging volume + id-remap; Tier-2 full-reload; SaaS (`row_filter` + non-`row_filter`); SCD2 boundary stitching beyond plain UNION (Stage-1 SCD2 uses plain UNION ALL — adequate for query-based test; revisit if a boundary-overlap test fails).
- **Type consistency:** `classify_pipeline`/`extract_table_configs`/`build_query_based_create_spec`/`build_unified_view_sql` signatures match across Tasks 4–8; `clone_table(..., target_fqn=, object_type=)` matches Task 2; statuses match Tasks 1/8/10.
- **Execution-time validation flag:** the `pipelines.create` spec shape (raw dict vs typed `CreatePipeline.from_dict`) must be confirmed against the installed SDK in Task 8 Step 3 — the only place the plan can't be 100% verified without the live API.
