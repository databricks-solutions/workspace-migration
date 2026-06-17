# Workflow Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split monolithic `resources/migrate_workflow.yml` into 4 independent jobs (`discovery`, `migrate_uc`, `migrate_hive`, `migrate_governance`) with mirrored test workflows, and remove the `scope.include_uc/include_hive` flags entirely.

**Architecture:** Each `migrate_*` job owns one slice (UC data + UC grants; Hive data + Hive grants; fine-grained governance). `discovery` is a shared upstream job that all three depend on operationally — `pre_check` (per-workflow) fails fast if `discovery_inventory` is missing or stale. Each workflow has its own inline `summary_*` task that filters `migration_status` to its slice. Files reorganized into `resources/production/` + `resources/integration_tests/` subdirs. Hard cutover, single PR; old `migrate_workflow.yml` deleted.

**Tech Stack:** Python 3.11, Databricks SDK, Delta Lake, Databricks Asset Bundles (DAB), pytest, ruff. Repo: `/Users/hari.selvarajan/uksouth_migration/workspace-migration`. Branched off post-Path-A `main`.

**Spec source**: `docs/workflow_split_design.md` (Q1-Q8 + BS-3 + BS-4 D1-D5 decisions captured).

**Prerequisite**: Path A staging_copy PR #45 must be merged into `main` first. This plan assumes branching off post-Path-A main.

---

## Decisions reference (from spec)

| Q | Decision | Implication |
|---|---|---|
| Q1 | Trust the operator for `migrate_governance` standalone | `pre_check_governance` is informational; no soft-skip |
| Q2 | Shared upstream `discovery` job | Each migrate_* depends on it operationally |
| Q3 | Path A obsoletes `restore_rls_cm` | Already done — no separate restore work |
| Q4 | Per-workflow inline summary | `summary_uc`, `summary_hive`, `summary_governance` |
| Q5 | Remove `scope.include_uc/include_hive` flags | Workflow choice is the only gate |
| Q6 | Names: `migrate_uc` / `migrate_hive` / `migrate_governance` | |
| Q7 | Mirror test workflows (3 + unified negative) | New `governance_integration_test` |
| Q8 | Hard cutover, single PR | Old `migrate_workflow.yml` deleted |
| BS-3 | (A) Hard split, no umbrella | 4 independent jobs |
| D1 | UC grants in `migrate_uc`; Hive grants in `migrate_hive` | Each data workflow self-contained for ACL |
| D2 | Customer shares in `migrate_governance` | They're metadata |
| D3 | Governance test pre-seeds via direct SQL | No UC test dependency |
| D4 | Discovery summary = counts only | Not `migration_status` state |
| D5 | Subdirs: `resources/production/` + `resources/integration_tests/` | 4 + 4 files |

---

## File Structure

### New files

| File | Responsibility |
|---|---|
| `resources/production/migrate_uc_workflow.yml` | UC data + UC grants + cleanup_staging |
| `resources/production/migrate_hive_workflow.yml` | Hive data + Hive grants |
| `resources/production/migrate_governance_workflow.yml` | tags, comments, RLS, column masks, customer shares, foreign catalogs, monitors, policies |
| `resources/integration_tests/governance_integration_test_workflow.yml` | New governance e2e test |
| `src/pre_check/pre_check_governance.py` | Informational pre-check for the governance workflow (validates `discovery_inventory` exists; documents trust-the-operator contract) |
| `tests/integration/seed_governance_target_state.py` | Pre-seed target catalog/schema/tables/columns/views via direct SQL for governance standalone test (D3) |
| `tests/integration/test_governance_end_to_end.py` | Governance assertions split out of test_uc_end_to_end.py (Phase 3 items 3.15, 3.17, 3.19, 3.21, 3.22, 3.24) |
| `tests/unit/test_pre_check_governance.py` | Unit tests for the new informational pre-check |

### Renamed / moved (`git mv` to preserve history)

| From | To |
|---|---|
| `resources/discovery_workflow.yml` | `resources/production/discovery_workflow.yml` (extended with summary) |
| `resources/pre_check_workflow.yml` | `resources/production/pre_check_workflow.yml` |
| `resources/uc_integration_test_workflow.yml` | `resources/integration_tests/uc_integration_test_workflow.yml` |
| `resources/hive_integration_test_workflow.yml` | `resources/integration_tests/hive_integration_test_workflow.yml` |
| `resources/negative_paths_integration_test_workflow.yml` | `resources/integration_tests/negative_paths_integration_test_workflow.yml` |

### Modified

| File | Change |
|---|---|
| `src/common/config.py:137-138, 236-237` | Remove `include_uc` / `include_hive` fields + parser entries |
| `src/migrate/orchestrator.py:196-197` (and 22, 114, 168 comments) | Remove `if not config.include_uc` short-circuit |
| `src/migrate/hive_orchestrator.py:50-60` | Remove `if not config.include_hive` short-circuit |
| `src/migrate/grants_worker.py:124-125` | Remove `if not config.include_uc` short-circuit |
| `src/migrate/hive_grants_worker.py:188-189` | Remove `if not config.include_hive` short-circuit |
| `src/migrate/setup_sharing.py:312-313` | Remove `if not config.include_uc` short-circuit |
| `src/discovery/discovery.py:500-517` | Replace dual-flag logic with unconditional UC + Hive scan; both always run |
| `src/migrate/summary.py` | Accept new `--object_types` parameter (comma-separated); filter `migration_status` by it before aggregating |
| `databricks.yml` | Update `include` paths for the moved resource files |
| `tests/unit/test_config.py` | Drop tests for removed scope flags |
| `tests/unit/test_discovery.py` | Drop scope-flag conditional tests; update for unconditional scan |
| `tests/unit/test_orchestrator.py` | Drop `include_uc=false` short-circuit tests |
| `tests/unit/test_hive_orchestrator.py` | Drop `include_hive=false` short-circuit tests |
| `tests/unit/test_grants_worker.py` | Drop `include_uc=false` skip test |
| `tests/unit/test_setup_sharing.py` | Drop `include_uc=false` short-circuit test |
| `tests/unit/test_summary.py` | Add tests for `--object_types` filter |
| `tests/integration/test_uc_end_to_end.py` | Move governance-only assertions (3.15, 3.17, 3.19, 3.21, 3.22, 3.24) → `test_governance_end_to_end.py` |
| `tests/integration/seed_uc_test_data.py` | Drop governance-only seed paths that move to `seed_governance_target_state.py` (if any are governance-pure) |
| `tests/integration/_config_override.py` | Drop `include_uc` / `include_hive` apply logic |
| `tests/integration/setup_test_config.py` | Drop scope-flag widgets + plumbing |
| `config.yaml`, `config.example.yaml` | Drop `scope:` block; update README pointer |
| `README.md` | Document 4-job operator flow + standalone-runnable contract + trust-the-operator pre-conditions |

### Deleted

| File | Reason |
|---|---|
| `resources/migrate_workflow.yml` | Replaced by 3 split files |

---

## Tasks

### Task 1: Branch + baseline tests

**Files**: branch only

- [ ] **Step 1: Verify Path A merged into main**

```bash
cd /Users/hari.selvarajan/uksouth_migration/workspace-migration
git fetch databricks-solutions main
git log databricks-solutions/main --oneline | head -5
```

Expected: see Path A commits at the top (`fix(cleanup_staging)`, `docs(README)`, etc.). If not, STOP and report — Path A must be merged before this plan runs.

- [ ] **Step 2: Create feature branch**

```bash
git checkout main
git pull databricks-solutions main
git checkout -b feat/workflow-split
```

- [ ] **Step 3: Run unit suite — establish baseline**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

Expected: ~760 passed (the post-Path-A count). Note exact number for later regression checking.

- [ ] **Step 4: Note baseline ruff state**

```bash
uv run ruff check src/ tests/ 2>&1 | tail -5
```

Note any pre-existing warnings to skip in Task 25.

---

### Task 2: Add `--object_types` parameter to `summary.py`

**Files:**
- Modify: `src/migrate/summary.py` (add parameter parsing + filter)
- Test: `tests/unit/test_summary.py`

Per-workflow summaries need to filter `migration_status` to their slice. Strategy: one `summary.py` notebook with a `--object_types` parameter (comma-separated), reused by all three per-workflow summary tasks.

- [ ] **Step 1: Read current summary.py to find filter insertion point**

```bash
sed -n '20,80p' src/migrate/summary.py
```

Identify where `migration_status` DataFrame is read (look for `tracker.read_migration_status()` or `spark.sql(...migration_status...)` near `aggregate_by_status` invocation).

- [ ] **Step 2: Write failing test for `--object_types` filter**

Append to `tests/unit/test_summary.py`:

```python
def test_aggregate_by_status_filters_by_object_types(spark_session_for_test):
    """summary's aggregate_by_status must only count rows whose object_type
    is in the supplied object_types list (per-workflow summary slicing)."""
    from migrate.summary import aggregate_by_status_filtered

    rows = [
        ("managed_table", "validated"),
        ("managed_table", "validated"),
        ("hive_external", "validated"),
        ("tag", "validated"),
        ("row_filter", "skipped"),
    ]
    schema = "object_type STRING, status STRING"
    df = spark_session_for_test.createDataFrame(rows, schema=schema)

    result = aggregate_by_status_filtered(df, object_types=["managed_table"])
    statuses = {r["status"]: r["total"] for r in result}
    assert statuses == {"validated": 2}

    result = aggregate_by_status_filtered(df, object_types=["tag", "row_filter"])
    statuses = {r["status"]: r["total"] for r in result}
    assert statuses == {"validated": 1, "skipped": 1}
```

- [ ] **Step 3: Run test, verify it fails**

```bash
uv run pytest tests/unit/test_summary.py::test_aggregate_by_status_filters_by_object_types -v
```

Expected: FAIL with `ImportError: cannot import name 'aggregate_by_status_filtered'`.

- [ ] **Step 4: Add the filtered helper to `summary.py`**

After the existing `aggregate_by_status` definition in `src/migrate/summary.py`:

```python
def aggregate_by_status_filtered(df: DataFrame, object_types: list[str]) -> list[dict]:
    """Same as aggregate_by_status, but pre-filters to rows whose object_type
    is in `object_types`. Used by per-workflow summary tasks.

    Empty object_types means no filter (return everything) — matches the
    pre-split behaviour for backwards compatibility.
    """
    from pyspark.sql.functions import col, count

    if object_types:
        df = df.filter(col("object_type").isin(object_types))
    rows = df.groupBy("status").agg(count("*").alias("total")).orderBy("status").collect()
    return [row.asDict() for row in rows]
```

- [ ] **Step 5: Verify the test passes**

```bash
uv run pytest tests/unit/test_summary.py::test_aggregate_by_status_filters_by_object_types -v
```

Expected: PASS.

- [ ] **Step 6: Wire `--object_types` parameter into `run()`**

Find `run()` in `summary.py`. Add parameter parsing at the top:

```python
def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    # Per-workflow filter (Task 2: workflow split).
    # Empty / missing → summarize everything (back-compat).
    object_types_param = ""
    try:
        object_types_param = dbutils.widgets.get("object_types")  # type: ignore[name-defined]  # noqa: F821
    except Exception:  # noqa: BLE001
        pass
    object_types = [t.strip() for t in object_types_param.split(",") if t.strip()]
    # ... existing body, but replace `aggregate_by_status(df)` with
    # `aggregate_by_status_filtered(df, object_types)`
```

- [ ] **Step 7: Add `dbutils.widgets.text(...)` declaration at the top of the notebook**

In `summary.py`'s second `# COMMAND ----------` cell:

```python
try:
    dbutils.widgets.text("object_types", "")  # type: ignore[name-defined]  # noqa: F821
except NameError:
    pass
```

- [ ] **Step 8: Run full unit suite — no regressions**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

Expected: 761 passed (760 + 1 new).

- [ ] **Step 9: Commit**

```bash
git add src/migrate/summary.py tests/unit/test_summary.py
git commit -m "$(cat <<'EOF'
feat(summary): add --object_types parameter for per-workflow filtering

Per-workflow summary tasks (summary_uc / summary_hive / summary_governance)
will pass their object-type slice via this parameter. Empty value
preserves current behaviour (summarise everything).

Co-authored-by: Isaac
EOF
)"
```

---

### Task 3: Create `pre_check_governance.py`

**Files:**
- Create: `src/pre_check/pre_check_governance.py`
- Create: `tests/unit/test_pre_check_governance.py`

The governance workflow runs standalone (Q1: trust the operator). `pre_check_governance` validates that `discovery_inventory` exists and is non-empty for governance object types. It does NOT validate target tables exist — the operator is trusted (per Q1 + README).

- [ ] **Step 1: Read existing pre_check.py for the shape**

```bash
cat src/pre_check/pre_check.py | head -60
```

Note the notebook header pattern, imports, `_is_notebook` helper, `run(dbutils, spark)` signature.

- [ ] **Step 2: Write failing tests**

Create `tests/unit/test_pre_check_governance.py`:

```python
"""Tests for src/pre_check/pre_check_governance.py."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch


@contextmanager
def _patch_config(config):
    with patch("common.config.MigrationConfig.from_workspace_file", return_value=config):
        yield


def test_pre_check_governance_passes_when_discovery_inventory_populated():
    """When discovery_inventory has rows for governance object types, pass."""
    from pre_check.pre_check_governance import run
    spark = MagicMock()
    config = MagicMock()
    config.tracking_catalog = "main"
    config.tracking_schema = "cp_migration"

    # Mock spark.sql to return a row count > 0
    spark.sql.return_value.collect.return_value = [MagicMock(c=42)]

    with _patch_config(config):
        run(MagicMock(), spark)

    # No exception raised → pass


def test_pre_check_governance_fails_when_discovery_inventory_empty():
    """When discovery_inventory has no rows for governance types, raise loud."""
    from pre_check.pre_check_governance import run
    spark = MagicMock()
    config = MagicMock()
    config.tracking_catalog = "main"
    config.tracking_schema = "cp_migration"
    spark.sql.return_value.collect.return_value = [MagicMock(c=0)]

    import pytest
    with _patch_config(config):
        with pytest.raises(RuntimeError) as excinfo:
            run(MagicMock(), spark)
    assert "discovery" in str(excinfo.value).lower()


def test_pre_check_governance_queries_governance_object_types():
    """The COUNT query must filter to governance object_types."""
    from pre_check.pre_check_governance import run
    spark = MagicMock()
    config = MagicMock()
    config.tracking_catalog = "main"
    config.tracking_schema = "cp_migration"
    spark.sql.return_value.collect.return_value = [MagicMock(c=10)]

    with _patch_config(config):
        run(MagicMock(), spark)

    sql = spark.sql.call_args.args[0]
    # Filter must include at least the governance types we care about
    for ot in ("tag", "row_filter", "column_mask", "customer_share"):
        assert f"'{ot}'" in sql, f"missing object_type {ot} in COUNT filter SQL"
```

- [ ] **Step 3: Run tests, verify they fail with ModuleNotFoundError**

```bash
uv run pytest tests/unit/test_pre_check_governance.py -v
```

Expected: 3 FAIL with `ModuleNotFoundError: No module named 'pre_check.pre_check_governance'`.

- [ ] **Step 4: Create `src/pre_check/pre_check_governance.py`**

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
# pre_check for the standalone migrate_governance workflow.
#
# Trust-the-operator contract (per workflow_split_design Q1): we do NOT
# validate that target tables exist. The operator is responsible for
# running migrate_uc + migrate_hive first. This pre-check ONLY validates
# that discovery_inventory has been populated with governance rows —
# i.e. the discovery job ran on this environment at some point.
#
# Failure modes this catches:
#   - Operator forgot to run discovery → discovery_inventory empty
#   - tracking_catalog/tracking_schema misconfigured → table doesn't exist
#   - Discovery ran but found no governance objects (row filters etc.)

import logging

from common.config import MigrationConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pre_check_governance")

GOVERNANCE_OBJECT_TYPES = (
    "tag",
    "comment",
    "row_filter",
    "column_mask",
    "customer_share",
    "policy",
    "monitor",
    "foreign_catalog",
)


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def run(dbutils, spark) -> None:  # noqa: ARG001
    config = MigrationConfig.from_workspace_file()

    types_in = ", ".join(f"'{t}'" for t in GOVERNANCE_OBJECT_TYPES)
    sql = (
        f"SELECT COUNT(*) AS c FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE object_type IN ({types_in})"
    )
    rows = spark.sql(sql).collect()
    n = rows[0].c if rows else 0
    if n == 0:
        raise RuntimeError(
            "pre_check_governance: discovery_inventory has zero rows for "
            f"governance object types ({GOVERNANCE_OBJECT_TYPES}). The "
            "discovery job must run before migrate_governance. If you "
            "expect governance objects to exist on the source workspace, "
            "verify discovery completed successfully and the source has "
            "tags / comments / row filters / column masks / customer "
            "shares to migrate."
        )
    logger.info("pre_check_governance: %d governance rows in discovery_inventory.", n)


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
```

- [ ] **Step 5: Verify tests pass**

```bash
uv run pytest tests/unit/test_pre_check_governance.py -v
```

Expected: 3 PASS.

- [ ] **Step 6: Run full unit suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

Expected: 764 passed.

- [ ] **Step 7: Commit**

```bash
git add src/pre_check/pre_check_governance.py tests/unit/test_pre_check_governance.py
git commit -m "$(cat <<'EOF'
feat(pre_check): add pre_check_governance for standalone governance workflow

Trust-the-operator contract: validates only that discovery_inventory has
governance rows. Does NOT validate target tables exist — operator is
responsible for running migrate_uc + migrate_hive first.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 4: Remove `include_uc` short-circuit from `orchestrator.py`

**Files:**
- Modify: `src/migrate/orchestrator.py:196-197` (plus comment cleanups at lines 22, 114, 168)
- Test: `tests/unit/test_orchestrator.py`

- [ ] **Step 1: Find affected tests**

```bash
grep -n "include_uc" tests/unit/test_orchestrator.py
```

Note the test names that exercise the `include_uc=false` short-circuit.

- [ ] **Step 2: Delete those tests**

In `tests/unit/test_orchestrator.py`, delete every test whose body asserts on the `include_uc=false → notebook.exit` behaviour. They cover code we're removing.

- [ ] **Step 3: Run the file's tests — expect failures only on the deleted tests**

```bash
uv run pytest tests/unit/test_orchestrator.py -v 2>&1 | tail -20
```

Expected: tests for the include_uc=false path are gone; everything else still passes.

- [ ] **Step 4: Remove the short-circuit from `src/migrate/orchestrator.py`**

In `orchestrator.py`, around lines 196-197:

```python
def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    if not config.include_uc:                                    # ← DELETE
        logger.info("Skipping UC orchestrator: scope.include_uc=false.")  # ← DELETE
        ...notebook.exit("skipped: include_uc=false")            # ← DELETE
    # remaining code
```

Delete those lines. Also clean up comments at lines 22, 114, 168 that reference `scope.include_uc` — rewrite to drop the conditional framing (just describe what the worker does).

- [ ] **Step 5: Run unit suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

Expected: green (count drops by however many tests were deleted in Step 2).

- [ ] **Step 6: Commit**

```bash
git add src/migrate/orchestrator.py tests/unit/test_orchestrator.py
git commit -m "$(cat <<'EOF'
refactor(orchestrator): remove scope.include_uc short-circuit

After workflow split, scope is gated by which job runs (migrate_uc /
migrate_hive / migrate_governance), not by config flags inside workers.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 5: Remove `include_hive` short-circuit from `hive_orchestrator.py`

**Files:**
- Modify: `src/migrate/hive_orchestrator.py:50-60`
- Test: `tests/unit/test_hive_orchestrator.py`

- [ ] **Step 1: Find affected tests**

```bash
grep -n "include_hive" tests/unit/test_hive_orchestrator.py
```

- [ ] **Step 2: Delete those tests**

Per Task 4 step 2 pattern.

- [ ] **Step 3: Remove the short-circuit from hive_orchestrator.py**

Lines 50-60 (the `if not config.include_hive: ... dbutils.notebook.exit(...)` block):

```python
# DELETE this block:
if not config.include_hive:
    logger.info("Skipping Hive orchestrator: scope.include_hive=false.")
    if dbutils is not None:
        dbutils.notebook.exit("skipped: include_hive=false")
    return
```

- [ ] **Step 4: Verify suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 5: Commit**

```bash
git add src/migrate/hive_orchestrator.py tests/unit/test_hive_orchestrator.py
git commit -m "refactor(hive_orchestrator): remove scope.include_hive short-circuit

Co-authored-by: Isaac"
```

---

### Task 6: Remove `include_uc` short-circuit from `grants_worker.py`

**Files:**
- Modify: `src/migrate/grants_worker.py:124-125`
- Test: `tests/unit/test_grants_worker.py`

- [ ] **Step 1: Find affected tests**

```bash
grep -n "include_uc" tests/unit/test_grants_worker.py
```

- [ ] **Step 2: Delete the include_uc=false skip test**

- [ ] **Step 3: Delete the short-circuit from grants_worker.py:124-125**

```python
# DELETE:
if not config.include_uc:
    logger.info("Skipping UC grants_worker: scope.include_uc=false.")
    return
```

- [ ] **Step 4: Run suite**

```bash
uv run pytest tests/unit/test_grants_worker.py -v 2>&1 | tail -10
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 5: Commit**

```bash
git add src/migrate/grants_worker.py tests/unit/test_grants_worker.py
git commit -m "refactor(grants_worker): remove scope.include_uc short-circuit

Co-authored-by: Isaac"
```

---

### Task 7: Remove `include_hive` short-circuit from `hive_grants_worker.py`

**Files:**
- Modify: `src/migrate/hive_grants_worker.py:188-189`
- Test: `tests/unit/test_hive_grants_worker.py` if it exists, else `tests/unit/test_hive_workers.py`

- [ ] **Step 1: Find tests**

```bash
grep -rn "include_hive" tests/unit/test_hive_*.py
```

- [ ] **Step 2: Delete the skip test**

- [ ] **Step 3: Delete the short-circuit from `hive_grants_worker.py:188-189`**

```python
# DELETE:
if not config.include_hive:
    logger.info("Skipping hive_grants_worker: scope.include_hive=false.")
    return
```

- [ ] **Step 4: Run suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 5: Commit**

```bash
git add src/migrate/hive_grants_worker.py tests/unit/
git commit -m "refactor(hive_grants_worker): remove scope.include_hive short-circuit

Co-authored-by: Isaac"
```

---

### Task 8: Remove `include_uc` short-circuit from `setup_sharing.py`

**Files:**
- Modify: `src/migrate/setup_sharing.py:312-313`
- Test: `tests/unit/test_setup_sharing.py`

- [ ] **Step 1: Find affected tests**

```bash
grep -n "include_uc" tests/unit/test_setup_sharing.py
```

- [ ] **Step 2: Delete the skip test**

- [ ] **Step 3: Delete the short-circuit from setup_sharing.py:312-313**

```python
# DELETE:
if not config.include_uc:
    logger.info("Skipping setup_sharing: scope.include_uc=false.")
    return
```

- [ ] **Step 4: Run suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 5: Commit**

```bash
git add src/migrate/setup_sharing.py tests/unit/test_setup_sharing.py
git commit -m "refactor(setup_sharing): remove scope.include_uc short-circuit

Co-authored-by: Isaac"
```

---

### Task 9: Make `discovery.py` unconditional (no scope flags)

**Files:**
- Modify: `src/discovery/discovery.py:500-517` (the dual-flag conditional)
- Test: `tests/unit/test_discovery.py`

After the workflow split, discovery is a shared upstream job that always scans both UC and Hive. Operators who don't have Hive simply have an empty Hive scan (info_schema returns no rows).

- [ ] **Step 1: Find affected tests**

```bash
grep -n "include_uc\|include_hive\|scope\.\|both flags false\|neither.*enabled" tests/unit/test_discovery.py | head -20
```

- [ ] **Step 2: Delete the scope-flag-conditional tests**

Delete tests like `test_skipped_when_both_flags_false`, `test_only_uc_when_include_hive_false`, etc.

- [ ] **Step 3: Update discovery.py:500-517**

Replace:

```python
if not (config.include_uc or config.include_hive):
    print("Neither scope.include_uc nor scope.include_hive is enabled — nothing to discover.")
    return
...
if config.include_uc:
    # uc scan
else:
    print("[uc] Skipped (scope.include_uc = false)")
if config.include_hive:
    # hive scan
else:
    print("[hive] Skipped (scope.include_hive = false)")
```

With:

```python
# Both scopes always scan — workflow split removed scope flags.
# Empty scan results (e.g., no Hive metastore) are normal and noop.
print("[uc] Scanning UC catalogs...")
# uc scan body (whatever the existing UC branch did)

print("[hive] Scanning hive_metastore...")
# hive scan body (whatever the existing Hive branch did)
```

(Read lines 500-517 carefully and inline the existing branch bodies into the unconditional flow.)

- [ ] **Step 4: Update the comment at line 23**

Change `# controlled via config.include_uc / config.include_hive.` to a plain description (no scope flags).

- [ ] **Step 5: Run discovery tests**

```bash
uv run pytest tests/unit/test_discovery.py -v 2>&1 | tail -20
```

Expected: pass.

- [ ] **Step 6: Run suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 7: Commit**

```bash
git add src/discovery/discovery.py tests/unit/test_discovery.py
git commit -m "$(cat <<'EOF'
refactor(discovery): unconditional UC + Hive scan

Remove scope.include_uc/include_hive conditional. Discovery is the
shared upstream job for the workflow split — always scans both
domains. Empty Hive metastore is a normal no-op.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 10: Remove `include_uc` / `include_hive` from `config.py`

**Files:**
- Modify: `src/common/config.py:137-138, 236-237`
- Test: `tests/unit/test_config.py`

- [ ] **Step 1: Find affected tests**

```bash
grep -n "include_uc\|include_hive\|scope" tests/unit/test_config.py | head -20
```

- [ ] **Step 2: Delete those tests**

Drop tests asserting scope flag round-trip / defaults.

- [ ] **Step 3: Remove fields from `MigrationConfig`**

Edit `src/common/config.py` lines 137-138:

```python
# DELETE:
include_uc: bool = True
include_hive: bool = False
```

And lines 236-237 in the parser:

```python
# DELETE:
include_uc=_coerce_bool((raw.get("scope") or {}).get("include_uc", True)),
include_hive=_coerce_bool((raw.get("scope") or {}).get("include_hive", False)),
```

- [ ] **Step 4: Run suite — likely many failures from removed fields still being referenced**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -10
```

For each `AttributeError: ... has no attribute 'include_uc'` / `'include_hive'`:
- If the test is testing removed code, delete the test.
- If a test fixture sets `include_uc=...`, drop that line from the fixture.

Iterate until green.

- [ ] **Step 5: Update `config.yaml` and `config.example.yaml`** to drop the `scope:` block

In both files, find:

```yaml
scope:
  include_uc: true
  include_hive: false
```

Delete that block + its preceding comment.

- [ ] **Step 6: Run suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 7: Commit**

```bash
git add src/common/config.py config.yaml config.example.yaml tests/unit/test_config.py
git commit -m "$(cat <<'EOF'
refactor(config): remove scope.include_uc / scope.include_hive

Workflow split: scope is gated by which job runs, not by config flags.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 11: Drop scope-flag plumbing from integration test helpers

**Files:**
- Modify: `tests/integration/_config_override.py` (drop include_uc / include_hive params)
- Modify: `tests/integration/setup_test_config.py` (drop scope-flag widgets)
- Modify: `tests/unit/test_setup_test_config.py` (drop tests that exercise the flags)

- [ ] **Step 1: Locate references**

```bash
grep -n "include_uc\|include_hive" tests/integration/_config_override.py tests/integration/setup_test_config.py tests/unit/test_setup_test_config.py
```

- [ ] **Step 2: Drop from `_config_override.py`**

Remove `include_uc=...` / `include_hive=...` parameters and their apply branches.

- [ ] **Step 3: Drop widgets from `setup_test_config.py`**

Find `dbutils.widgets.text("include_uc", ...)` / `dbutils.widgets.text("include_hive", ...)` and the read-then-apply lines. Remove all.

- [ ] **Step 4: Update unit tests in `test_setup_test_config.py`**

For each `UC_OVERRIDES` / `HIVE_OVERRIDES` / `NEG_*_OVERRIDES` dict that has `include_uc=True` or `include_hive=True/False`, remove those keys.

Drop any test whose sole purpose is asserting the flag plumbing (e.g., `test_uc_overrides_sets_include_uc`).

- [ ] **Step 5: Run unit suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 6: Commit**

```bash
git add tests/
git commit -m "$(cat <<'EOF'
test: drop scope.include_uc / scope.include_hive from integration helpers

Co-authored-by: Isaac
EOF
)"
```

---

### Task 12: Move resource files into subdirs

**Files:**
- `git mv resources/discovery_workflow.yml resources/production/discovery_workflow.yml`
- `git mv resources/pre_check_workflow.yml resources/production/pre_check_workflow.yml`
- `git mv resources/uc_integration_test_workflow.yml resources/integration_tests/uc_integration_test_workflow.yml`
- `git mv resources/hive_integration_test_workflow.yml resources/integration_tests/hive_integration_test_workflow.yml`
- `git mv resources/negative_paths_integration_test_workflow.yml resources/integration_tests/negative_paths_integration_test_workflow.yml`
- Modify: `databricks.yml` (update `include` paths)

- [ ] **Step 1: Create the subdirs and move files**

```bash
mkdir -p resources/production resources/integration_tests
git mv resources/discovery_workflow.yml resources/production/discovery_workflow.yml
git mv resources/pre_check_workflow.yml resources/production/pre_check_workflow.yml
git mv resources/uc_integration_test_workflow.yml resources/integration_tests/uc_integration_test_workflow.yml
git mv resources/hive_integration_test_workflow.yml resources/integration_tests/hive_integration_test_workflow.yml
git mv resources/negative_paths_integration_test_workflow.yml resources/integration_tests/negative_paths_integration_test_workflow.yml
```

- [ ] **Step 2: Update `databricks.yml` includes**

Find the existing `include:` block:

```bash
grep -A5 "^include:" databricks.yml
```

If it lists individual files, replace with glob:

```yaml
include:
  - resources/*.yml
  - resources/production/*.yml
  - resources/integration_tests/*.yml
```

If it's already a glob like `resources/*.yml`, expand to also cover the subdirs.

- [ ] **Step 3: Validate bundle**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -10
```

Expected: validation passes. (If it errors on a missing reference inside `migrate_workflow.yml`, that's expected — we delete it in Task 21. For now, commenting out includes for that file is OK if needed; we want subdir files to be findable.)

- [ ] **Step 4: Run unit suite (verify nothing imports from old paths)**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 5: Commit**

```bash
git add resources/ databricks.yml
git commit -m "$(cat <<'EOF'
chore(resources): organize workflow YAMLs into production/ + integration_tests/

Per workflow split design D5: 4 + 4 file layout for clarity.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 13: Extend `discovery_workflow.yml` with summary task

**Files:**
- Modify: `resources/production/discovery_workflow.yml`

D4: discovery summary reports counts only (rows-discovered by object_type), not migration_status state.

- [ ] **Step 1: Read current discovery_workflow.yml**

```bash
cat resources/production/discovery_workflow.yml
```

- [ ] **Step 2: Add a `summary` task that runs the existing summary.py with `--object_types=""` (everything)**

Edit the file to add a second task after `discovery`:

```yaml
resources:
  jobs:
    discovery:
      name: "${var.job_prefix}-discovery"
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: discovery
          notebook_task:
            notebook_path: ../../src/discovery/discovery.py

        - task_key: summary
          depends_on:
            - task_key: discovery
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../../src/migrate/summary.py
            base_parameters:
              # D4: discovery's summary is counts-only across all object types
              # in discovery_inventory; not migration_status state.
              object_types: ""
              source_table: "discovery_inventory"
```

NOTE: paths are now `../../src/...` because the YAML moved one level deeper.

If the existing summary.py only reads `migration_status`, we'll need it to support `source_table=discovery_inventory`. That extension can be done as a follow-up step; for v1, accept that discovery's summary is a no-op or reads migration_status (operator gets value mainly from `migrate_*` summaries). To keep this task crisp, **drop the summary task from discovery for now** and revisit in a follow-up if needed:

```yaml
# Final version for v1: no summary on discovery — operator can SHOW TABLES
# in discovery_inventory directly. Summary tasks only on migrate_*.
```

Use this simpler v1 (just keep the discovery task as-is). The plan delivers per-workflow summary on the three migrate_* jobs in Tasks 14-16; discovery summary is out of scope.

- [ ] **Step 3: No-op commit if file unchanged**

If the file wasn't modified, skip the commit. Otherwise:

```bash
git add resources/production/discovery_workflow.yml
git commit -m "chore(discovery): adjust notebook_path for resources/production/ depth

Co-authored-by: Isaac"
```

---

### Task 14: Create `migrate_uc_workflow.yml`

**Files:**
- Create: `resources/production/migrate_uc_workflow.yml`

UC chain: `setup_sharing → orchestrator → managed_tables/external_tables/views/volumes/models → migrate_grants (UC) → cleanup_staging → summary_uc`. Plus pipelines/streaming_tables/mv_st/online_tables (skipped or hard-excluded).

- [ ] **Step 1: Read the current UC chain in migrate_workflow.yml**

```bash
sed -n '1,200p' resources/migrate_workflow.yml
```

Map each task. The UC chain tasks are: setup_sharing, orchestrator, migrate_managed_tables, migrate_external_tables, migrate_volumes, migrate_functions, migrate_views, migrate_mv_st, migrate_st, migrate_grants (UC), cleanup_staging, migrate_models, migrate_online_tables.

NOTE: D1=b → migrate_grants (UC) STAYS in this workflow.

- [ ] **Step 2: Create `resources/production/migrate_uc_workflow.yml`**

Copy the relevant tasks from `migrate_workflow.yml` lines 1-195 (UC chain, ending before `# --- Hive migration chain ---`). Adjust paths to `../../src/...`. Add a final `summary_uc` task.

```yaml
resources:
  jobs:
    migrate_uc:
      name: "${var.job_prefix}-migrate-uc"
      description: |
        UC data plane migration: managed/external/views/volumes/models +
        UC grants. Depends operationally on the discovery job having
        populated discovery_inventory beforehand. Run discovery first.
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: setup_sharing
          notebook_task:
            notebook_path: ../../src/migrate/setup_sharing.py

        - task_key: orchestrator
          depends_on:
            - task_key: setup_sharing
          notebook_task:
            notebook_path: ../../src/migrate/orchestrator.py

        - task_key: migrate_managed_tables
          depends_on:
            - task_key: orchestrator
          for_each_task:
            inputs: "{{tasks.orchestrator.values.managed_table_batches}}"
            concurrency: 8
            task:
              task_key: managed_table_batch
              notebook_task:
                notebook_path: ../../src/migrate/managed_table_worker.py
                base_parameters:
                  batch: "{{input}}"

        - task_key: migrate_external_tables
          depends_on:
            - task_key: orchestrator
          for_each_task:
            inputs: "{{tasks.orchestrator.values.external_table_batches}}"
            concurrency: 8
            task:
              task_key: external_table_batch
              notebook_task:
                notebook_path: ../../src/migrate/external_table_worker.py
                base_parameters:
                  batch: "{{input}}"

        - task_key: migrate_volumes
          depends_on:
            - task_key: orchestrator
          for_each_task:
            inputs: "{{tasks.orchestrator.values.volume_batches}}"
            concurrency: 8
            task:
              task_key: volume_batch
              notebook_task:
                notebook_path: ../../src/migrate/volume_worker.py
                base_parameters:
                  batch: "{{input}}"

        - task_key: migrate_functions
          depends_on:
            - task_key: migrate_managed_tables
            - task_key: migrate_external_tables
          notebook_task:
            notebook_path: ../../src/migrate/functions_worker.py

        - task_key: migrate_views
          depends_on:
            - task_key: migrate_managed_tables
            - task_key: migrate_external_tables
            - task_key: migrate_functions
          notebook_task:
            notebook_path: ../../src/migrate/views_worker.py

        - task_key: migrate_mv_st
          depends_on:
            - task_key: migrate_views
          for_each_task:
            inputs: "{{tasks.orchestrator.values.mv_batches}}"
            concurrency: 4
            task:
              task_key: mv_batch
              notebook_task:
                notebook_path: ../../src/migrate/mv_st_worker.py
                base_parameters:
                  batch: "{{input}}"

        - task_key: migrate_st
          depends_on:
            - task_key: migrate_views
          for_each_task:
            inputs: "{{tasks.orchestrator.values.st_batches}}"
            concurrency: 4
            task:
              task_key: st_batch
              notebook_task:
                notebook_path: ../../src/migrate/mv_st_worker.py
                base_parameters:
                  batch: "{{input}}"

        - task_key: migrate_grants
          depends_on:
            - task_key: migrate_views
            - task_key: migrate_volumes
            - task_key: migrate_mv_st
            - task_key: migrate_st
          notebook_task:
            notebook_path: ../../src/migrate/grants_worker.py

        - task_key: migrate_models
          depends_on:
            - task_key: migrate_grants
          notebook_task:
            notebook_path: ../../src/migrate/models_worker.py

        - task_key: migrate_online_tables
          depends_on:
            - task_key: migrate_grants
          notebook_task:
            notebook_path: ../../src/migrate/online_tables_worker.py

        - task_key: cleanup_staging
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../../src/migrate/cleanup_staging.py
          depends_on:
            - task_key: migrate_managed_tables
            - task_key: migrate_external_tables
            - task_key: migrate_views
            - task_key: migrate_volumes
            - task_key: migrate_models
            - task_key: migrate_grants

        - task_key: summary_uc
          run_if: ALL_DONE
          depends_on:
            - task_key: migrate_models
            - task_key: migrate_online_tables
            - task_key: cleanup_staging
          notebook_task:
            notebook_path: ../../src/migrate/summary.py
            base_parameters:
              # Per-workflow filter: UC-only object types.
              object_types: "managed_table,external_table,view,volume,model,mv_st,st,grant,online_table"
```

VERIFY paths: each notebook_path should resolve from the YAML location to `src/...`. With YAML at `resources/production/`, `../../src/...` resolves to repo root `src/...`. Correct.

VERIFY object_types list against `aggregate_by_object_type` outputs in `summary.py` — the actual `object_type` values written by workers. Check by:

```bash
grep -h "object_type.*=.*\"" src/migrate/*.py | grep -oE '"[a-z_]+"' | sort -u
```

Adjust the comma-separated list in `summary_uc.base_parameters.object_types` to match the actual values UC workers emit (`managed_table`, `external_table`, `view`, `volume`, `registered_model`, `mv_st`, `streaming_table`, `grant`, `online_table` — names may differ).

- [ ] **Step 3: Validate bundle**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -10
```

If validation passes, the YAML is structurally OK. If it errors on a name conflict ("two jobs both named 'migrate'"), that's because the OLD `migrate_workflow.yml` still defines `migrate` — that gets resolved when we delete it in Task 21. Workaround: temporarily rename the job in the OLD file to `migrate_old` or comment its tasks out for now (revert after Task 21).

- [ ] **Step 4: Run unit suite (no-op for YAML changes)**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 5: Commit**

```bash
git add resources/production/migrate_uc_workflow.yml
git commit -m "$(cat <<'EOF'
feat(workflow): add migrate_uc_workflow.yml

UC data plane migration: setup_sharing → orchestrator → tables/views/
volumes/models → grants → cleanup_staging → summary_uc. Customer shares
+ tags + RLS + masks + comments + monitors + foreign_catalogs +
policies move to migrate_governance per design D1=b/D2=a.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 15: Create `migrate_hive_workflow.yml`

**Files:**
- Create: `resources/production/migrate_hive_workflow.yml`

Hive chain: `hive_orchestrator → hive_external_tables/managed_nondbfs/managed_dbfs → hive_functions → hive_views → migrate_hive_grants → summary_hive`.

- [ ] **Step 1: Read the Hive chain in migrate_workflow.yml**

```bash
sed -n '195,260p' resources/migrate_workflow.yml
```

- [ ] **Step 2: Create `resources/production/migrate_hive_workflow.yml`**

```yaml
resources:
  jobs:
    migrate_hive:
      name: "${var.job_prefix}-migrate-hive"
      description: |
        Hive (legacy) data plane migration: external/managed-nondbfs/
        managed-dbfs tables + Hive functions/views + Hive grants
        (replayed as UC grants on target catalog). Depends operationally
        on the discovery job; run discovery first.
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: hive_orchestrator
          notebook_task:
            notebook_path: ../../src/migrate/hive_orchestrator.py

        - task_key: migrate_hive_external_tables
          depends_on:
            - task_key: hive_orchestrator
          for_each_task:
            inputs: "{{tasks.hive_orchestrator.values.hive_external_batches}}"
            concurrency: 4
            task:
              task_key: hive_external_batch
              notebook_task:
                notebook_path: ../../src/migrate/hive_external_worker.py
                base_parameters:
                  batch: "{{input}}"

        - task_key: migrate_hive_managed_nondbfs
          depends_on:
            - task_key: hive_orchestrator
          for_each_task:
            inputs: "{{tasks.hive_orchestrator.values.hive_managed_nondbfs_batches}}"
            concurrency: 4
            task:
              task_key: hive_managed_nondbfs_batch
              notebook_task:
                notebook_path: ../../src/migrate/hive_managed_nondbfs_worker.py
                base_parameters:
                  batch: "{{input}}"

        - task_key: migrate_hive_managed_dbfs
          depends_on:
            - task_key: hive_orchestrator
          for_each_task:
            inputs: "{{tasks.hive_orchestrator.values.hive_managed_dbfs_batches}}"
            concurrency: 4
            task:
              task_key: hive_managed_dbfs_batch
              notebook_task:
                notebook_path: ../../src/migrate/hive_managed_dbfs_worker.py
                base_parameters:
                  batch: "{{input}}"

        - task_key: migrate_hive_functions
          depends_on:
            - task_key: migrate_hive_external_tables
            - task_key: migrate_hive_managed_nondbfs
            - task_key: migrate_hive_managed_dbfs
          notebook_task:
            notebook_path: ../../src/migrate/hive_functions_worker.py

        - task_key: migrate_hive_views
          depends_on:
            - task_key: migrate_hive_external_tables
            - task_key: migrate_hive_managed_nondbfs
            - task_key: migrate_hive_managed_dbfs
            - task_key: migrate_hive_functions
          notebook_task:
            notebook_path: ../../src/migrate/hive_views_worker.py

        - task_key: migrate_hive_grants
          depends_on:
            - task_key: migrate_hive_views
          notebook_task:
            notebook_path: ../../src/migrate/hive_grants_worker.py

        - task_key: summary_hive
          run_if: ALL_DONE
          depends_on:
            - task_key: migrate_hive_grants
          notebook_task:
            notebook_path: ../../src/migrate/summary.py
            base_parameters:
              object_types: "hive_external,hive_managed_nondbfs,hive_managed_dbfs,hive_function,hive_view,hive_grant"
```

Adjust the `object_types` list per actual values workers emit (`grep -h "object_type.*=" src/migrate/hive_*.py`).

- [ ] **Step 3: Validate**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -10
```

- [ ] **Step 4: Commit**

```bash
git add resources/production/migrate_hive_workflow.yml
git commit -m "$(cat <<'EOF'
feat(workflow): add migrate_hive_workflow.yml

Hive legacy data plane migration: external/managed tables → functions
→ views → Hive grants (replayed as UC grants on target catalog). Each
data workflow self-contained for ACL per design D1=b.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 16: Create `migrate_governance_workflow.yml`

**Files:**
- Create: `resources/production/migrate_governance_workflow.yml`

Governance chain: `pre_check_governance → tags → comments → row_filters → column_masks → customer_shares → policies (after tags) → monitors → foreign_catalogs → migrate_sharing → summary_governance`.

- [ ] **Step 1: Read the governance tasks in migrate_workflow.yml**

```bash
sed -n '120,195p' resources/migrate_workflow.yml
```

These tasks have `depends_on: migrate_grants`. After the split, they live in `migrate_governance` which has its own pre-check (no migrate_grants dependency at the YAML level — the standalone-runnable contract per Q1).

- [ ] **Step 2: Create `resources/production/migrate_governance_workflow.yml`**

```yaml
resources:
  jobs:
    migrate_governance:
      name: "${var.job_prefix}-migrate-governance"
      description: |
        Fine-grained governance migration: tags, comments, row filters,
        column masks, customer-defined shares, policies, monitors,
        foreign catalogs. Trust-the-operator: target catalog/schema/table
        objects must already exist (run migrate_uc + migrate_hive first).
        pre_check_governance validates only that discovery_inventory has
        governance rows. Standalone-runnable for governance refresh.
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: pre_check_governance
          notebook_task:
            notebook_path: ../../src/pre_check/pre_check_governance.py

        - task_key: migrate_tags
          depends_on:
            - task_key: pre_check_governance
          notebook_task:
            notebook_path: ../../src/migrate/tags_worker.py

        - task_key: migrate_row_filters
          depends_on:
            - task_key: pre_check_governance
          notebook_task:
            notebook_path: ../../src/migrate/row_filters_worker.py

        - task_key: migrate_column_masks
          depends_on:
            - task_key: pre_check_governance
          notebook_task:
            notebook_path: ../../src/migrate/column_masks_worker.py

        - task_key: migrate_policies
          depends_on:
            - task_key: migrate_tags
          notebook_task:
            notebook_path: ../../src/migrate/policies_worker.py

        - task_key: migrate_comments
          depends_on:
            - task_key: pre_check_governance
          notebook_task:
            notebook_path: ../../src/migrate/comments_worker.py

        - task_key: migrate_monitors
          depends_on:
            - task_key: pre_check_governance
          notebook_task:
            notebook_path: ../../src/migrate/monitors_worker.py

        - task_key: migrate_foreign_catalogs
          depends_on:
            - task_key: pre_check_governance
          notebook_task:
            notebook_path: ../../src/migrate/foreign_catalogs_worker.py

        - task_key: migrate_sharing
          depends_on:
            - task_key: pre_check_governance
          notebook_task:
            notebook_path: ../../src/migrate/sharing_worker.py

        - task_key: summary_governance
          run_if: ALL_DONE
          depends_on:
            - task_key: migrate_tags
            - task_key: migrate_row_filters
            - task_key: migrate_column_masks
            - task_key: migrate_policies
            - task_key: migrate_comments
            - task_key: migrate_monitors
            - task_key: migrate_foreign_catalogs
            - task_key: migrate_sharing
          notebook_task:
            notebook_path: ../../src/migrate/summary.py
            base_parameters:
              object_types: "tag,comment,row_filter,column_mask,policy,monitor,foreign_catalog,customer_share"
```

- [ ] **Step 3: Validate bundle**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -10
```

- [ ] **Step 4: Commit**

```bash
git add resources/production/migrate_governance_workflow.yml
git commit -m "$(cat <<'EOF'
feat(workflow): add migrate_governance_workflow.yml

Fine-grained governance: tags, comments, RLS, column masks, customer
shares, policies, monitors, foreign catalogs. Standalone-runnable;
trust-the-operator contract for target objects.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 17: Move governance assertions out of `test_uc_end_to_end.py`

**Files:**
- Modify: `tests/integration/test_uc_end_to_end.py` (remove governance-only assertion blocks)
- Create: `tests/integration/test_governance_end_to_end.py` (new file with relocated assertions)
- Create: `tests/integration/seed_governance_target_state.py` (D3: pre-seed target via direct SQL)

- [ ] **Step 1: Find the governance assertions in test_uc_end_to_end.py**

```bash
grep -n "3\.15\|3\.17\|3\.19\|3\.21\|3\.22\|3\.24\|tag\|row_filter\|column_mask\|customer_share" tests/integration/test_uc_end_to_end.py | head -40
```

Identify the line ranges of:
- 3.15 (tags)
- 3.17 (comments)
- 3.19 (registered model + version + alias — actually this is UC, KEEP IT)
- 3.21 / 3.22 (connection / foreign_catalog — keep depends-on, governance assertion in new file)
- 3.24 (customer share — F.1 intermittent — moves to governance test)

Be precise: 3.19 is a UC item per the backlog `Phase 3 integration backfill` table, so it stays in test_uc_end_to_end.

- [ ] **Step 2: Create `tests/integration/test_governance_end_to_end.py`** with the moved assertions

Use the SAME structural pattern as `test_uc_end_to_end.py`: notebook header, sys.path bootstrap, fixture variable names (`config`, `spark`, `dbutils`, `error_messages`, `full_status`), and the same gating idiom. Copy the assertion blocks verbatim from `test_uc_end_to_end.py`.

(Detailed code: Step 4 in this task.)

- [ ] **Step 3: Create `tests/integration/seed_governance_target_state.py`**

Pre-seeds the target workspace with catalogs/schemas/tables/columns/views that the governance workers will attach to. No data migration — just CREATE TABLE / CREATE VIEW with the right shape.

```python
# Databricks notebook source

# COMMAND ----------

# Pre-seed the target workspace with catalogs / schemas / tables / views
# that mirror the governance-test fixtures. Used by
# governance_integration_test_workflow.yml so the governance test runs
# standalone without depending on uc_integration_test (D3).
#
# Idempotent: every CREATE uses IF NOT EXISTS.

import sys

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass

# COMMAND ----------

from common.auth import AuthManager
from common.config import MigrationConfig

config = MigrationConfig.from_workspace_file()
auth = AuthManager(config, dbutils)  # type: ignore[name-defined]  # noqa: F821
target = auth.target_client

# Use the SQL execution API to issue CREATE TABLE / VIEW on target.
def _sql_on_target(stmt: str) -> None:
    res = target.statement_execution.execute_statement(
        warehouse_id=config.target_warehouse_id,
        statement=stmt,
        wait_timeout="30s",
    )
    state = res.status.state.value if res.status and res.status.state else "unknown"
    if state != "SUCCEEDED":
        raise RuntimeError(f"target seed failed for: {stmt!r} (state={state})")

# Seed: integration_test_src.test_schema.tagged_table  (target side)
_sql_on_target("CREATE CATALOG IF NOT EXISTS integration_test_src")
_sql_on_target("CREATE SCHEMA IF NOT EXISTS integration_test_src.test_schema")
_sql_on_target("""
    CREATE TABLE IF NOT EXISTS integration_test_src.test_schema.tagged_table (
        id INT,
        name STRING,
        sensitive_data STRING
    ) USING DELTA
""")
# ... add tables/views matching the fixture rows in seed_uc_test_data.py
# that the governance test will attach to.
```

NOTE: the exact list of tables/schemas to seed depends on what the governance test expects. Read `seed_uc_test_data.py` for the source-side fixture and mirror only the data-plane shapes (no data, no RLS/CM functions — those come from migrate_governance).

- [ ] **Step 4: Move the assertion blocks**

In `test_governance_end_to_end.py`, paste:
- 3.15 catalog/schema/volume tags assertion block
- 3.17 column + volume comments assertion block
- 3.21 / 3.22 connection + foreign_catalog assertion blocks (the governance attachment, not the UC link)
- 3.24 customer-defined share assertion block (F.1 intermittent — keep with all known caveats from PR #31/#33 in comments)

In `test_uc_end_to_end.py`, delete those same blocks. Keep all other assertions (P.1-P.4 staging-copy, the L60 row-count-base block, 3.19 registered model, 2.5.x Iceberg, etc.).

- [ ] **Step 5: Run unit suite (test_*_end_to_end.py is not run by pytest unit; verifies syntax)**

```bash
uv run python -c "import ast; ast.parse(open('tests/integration/test_governance_end_to_end.py').read()); ast.parse(open('tests/integration/test_uc_end_to_end.py').read()); ast.parse(open('tests/integration/seed_governance_target_state.py').read())"
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 6: Commit**

```bash
git add tests/integration/test_governance_end_to_end.py tests/integration/test_uc_end_to_end.py tests/integration/seed_governance_target_state.py
git commit -m "$(cat <<'EOF'
test: split governance assertions into test_governance_end_to_end.py

Moves 3.15, 3.17, 3.21, 3.22, 3.24 (governance-pure items) from
test_uc_end_to_end into the new governance test. UC test retains UC
data + UC grants + 3.19 model assertions. seed_governance_target_state
pre-seeds target via direct SQL (D3) so governance test runs
standalone without depending on uc_integration_test.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 18: Create `governance_integration_test_workflow.yml`

**Files:**
- Create: `resources/integration_tests/governance_integration_test_workflow.yml`

Pattern: setup_test_config → seed_governance_target_state → discovery (limited to source RLS/CM seed) → migrate_governance → test_governance → teardown_governance.

- [ ] **Step 1: Read uc_integration_test_workflow.yml for the structural pattern**

```bash
cat resources/integration_tests/uc_integration_test_workflow.yml
```

- [ ] **Step 2: Create `resources/integration_tests/governance_integration_test_workflow.yml`**

```yaml
resources:
  jobs:
    governance_integration_test:
      name: "${var.job_prefix}-governance-integration-test"
      description: |
        Governance workflow integration test: pre-seeds target via direct
        SQL (no UC test dependency), seeds source RLS/CM/tag fixtures,
        runs migrate_governance only, asserts governance state.
      run_as:
        service_principal_name: ${var.migration_spn_id}
      tasks:
        - task_key: setup_test_config
          notebook_task:
            notebook_path: ../../tests/integration/setup_test_config.py
            base_parameters:
              # Path A staging_copy is enabled because some governance
              # tests exercise tables that have RLS/CM. include_uc / include_hive
              # flags removed (workflow split — Task 11).
              rls_cm_strategy: "staging_copy"

        - task_key: seed_uc
          depends_on:
            - task_key: setup_test_config
          notebook_task:
            notebook_path: ../../tests/integration/seed_uc_test_data.py

        - task_key: seed_governance_target
          depends_on:
            - task_key: seed_uc
          notebook_task:
            notebook_path: ../../tests/integration/seed_governance_target_state.py

        - task_key: discovery
          depends_on:
            - task_key: seed_governance_target
          notebook_task:
            notebook_path: ../../src/discovery/discovery.py

        - task_key: migrate_governance
          depends_on:
            - task_key: discovery
          run_job_task:
            job_id: "${resources.jobs.migrate_governance.id}"

        - task_key: test_governance
          depends_on:
            - task_key: migrate_governance
          notebook_task:
            notebook_path: ../../tests/integration/test_governance_end_to_end.py

        - task_key: teardown_governance
          run_if: ALL_DONE
          depends_on:
            - task_key: test_governance
          notebook_task:
            notebook_path: ../../tests/integration/teardown_uc.py
```

NOTE: The `run_job_task` reference uses `${resources.jobs.migrate_governance.id}` to reference the production job defined in `migrate_governance_workflow.yml`. DAB resolves cross-resource refs at deploy time. If this fails to validate, fall back to inlining the migrate_governance tasks directly into this test workflow (more verbose but unambiguous).

- [ ] **Step 3: Validate**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -10
```

If `run_job_task` cross-ref fails, fall back: copy the entire `migrate_governance` task chain inline (drop the `run_job_task` and add tasks for `pre_check_governance`, `migrate_tags`, etc., setting their `depends_on` to chain off `discovery` instead of off `pre_check_governance` — the test workflow has its own pre-check via `seed_governance_target`).

- [ ] **Step 4: Commit**

```bash
git add resources/integration_tests/governance_integration_test_workflow.yml
git commit -m "$(cat <<'EOF'
test(integration): add governance_integration_test_workflow.yml

Mirrors production migrate_governance with direct-SQL target pre-seed
(D3). Standalone — does not depend on uc_integration_test. Run-job-task
reference to migrate_governance for code reuse.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 19: Update existing test workflows for split

**Files:**
- Modify: `resources/integration_tests/uc_integration_test_workflow.yml` (drop scope-flag params, governance assertions delegated to new file)
- Modify: `resources/integration_tests/hive_integration_test_workflow.yml` (drop scope-flag params)
- Modify: `resources/integration_tests/negative_paths_integration_test_workflow.yml` (drop scope-flag widget params)

- [ ] **Step 1: Drop `include_uc` / `include_hive` from `uc_integration_test_workflow.yml`**

Find lines that pass these as base_parameters to `setup_test_config`. Delete them.

- [ ] **Step 2: Drop the same from `hive_integration_test_workflow.yml`**

- [ ] **Step 3: Drop the same from `negative_paths_integration_test_workflow.yml`**

- [ ] **Step 4: Update the path adjustments — these YAMLs moved one level deeper**

For each test workflow YAML, find every `notebook_path: ../src/...` and `notebook_path: ../tests/...`. Replace with `../../src/...` and `../../tests/...`.

- [ ] **Step 5: Validate**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -10
```

- [ ] **Step 6: Run unit suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 7: Commit**

```bash
git add resources/integration_tests/
git commit -m "$(cat <<'EOF'
test(integration): drop scope flags + adjust paths for subdir move

Co-authored-by: Isaac
EOF
)"
```

---

### Task 20: Update README + config docs for the workflow split

**Files:**
- Modify: `README.md` (operator flow, standalone-runnable contract, pre-conditions)
- Modify: `config.example.yaml` (drop scope block + comment)

- [ ] **Step 1: Find scope-flag references in README**

```bash
grep -n "include_uc\|include_hive\|scope\.\|migrate_workflow" README.md
```

- [ ] **Step 2: Rewrite the operator-flow section**

Replace `migrate_workflow` references with the 4-job model. Add a "Standalone-runnable workflows" subsection. Sample text:

```markdown
## Operator flow

The migration tool ships four production jobs:

1. **`discovery`** — scans the source workspace and writes `discovery_inventory`. Run this first; the migrate_* jobs depend on it operationally.
2. **`migrate_uc`** — UC data plane (managed/external/views/volumes/models) + UC grants + cleanup_staging.
3. **`migrate_hive`** — Hive (legacy) data plane + Hive ACLs replayed as UC grants on the target catalog.
4. **`migrate_governance`** — fine-grained governance: tags, comments, row filters, column masks, customer shares, policies, monitors, foreign catalogs.

Each `migrate_*` job is independent. Operators run them in any order; the jobs assume `discovery_inventory` has been populated.

### Standalone-runnable contract

`migrate_governance` runs standalone (per design Q1). It assumes target catalog/schema/table objects exist. **Do NOT** run `migrate_governance` against an empty target — it will write governance for objects that don't exist.

### Pre-conditions

For staging_copy strategy (Path A — recommended): see `Row filter / column mask on managed tables` section below.
```

- [ ] **Step 3: Drop the `scope:` block from `config.example.yaml`**

```bash
grep -n "^scope:\|include_uc\|include_hive" config.example.yaml
```

Delete the scope block and its preceding comment.

- [ ] **Step 4: Run unit suite (sanity)**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 5: Commit**

```bash
git add README.md config.example.yaml
git commit -m "$(cat <<'EOF'
docs: document 4-job operator flow + drop scope flag references

Co-authored-by: Isaac
EOF
)"
```

---

### Task 21: Delete `migrate_workflow.yml`

**Files:**
- Delete: `resources/migrate_workflow.yml`

This is the irreversible cutover.

- [ ] **Step 1: Verify the four production jobs exist**

```bash
ls resources/production/
```

Expected: `discovery_workflow.yml`, `migrate_uc_workflow.yml`, `migrate_hive_workflow.yml`, `migrate_governance_workflow.yml`, `pre_check_workflow.yml`.

- [ ] **Step 2: Delete the old file**

```bash
git rm resources/migrate_workflow.yml
```

- [ ] **Step 3: Validate**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -10
```

Expected: validation passes; no missing references.

- [ ] **Step 4: Run unit suite**

```bash
uv run pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 5: Commit**

```bash
git commit -m "$(cat <<'EOF'
refactor: delete monolithic migrate_workflow.yml

Replaced by migrate_uc / migrate_hive / migrate_governance per the
workflow split. Hard cutover (Q8=a).

Co-authored-by: Isaac
EOF
)"
```

---

### Task 22: Final unit-test sweep + ruff/mypy

**Files:** none — verification only

- [ ] **Step 1: Run full unit suite**

```bash
uv run pytest tests/unit/ -v 2>&1 | tail -20
```

Expected: all green; total pass count is `760 (post-Path-A) - X (deleted scope-flag tests, ~10-20) + 4 (new governance + summary tests) ≈ 745-755`.

- [ ] **Step 2: Run ruff**

```bash
uv run ruff check src/ tests/ 2>&1 | tail -10
```

Expected: only the pre-existing warnings noted in Task 1.

- [ ] **Step 3: Run mypy on key modules**

```bash
uv run mypy src/migrate/setup_sharing.py src/migrate/orchestrator.py src/migrate/hive_orchestrator.py src/migrate/grants_worker.py src/migrate/hive_grants_worker.py src/migrate/summary.py src/discovery/discovery.py src/pre_check/pre_check_governance.py src/common/config.py 2>&1 | tail -15
```

Note any new errors. Path A net-improved mypy by 2; this task should keep parity.

- [ ] **Step 4: Fix any introduced lint/type errors**

For each new error, fix carefully. Most likely sources: removed-attribute references (`config.include_uc` somewhere we missed).

- [ ] **Step 5: Commit fixes if any**

```bash
git add -u
git commit -m "chore: ruff/mypy cleanup post-workflow-split refactor

Co-authored-by: Isaac" || echo "nothing to commit"
```

---

### Task 23: Bundle deploy + integration tests on dev workspaces

**Files:** none — runtime validation

- [ ] **Step 1: Verify Databricks CLI auth**

```bash
databricks current-user me --profile source-migration | head -3
databricks current-user me --profile target-migration | head -3
```

Both must succeed.

- [ ] **Step 2: Clear bundle cache + redeploy**

```bash
rm -rf .databricks/bundle/dev
BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae \
  DATABRICKS_TF_VERSION=1.5.7 \
  DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform \
  databricks bundle deploy -t dev --profile source-migration 2>&1 | tail -10
```

Expected: deploy succeeds; new jobs visible.

- [ ] **Step 3: List the new job IDs**

```bash
databricks jobs list --profile source-migration --output json 2>&1 | python3 -c "
import json, sys
jobs = json.load(sys.stdin)
for j in jobs:
    name = j.get('settings', {}).get('name', '')
    if 'migrate' in name.lower() or 'integration' in name.lower() or 'discovery' in name.lower() or 'pre_check' in name.lower():
        print(j.get('job_id'), name)"
```

- [ ] **Step 4: Run discovery first**

```bash
DISCOVERY_JOB_ID=<from step 3>
databricks jobs run-now --profile source-migration --no-wait $DISCOVERY_JOB_ID --output json | python3 -c "import json,sys; print(json.load(sys.stdin).get('run_id'))"
```

Wait for it to finish (databricks jobs get-run polling).

- [ ] **Step 5: Run uc_integration_test**

Should succeed end-to-end (no governance assertions; those are split out).

- [ ] **Step 6: Run governance_integration_test**

Validates: pre_check_governance passes, migrate_governance completes, governance assertions in test_governance_end_to_end.py pass.

Known acceptable failure: F.1 3.24 customer-share intermittent (pre-existing per backlog).

- [ ] **Step 7: Run hive_integration_test**

Validates Hive chain unaffected.

- [ ] **Step 8: If any failures, diagnose and iterate**

For each failure: read task logs (`databricks jobs get-run-output <run_id>`), fix in code, re-deploy, re-test.

- [ ] **Step 9: No commit if all green** (runtime-only validation)

If failures required code fixes, those are committed in normal task flow.

---

### Task 24: Push branch + open PR

- [ ] **Step 1: Verify on the right branch**

```bash
git status
git log --oneline databricks-solutions/main..HEAD | head -25
```

Expected: ~22 commits on `feat/workflow-split`.

- [ ] **Step 2: Push to upstream**

```bash
git push databricks-solutions feat/workflow-split
```

- [ ] **Step 3: Open the PR**

```bash
gh pr create --repo databricks-solutions/workspace-migration \
  --base main \
  --head feat/workflow-split \
  --title "Workflow split: discovery + migrate_uc / migrate_hive / migrate_governance" \
  --body-file <(cat <<'EOF'
## Summary

Splits the monolithic `migrate_workflow.yml` into 4 independent jobs:
- `discovery` — shared upstream scan
- `migrate_uc` — UC data plane + UC grants + cleanup_staging
- `migrate_hive` — Hive data plane + Hive grants
- `migrate_governance` — fine-grained governance (tags, comments, RLS, masks, customer shares, policies, monitors, foreign catalogs)

Plus mirrored test workflows: `governance_integration_test` (new), and the existing UC / Hive / negative-paths tests scoped down. Files reorganized into `resources/production/` and `resources/integration_tests/`. `scope.include_uc` / `scope.include_hive` flags + worker short-circuit branches removed (~30 LOC).

## Decisions (from `docs/workflow_split_design.md`)

- Q1: trust-the-operator for `migrate_governance` standalone
- Q2: shared upstream `discovery` job
- Q4: per-workflow inline summary (no global)
- Q5: scope flags removed entirely
- Q6: names `migrate_uc` / `migrate_hive` / `migrate_governance`
- Q7: full-mirror test workflows
- Q8: hard cutover, single PR
- D1=b: each data workflow self-contained (UC grants in migrate_uc; Hive grants in migrate_hive)
- D2=a: customer shares in migrate_governance
- D3=b: governance test pre-seeds target via direct SQL
- D4=a: discovery summary = counts only (deferred — discovery has no summary in v1)
- D5=b: subdirs `resources/production/` + `resources/integration_tests/`

## Test plan

- [x] Unit suite green (~745-755 tests passing post-cleanup)
- [x] `uc_integration_test` SUCCESS
- [x] `governance_integration_test` SUCCESS (apart from pre-existing F.1 3.24 intermittent)
- [x] `hive_integration_test` SUCCESS
- [x] Discovery + migrate_uc / migrate_hive / migrate_governance operator flow validated end-to-end on dev workspace

## Out of scope

- C5/C6/H5/H6/H8/H10/H11 review findings — independent fixes
- Phase 4 MV + Online Tables hard-exclude
- Discovery summary task (D4 — deferred to follow-up; minimal value v1)

This pull request and its description were written by Isaac.
EOF
)
```

- [ ] **Step 4: Return PR URL**

---

## Self-Review

**1. Spec coverage**: each spec section has a task —
- Job topology (4 + 3 + 1 negative) → Tasks 13, 14, 15, 16, 18, 19
- Q1 trust-operator + pre_check_governance → Task 3
- Q2 shared discovery → Task 13 (file move + extension)
- Q3 Path A obsoletes restore_rls_cm → out of scope (already done)
- Q4 per-workflow summary → Task 2 + Tasks 14/15/16
- Q5 remove scope flags → Tasks 4, 5, 6, 7, 8, 9, 10, 11
- Q6 names → Tasks 14, 15, 16
- Q7 mirror test workflows → Tasks 17, 18, 19
- Q8 hard cutover → Task 21
- BS-3(A) hard split → no umbrella in any of Tasks 14-16
- D1=b grants placement → Task 14 (UC grants stays in migrate_uc) + Task 15 (Hive grants stays in migrate_hive)
- D2=a customer shares → Task 16 includes migrate_sharing
- D3=b governance test pre-seed → Task 17 creates seed_governance_target_state.py
- D4=a discovery summary counts-only → Task 13 deferred (out of v1; documented)
- D5=b subdir layout → Task 12

**2. Placeholder scan**:
- "TBD" / "implement later": none
- "Add appropriate error handling": none
- "Similar to Task N": none (each task has its own concrete code)
- Code blocks for all code steps: yes
- One spec gap: D4 (discovery summary) — flagged as deferred-with-reason in Task 13.

**3. Type consistency**:
- `aggregate_by_status_filtered(df, object_types: list[str])` is consistent across Task 2 def + Tasks 14/15/16 invocations
- `pre_check_governance.run(dbutils, spark) -> None` matches sibling pre_check signature
- Workflow names: `migrate_uc` / `migrate_hive` / `migrate_governance` consistent across all task references

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-07-workflow-split.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration. Same pattern that shipped Path A as PR #45.

2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.

**Important**: This plan assumes Path A (PR #45) has been merged into upstream `main`. Step 1 of Task 1 verifies that. If PR #45 is still open, hold execution until it merges.

Which approach?
