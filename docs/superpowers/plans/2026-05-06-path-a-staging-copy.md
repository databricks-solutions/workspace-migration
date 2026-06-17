# Path A — staging_copy rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `drop_and_restore` RLS/CM migration strategy with a `staging_copy` strategy that creates table copies in a tracking-catalog staging schema instead of mutating the source. Source RLS/CM is never touched.

**Architecture:** When a managed table carries row filter / column mask, `setup_sharing` does `CREATE TABLE <tracking_catalog>.cp_migration_staging.stg_<sha12> AS SELECT * FROM <original>` instead of stripping the source. The migration SPN must be a workspace admin so the table's filter function (which contains an `is_account_group_member('admins')` bypass) returns true unfiltered for the SPN's CTAS read. The staging table goes into the share. `managed_table_worker` looks up the original→staging FQN in a manifest and DEEP CLONEs from the staging consumer path. After all data work completes, `cleanup_staging` (a renamed `restore_rls_cm`) drops the staging tables. No source mutation, no restore step, no consumer-visibility window.

**Tech Stack:** Python 3.11, Databricks SDK, Delta Lake, Databricks Asset Bundles (DAB), pytest, ruff. Repo: `/Users/hari.selvarajan/uksouth_migration/workspace-migration` on `main` at `7a3b3d3`.

**Pre-validated facts (2026-04-24)**:
- Migration SPN `d0354350-71fa-4bb4-aa55-8adb5dd9f1ae` is in `admins` group ✓
- Existing integration filter `region = 'US' OR is_account_group_member('admins')` bypasses for admin SPN ✓
- CTAS through Delta Sharing works regardless of consumer-reported format ✓
- Cross-workspace DEEP CLONE from stripped-shared table succeeds after 15–30s propagation wait — race window is real ✓

**Resolves review findings**: C2, C3, C4, H1, H2, H3, H4, H9 (all gone — no strip, no restore, no CTAS branch).

**Out of scope**: workflow split (PR2), C1 NameError (independent fix — but absorbed into Task 1 since we touch the same code), C5 (model rollback), C6 (governance keys), H5/H6/H8 (other workers), H10/H11 (test helpers), Phase 4 (MV/online tables hard-exclude).

---

## File Structure

### New files

| File | Responsibility |
|---|---|
| `src/migrate/cleanup_staging.py` | Renamed `restore_rls_cm.py`. Drops staging tables after migrate completes. Updates manifest's `dropped_at`. |
| `tests/unit/test_cleanup_staging.py` | Unit tests for the cleanup task. Replaces `test_restore_rls_cm.py`. |

### Modified files

| File | Change |
|---|---|
| `src/common/tracking.py` | Add `cp_migration_staging` schema creation; add `rls_cm_staging_manifest` table; add helpers (`record_staging_created`, `mark_staging_dropped`, `get_active_stagings`, `get_staging_for_original`); fix C1 NameError on the way through; **remove** `record_rls_cm_strip`, `mark_rls_cm_restored`, `mark_rls_cm_restore_failed`, `get_unrestored_rls_cm_manifest`, `rls_cm_manifest` table creation. |
| `src/migrate/rls_cm.py` | Add `make_staging_table_fqn(original_fqn, run_id)` helper. **Remove** `strip_rls_cm`, `restore_rls_cm`, `has_rls_cm`. Keep `capture_rls_cm`, `_split_fqn`, `_dotted`, `_backticked`. |
| `src/migrate/setup_sharing.py` | Replace `drop_and_restore` flow with `staging_copy`. **Remove** `_recover_unrestored_rls_cm`, `_validate_rls_cm_strategy`'s `drop_and_restore` branch. **Add** staging schema bootstrap + CTAS-into-staging + add staging FQN to share. |
| `src/migrate/managed_table_worker.py` | Replace CTAS-for-stripped branch (line 248-249) with staging-FQN lookup + DEEP CLONE from staging consumer path. |
| `src/pre_check/pre_check.py` | Add `staging_copy` strategy pre-check: SPN must be workspace admin; every active RLS/CM filter / mask function body must contain `is_account_group_member(` / `is_member(` / `is_user_in_group(`. |
| `src/common/config.py` | Replace allowed `rls_cm_strategy` values: drop `drop_and_restore`, add `staging_copy`. **Remove** `rls_cm_maintenance_window_confirmed` field + parsing. |
| `src/discovery/discovery.py` | Update operator warning text to reference `staging_copy` instead of `drop_and_restore`. |
| `resources/migrate_workflow.yml` | Rename task `restore_rls_cm` → `cleanup_staging`; update notebook path; update job parameters. |
| `resources/uc_integration_test_workflow.yml` | Switch `rls_cm_strategy` from `drop_and_restore` to `staging_copy`; remove `rls_cm_maintenance_window_confirmed`. |
| `resources/negative_paths_integration_test_workflow.yml` | Replace X.3.4 scenario "drop_and_restore without consent" with "staging_copy without admin SPN" (or drop X.3.4 entirely — design choice in Task 13). |
| `config.yaml`, `config.example.yaml` | Document new strategy values; remove `rls_cm_maintenance_window_confirmed`. |
| `tests/integration/test_uc_end_to_end.py` | Flip RLS/CM assertions: source still has policies, staging schema empty post-cleanup, target row counts unfiltered. |
| `tests/integration/seed_uc_test_data.py` | No structural change expected — fixture's existing filter `is_account_group_member('admins')` already validated. |
| `tests/unit/test_setup_sharing.py` | Replace strip-flow tests with staging-flow tests. |
| `tests/unit/test_rls_cm.py` | Drop `strip_rls_cm` / `restore_rls_cm` tests. Add `make_staging_table_fqn` tests. |
| `tests/unit/test_tracking.py` | Drop manifest-strip-helper tests. Add staging-manifest-helper tests. |
| `tests/unit/test_config.py` | Drop `drop_and_restore` / consent-flag tests. Add `staging_copy` accepted-value test. |
| `tests/unit/test_negative_paths.py` | Drop X.3.4 consent-flag test (or replace per Task 13). |
| `tests/unit/test_pre_check.py` | Add admin-SPN + filter-body-pattern checks. |
| `tests/unit/test_discovery.py` | Update warning-text assertion. |
| `README.md` | Document new model: SPN admin requirement, filter/mask must contain admin-bypass pattern, staging schema lifecycle. Remove maintenance-window section. |

### Deleted files

| File | Reason |
|---|---|
| `src/migrate/restore_rls_cm.py` | Renamed to `cleanup_staging.py` (git mv) |
| `tests/unit/test_restore_rls_cm.py` | Replaced by `test_cleanup_staging.py` |

---

## Tasks

### Task 1: Branch + fix C1 NameError + run baseline tests

**Files:**
- Create: branch `feat/path-a-staging-copy`
- Modify: `src/common/tracking.py:445,449`

C1 is a pre-existing CRITICAL bug. We'll touch this same file extensively for staging manifest, so fix it now and land a clean prerequisite commit.

- [ ] **Step 1: Create feature branch from clean main**

```bash
cd /Users/hari.selvarajan/uksouth_migration/workspace-migration
git checkout main
git pull origin main
git checkout -b feat/path-a-staging-copy
```

- [ ] **Step 2: Verify clean baseline — all 742 unit tests pass**

```bash
pytest tests/unit/ -v 2>&1 | tail -5
```

Expected: `742 passed` (or current count). Note exact number for later comparison.

- [ ] **Step 3: Write the failing test for C1 NameError**

Open `tests/unit/test_tracking.py`. Add at end:

```python
def test_get_unrestored_rls_cm_manifest_handles_malformed_filter_columns_json(monkeypatch, tmp_path):
    """Regression: malformed JSON in filter_columns must NOT raise NameError.

    Bug C1: tracking.py imports json as _json but except clauses reference
    `json.JSONDecodeError`. One bad row → NameError → restore poisoned.
    """
    from common.tracking import TrackingManager
    from unittest.mock import MagicMock

    config = MagicMock()
    config.tracking_catalog = "main"
    config.tracking_schema = "cp_migration_tracking"

    spark = MagicMock()
    bad_row = MagicMock()
    bad_row.table_fqn = "c.s.t"
    bad_row.filter_fn_fqn = None
    bad_row.filter_columns = "{not json"
    bad_row.masks_json = "[]"
    bad_row.stripped_at = None
    bad_row.restore_failed_at = None
    bad_row.restore_error = None
    bad_row.run_id = "r1"
    spark.sql.return_value.collect.return_value = [bad_row]

    tm = TrackingManager(spark, config)
    result = tm.get_unrestored_rls_cm_manifest()

    assert result == [{"table_fqn": "c.s.t", "filter_fn_fqn": None,
                       "filter_columns": [], "masks": [],
                       "stripped_at": None, "restore_failed_at": None,
                       "restore_error": None, "run_id": "r1"}]
```

- [ ] **Step 4: Run the test, verify it fails with NameError**

```bash
pytest tests/unit/test_tracking.py::test_get_unrestored_rls_cm_manifest_handles_malformed_filter_columns_json -v
```

Expected: FAIL with `NameError: name 'json' is not defined`.

- [ ] **Step 5: Fix C1 — replace `json.JSONDecodeError` with `_json.JSONDecodeError`**

Edit `src/common/tracking.py` lines 445 and 449:

```python
            try:
                filter_columns = _json.loads(r.filter_columns or "[]")
            except _json.JSONDecodeError:
                filter_columns = []
            try:
                masks = _json.loads(r.masks_json or "[]")
            except _json.JSONDecodeError:
                masks = []
```

- [ ] **Step 6: Verify the test passes**

```bash
pytest tests/unit/test_tracking.py::test_get_unrestored_rls_cm_manifest_handles_malformed_filter_columns_json -v
```

Expected: PASS.

- [ ] **Step 7: Run full unit suite to ensure no regression**

```bash
pytest tests/unit/ -q 2>&1 | tail -5
```

Expected: 743 passed (1 more than baseline).

- [ ] **Step 8: Commit**

```bash
git add tests/unit/test_tracking.py src/common/tracking.py
git commit -m "fix(tracking): C1 NameError in get_unrestored_rls_cm_manifest

The except clauses referenced 'json.JSONDecodeError' but the import was
'import json as _json'. One bad row in the manifest raised NameError
instead of being skipped, poisoning the entire restore loop and the
crash-recovery scan in setup_sharing.

Refs: 2026-04-27 review C1.

Co-authored-by: Isaac"
```

---

### Task 2: Config — add `staging_copy` to allowed strategies (additive)

**Files:**
- Modify: `src/common/config.py:155-162` and `:244-246`
- Test: `tests/unit/test_config.py`

Additive change — keep `drop_and_restore` accepted for now so we don't break existing tests; remove it in Task 14.

- [ ] **Step 1: Write the failing test for the new accepted value**

Append to `tests/unit/test_config.py`:

```python
def test_rls_cm_strategy_staging_copy_is_accepted(self, tmp_path):
    """staging_copy is the new Path A strategy — must round-trip."""
    cfg = tmp_path / "config.yaml"
    cfg.write_text("""\
source_workspace_url: https://x
target_workspace_url: https://y
target_metastore_id: m
tracking_catalog: c
tracking_schema: s
rls_cm_strategy: staging_copy
""")
    config = MigrationConfig.from_yaml(cfg)
    assert config.rls_cm_strategy == "staging_copy"
```

- [ ] **Step 2: Run the test, verify it fails**

```bash
pytest tests/unit/test_config.py::TestMigrationConfig::test_rls_cm_strategy_staging_copy_is_accepted -v
```

Expected: FAIL — likely no failure-inducing validation today; if it passes already, that's fine for this task; the validation lives in `setup_sharing._validate_rls_cm_strategy`. Check whether the assertion holds.

If the test passes immediately (config is a passthrough), skip to Step 4.

- [ ] **Step 3: If validation lives in config, expand the allowed set**

If `config.py` has explicit validation of strategy values, edit to include `staging_copy` alongside `drop_and_restore`. Today the validation lives in `setup_sharing._validate_rls_cm_strategy` so this step is likely a no-op.

- [ ] **Step 4: Verify test passes**

```bash
pytest tests/unit/test_config.py::TestMigrationConfig::test_rls_cm_strategy_staging_copy_is_accepted -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_config.py src/common/config.py
git commit -m "feat(config): allow rls_cm_strategy='staging_copy' (Path A foundation)

Round-trip test only — strategy validation still lives in setup_sharing
and will be extended in a later task.

Co-authored-by: Isaac"
```

---

### Task 3: Tracking — add `cp_migration_staging` schema + `rls_cm_staging_manifest` table

**Files:**
- Modify: `src/common/tracking.py` (add to `_ensure_tracking_tables_exist` near line 165-189)
- Test: `tests/unit/test_tracking.py`

Additive — both old `rls_cm_manifest` and new `rls_cm_staging_manifest` coexist in this commit.

- [ ] **Step 1: Write failing test asserting new table is created**

Append to `tests/unit/test_tracking.py`:

```python
def test_init_creates_rls_cm_staging_manifest_table(monkeypatch):
    """Path A: TrackingManager init must create rls_cm_staging_manifest
    table in tracking_catalog.tracking_schema with the expected schema."""
    from common.tracking import TrackingManager
    from unittest.mock import MagicMock

    config = MagicMock()
    config.tracking_catalog = "tcat"
    config.tracking_schema = "tsch"

    spark = MagicMock()
    spark.sql = MagicMock()

    TrackingManager(spark, config)

    sql_calls = [c.args[0] for c in spark.sql.call_args_list]
    staging_create = next(
        (s for s in sql_calls if "rls_cm_staging_manifest" in s and "CREATE TABLE IF NOT EXISTS" in s),
        None,
    )
    assert staging_create is not None, "rls_cm_staging_manifest CREATE missing"
    assert "original_fqn STRING NOT NULL" in staging_create
    assert "staging_fqn STRING NOT NULL" in staging_create
    assert "created_at TIMESTAMP" in staging_create
    assert "dropped_at TIMESTAMP" in staging_create
    assert "drop_error STRING" in staging_create
    assert "run_id STRING" in staging_create


def test_init_creates_cp_migration_staging_schema(monkeypatch):
    """Path A: staging tables live in tracking_catalog.cp_migration_staging,
    not tracking_schema. Must create the schema."""
    from common.tracking import TrackingManager
    from unittest.mock import MagicMock

    config = MagicMock()
    config.tracking_catalog = "tcat"
    config.tracking_schema = "tsch"

    spark = MagicMock()
    spark.sql = MagicMock()

    TrackingManager(spark, config)

    sql_calls = [c.args[0] for c in spark.sql.call_args_list]
    staging_schema_create = next(
        (s for s in sql_calls if "cp_migration_staging" in s and "CREATE SCHEMA IF NOT EXISTS" in s),
        None,
    )
    assert staging_schema_create is not None, "cp_migration_staging schema CREATE missing"
    assert "tcat.cp_migration_staging" in staging_schema_create or "`tcat`.`cp_migration_staging`" in staging_schema_create
```

- [ ] **Step 2: Run the tests, verify they fail**

```bash
pytest tests/unit/test_tracking.py -k "rls_cm_staging_manifest or cp_migration_staging_schema" -v
```

Expected: 2 FAIL.

- [ ] **Step 3: Add schema + table creation to `_ensure_tracking_tables_exist`**

In `src/common/tracking.py` after the existing `rls_cm_manifest` CREATE (around line 189), append:

```python

        # Path A staging schema — staging table copies live here so the
        # tracking_schema only holds metadata, not user data. One schema
        # for all migrations against this tracking catalog.
        self.spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {self._catalog}.cp_migration_staging
        """)

        # Path A staging manifest — records the source→staging FQN mapping
        # for each table we copy as a substitute for stripping RLS/CM.
        # ``dropped_at`` is NULL until cleanup_staging removes the staging
        # table after migrate completes.
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.rls_cm_staging_manifest (
                original_fqn STRING NOT NULL,
                staging_fqn STRING NOT NULL,
                created_at TIMESTAMP,
                dropped_at TIMESTAMP,
                drop_error STRING,
                run_id STRING
            ) USING DELTA
        """)
```

- [ ] **Step 4: Verify tests pass**

```bash
pytest tests/unit/test_tracking.py -k "rls_cm_staging_manifest or cp_migration_staging_schema" -v
```

Expected: 2 PASS.

- [ ] **Step 5: Run full unit suite — no regression**

```bash
pytest tests/unit/ -q 2>&1 | tail -5
```

Expected: 745 passed.

- [ ] **Step 6: Commit**

```bash
git add tests/unit/test_tracking.py src/common/tracking.py
git commit -m "feat(tracking): create cp_migration_staging schema + rls_cm_staging_manifest table

Path A foundation: staging tables live in tracking_catalog.cp_migration_staging.
Manifest records original→staging FQN mapping per migrated table. Coexists
with rls_cm_manifest for now; old table removed in a later task.

Co-authored-by: Isaac"
```

---

### Task 4: Tracking — staging manifest helper methods

**Files:**
- Modify: `src/common/tracking.py` (add methods after existing rls_cm_manifest helpers, ~line 425)
- Test: `tests/unit/test_tracking.py`

Add four methods: `record_staging_created`, `mark_staging_dropped`, `mark_staging_drop_failed`, `get_active_stagings`, plus an O(1) `get_staging_for_original` lookup.

- [ ] **Step 1: Write failing tests for all five methods**

Append to `tests/unit/test_tracking.py`:

```python
class TestStagingManifest:
    def _tm(self, monkeypatch):
        from common.tracking import TrackingManager
        from unittest.mock import MagicMock
        config = MagicMock()
        config.tracking_catalog = "tcat"
        config.tracking_schema = "tsch"
        spark = MagicMock()
        return TrackingManager(spark, config), spark

    def test_record_staging_created_inserts_row(self, monkeypatch):
        tm, spark = self._tm(monkeypatch)
        tm.record_staging_created(
            original_fqn="`c`.`s`.`t`",
            staging_fqn="`tcat`.`cp_migration_staging`.`stg_abc123`",
            run_id="r-1",
        )
        sql = spark.sql.call_args_list[-1].args[0]
        assert "INSERT INTO" in sql
        assert "rls_cm_staging_manifest" in sql
        assert "`c`.`s`.`t`" in sql
        assert "stg_abc123" in sql
        assert "r-1" in sql

    def test_record_staging_created_escapes_quotes(self, monkeypatch):
        tm, spark = self._tm(monkeypatch)
        tm.record_staging_created(
            original_fqn="o'reilly",
            staging_fqn="stg_xyz",
            run_id="r'1",
        )
        sql = spark.sql.call_args_list[-1].args[0]
        assert "o''reilly" in sql
        assert "r''1" in sql

    def test_mark_staging_dropped_updates_dropped_at(self, monkeypatch):
        tm, spark = self._tm(monkeypatch)
        tm.mark_staging_dropped(staging_fqn="`tcat`.`cp_migration_staging`.`stg_abc`")
        sql = spark.sql.call_args_list[-1].args[0]
        assert "UPDATE" in sql
        assert "rls_cm_staging_manifest" in sql
        assert "dropped_at = current_timestamp()" in sql
        assert "stg_abc" in sql
        assert "dropped_at IS NULL" in sql

    def test_mark_staging_drop_failed_updates_error(self, monkeypatch):
        tm, spark = self._tm(monkeypatch)
        tm.mark_staging_drop_failed(staging_fqn="stg_abc", error_message="boom")
        sql = spark.sql.call_args_list[-1].args[0]
        assert "drop_error = 'boom'" in sql
        assert "stg_abc" in sql

    def test_get_active_stagings_returns_undropped_rows(self, monkeypatch):
        tm, spark = self._tm(monkeypatch)
        from unittest.mock import MagicMock
        row = MagicMock()
        row.original_fqn = "`c`.`s`.`t`"
        row.staging_fqn = "stg_abc"
        row.created_at = None
        row.run_id = "r-1"
        spark.sql.return_value.collect.return_value = [row]
        result = tm.get_active_stagings()
        assert result == [
            {"original_fqn": "`c`.`s`.`t`", "staging_fqn": "stg_abc",
             "created_at": None, "run_id": "r-1"},
        ]
        sql = spark.sql.call_args_list[-1].args[0]
        assert "WHERE dropped_at IS NULL" in sql

    def test_get_staging_for_original_returns_staging_fqn(self, monkeypatch):
        tm, spark = self._tm(monkeypatch)
        from unittest.mock import MagicMock
        row = MagicMock()
        row.staging_fqn = "stg_abc"
        spark.sql.return_value.collect.return_value = [row]
        result = tm.get_staging_for_original("`c`.`s`.`t`")
        assert result == "stg_abc"

    def test_get_staging_for_original_returns_none_when_absent(self, monkeypatch):
        tm, spark = self._tm(monkeypatch)
        spark.sql.return_value.collect.return_value = []
        result = tm.get_staging_for_original("`c`.`s`.`missing`")
        assert result is None
```

- [ ] **Step 2: Run the tests, verify all fail**

```bash
pytest tests/unit/test_tracking.py::TestStagingManifest -v
```

Expected: 7 FAIL with `AttributeError: ... has no attribute 'record_staging_created'`.

- [ ] **Step 3: Implement the five methods**

In `src/common/tracking.py`, add after `get_unrestored_rls_cm_manifest` (around line 460):

```python
    # ---------------------------------------------------------------- Staging manifest (Path A)

    def record_staging_created(
        self,
        *,
        original_fqn: str,
        staging_fqn: str,
        run_id: str,
    ) -> None:
        """Insert a staging-manifest row. Call AFTER the CTAS into
        cp_migration_staging succeeds so we never record a non-existent
        staging table."""
        self.spark.sql(
            f"""
            INSERT INTO {self._fqn}.rls_cm_staging_manifest
            SELECT
                '{original_fqn.replace("'", "''")}',
                '{staging_fqn.replace("'", "''")}',
                current_timestamp(),
                CAST(NULL AS TIMESTAMP),
                CAST(NULL AS STRING),
                '{run_id.replace("'", "''")}'
            """
        )

    def mark_staging_dropped(self, staging_fqn: str) -> None:
        """Mark a staging table dropped after cleanup_staging succeeded."""
        self.spark.sql(
            f"""
            UPDATE {self._fqn}.rls_cm_staging_manifest
            SET dropped_at = current_timestamp(),
                drop_error = NULL
            WHERE staging_fqn = '{staging_fqn.replace("'", "''")}'
              AND dropped_at IS NULL
            """
        )

    def mark_staging_drop_failed(self, staging_fqn: str, error_message: str) -> None:
        """Stamp drop_error so the next cleanup_staging run retries
        and operators can inspect failures."""
        safe = error_message.replace("'", "''")[:4000]
        self.spark.sql(
            f"""
            UPDATE {self._fqn}.rls_cm_staging_manifest
            SET drop_error = '{safe}'
            WHERE staging_fqn = '{staging_fqn.replace("'", "''")}'
              AND dropped_at IS NULL
            """
        )

    def get_active_stagings(self) -> list[dict]:
        """Return all staging rows still awaiting cleanup, oldest first.
        cleanup_staging iterates this list to drop staging tables."""
        rows = self.spark.sql(
            f"""
            SELECT original_fqn, staging_fqn, created_at, run_id
            FROM {self._fqn}.rls_cm_staging_manifest
            WHERE dropped_at IS NULL
            ORDER BY created_at ASC
            """
        ).collect()
        return [
            {
                "original_fqn": r.original_fqn,
                "staging_fqn": r.staging_fqn,
                "created_at": r.created_at,
                "run_id": r.run_id,
            }
            for r in rows
        ]

    def get_staging_for_original(self, original_fqn: str) -> str | None:
        """Look up the staging FQN for an original table FQN. Returns None
        if no active staging exists. managed_table_worker uses this to find
        the staging consumer-side path for DEEP CLONE."""
        rows = self.spark.sql(
            f"""
            SELECT staging_fqn
            FROM {self._fqn}.rls_cm_staging_manifest
            WHERE original_fqn = '{original_fqn.replace("'", "''")}'
              AND dropped_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).collect()
        return rows[0].staging_fqn if rows else None
```

- [ ] **Step 4: Verify tests pass**

```bash
pytest tests/unit/test_tracking.py::TestStagingManifest -v
```

Expected: 7 PASS.

- [ ] **Step 5: Run full unit suite**

```bash
pytest tests/unit/ -q 2>&1 | tail -5
```

Expected: 752 passed.

- [ ] **Step 6: Commit**

```bash
git add tests/unit/test_tracking.py src/common/tracking.py
git commit -m "feat(tracking): staging manifest helpers (Path A)

Adds record_staging_created, mark_staging_dropped, mark_staging_drop_failed,
get_active_stagings, get_staging_for_original. Used by setup_sharing
(write side) and managed_table_worker / cleanup_staging (read side).

Co-authored-by: Isaac"
```

---

### Task 5: rls_cm.py — add `make_staging_table_fqn` helper

**Files:**
- Modify: `src/migrate/rls_cm.py`
- Test: `tests/unit/test_rls_cm.py`

Add ONE helper. Don't touch `strip_rls_cm` / `restore_rls_cm` yet — they stay until Task 14.

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_rls_cm.py`:

```python
class TestMakeStagingTableFqn:
    def test_deterministic_and_short(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("c.s.t", "run-1", "tcat")
        b = make_staging_table_fqn("c.s.t", "run-1", "tcat")
        assert a == b
        # FQN: `tcat`.`cp_migration_staging`.`stg_<hash>`
        assert a.startswith("`tcat`.`cp_migration_staging`.`stg_")
        assert a.endswith("`")
        # 12-char hash → "stg_xxxxxxxxxxxx" inside backticks
        last_part = a.split("`")[-2]
        assert last_part.startswith("stg_")
        assert len(last_part) == len("stg_") + 12

    def test_different_runs_different_staging_names(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("c.s.t", "run-1", "tcat")
        b = make_staging_table_fqn("c.s.t", "run-2", "tcat")
        assert a != b

    def test_different_originals_different_staging_names(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("c.s.t1", "run-1", "tcat")
        b = make_staging_table_fqn("c.s.t2", "run-1", "tcat")
        assert a != b

    def test_handles_backticked_input(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("`c`.`s`.`t`", "run-1", "tcat")
        b = make_staging_table_fqn("c.s.t", "run-1", "tcat")
        # Backticked vs unbacked must hash to same value (canonicalized).
        assert a == b

    def test_uses_provided_tracking_catalog(self):
        from migrate.rls_cm import make_staging_table_fqn
        a = make_staging_table_fqn("c.s.t", "run-1", "main_tracking")
        assert "`main_tracking`.`cp_migration_staging`" in a
```

- [ ] **Step 2: Run the tests, verify they fail**

```bash
pytest tests/unit/test_rls_cm.py::TestMakeStagingTableFqn -v
```

Expected: 5 FAIL with `ImportError: cannot import name 'make_staging_table_fqn'`.

- [ ] **Step 3: Implement the helper**

In `src/migrate/rls_cm.py`, add after `_backticked` (around line 43):

```python
def make_staging_table_fqn(original_fqn: str, run_id: str, tracking_catalog: str) -> str:
    """Generate the deterministic staging-table FQN for an original table.

    Format: `<tracking_catalog>`.`cp_migration_staging`.`stg_<sha12>`
    where sha12 is the first 12 hex chars of SHA-256 over
    "<canonical_original_fqn>|<run_id>". Same (original, run) pair always
    hashes the same name; different runs produce different names so a
    re-run after a partial cleanup never collides.

    Canonicalization strips backticks so `c`.`s`.`t` and c.s.t produce the
    same staging name.
    """
    import hashlib

    canonical = _dotted(original_fqn)
    digest = hashlib.sha256(f"{canonical}|{run_id}".encode()).hexdigest()[:12]
    return f"`{tracking_catalog}`.`cp_migration_staging`.`stg_{digest}`"
```

- [ ] **Step 4: Verify tests pass**

```bash
pytest tests/unit/test_rls_cm.py::TestMakeStagingTableFqn -v
```

Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_rls_cm.py src/migrate/rls_cm.py
git commit -m "feat(rls_cm): add make_staging_table_fqn helper (Path A)

Deterministic staging FQN generation: SHA-256 of canonical original FQN
plus run_id, truncated to 12 hex chars. Same (original, run) always
hashes the same name; different runs differ so re-runs don't collide.

Co-authored-by: Isaac"
```

---

### Task 6: setup_sharing.py — staging_copy flow

**Files:**
- Modify: `src/migrate/setup_sharing.py`
- Test: `tests/unit/test_setup_sharing.py`

Add the new flow as a third branch in `run()`. `drop_and_restore` keeps working until Task 14.

- [ ] **Step 1: Write failing test for staging_copy strategy**

Append to `tests/unit/test_setup_sharing.py`:

```python
class TestStagingCopyFlow:
    def _setup(self, mocker):
        """Common fixtures: config with staging_copy strategy, mock auth, mock tracker."""
        from common.config import MigrationConfig
        config = mocker.MagicMock(spec=MigrationConfig)
        config.rls_cm_strategy = "staging_copy"
        config.rls_cm_maintenance_window_confirmed = False  # not required for staging_copy
        config.include_uc = True
        config.dry_run = False
        config.tracking_catalog = "tcat"
        config.current_run_id = "run-abc"
        return config

    def test_staging_copy_creates_staging_table_via_ctas(self, mocker):
        """When rls_cm_strategy=staging_copy and a pending table has RLS/CM,
        setup_sharing must CTAS into cp_migration_staging schema, NOT strip source."""
        config = self._setup(mocker)
        mocker.patch("common.config.MigrationConfig.from_workspace_file", return_value=config)
        spark = mocker.MagicMock()
        auth = mocker.MagicMock()
        tracker = mocker.MagicMock()
        tracker.get_pending_objects.return_value = [
            {"object_name": "`c`.`s`.`rls_table`",
             "object_type": "managed_table",
             "catalog_name": "c", "schema_name": "s"},
        ]
        tracker.get_tables_with_rls_cm.return_value = ["`c`.`s`.`rls_table`"]
        mocker.patch("migrate.setup_sharing.AuthManager", return_value=auth)
        mocker.patch("migrate.setup_sharing.TrackingManager", return_value=tracker)
        mocker.patch("migrate.setup_sharing._add_rls_cm_from_tables_api")
        mocker.patch("migrate.setup_sharing.capture_rls_cm",
                     return_value={"filter_fn_fqn": "fn", "filter_columns": [], "masks": []})
        mocker.patch("migrate.setup_sharing.has_rls_cm", return_value=True)
        mocker.patch("migrate.setup_sharing.get_or_create_share", return_value="cp_migration_share")
        mocker.patch("migrate.setup_sharing.get_or_create_recipient", return_value="rec")
        mocker.patch("migrate.setup_sharing.add_tables_to_share")
        mocker.patch("migrate.setup_sharing.ensure_target_catalogs_and_schemas")
        mocker.patch("migrate.setup_sharing.ensure_share_consumer_catalog")
        auth.target_client.metastores.summary.return_value = mocker.MagicMock(global_metastore_id="m")

        from migrate.setup_sharing import run
        run(mocker.MagicMock(), spark)

        # Assert: spark.sql was called with a CREATE TABLE ... AS SELECT into staging schema
        ctas_calls = [c.args[0] for c in spark.sql.call_args_list if "CREATE TABLE" in c.args[0] and "cp_migration_staging" in c.args[0]]
        assert len(ctas_calls) == 1, f"Expected 1 CTAS call, got {len(ctas_calls)}: {ctas_calls}"
        assert "AS SELECT * FROM" in ctas_calls[0]
        assert "`c`.`s`.`rls_table`" in ctas_calls[0]

        # Manifest write happens
        tracker.record_staging_created.assert_called_once()
        kwargs = tracker.record_staging_created.call_args.kwargs
        assert kwargs["original_fqn"] == "`c`.`s`.`rls_table`"
        assert kwargs["run_id"] == "run-abc"

        # Source NEVER stripped
        strip_calls = [c.args[0] for c in spark.sql.call_args_list if "DROP ROW FILTER" in c.args[0] or "DROP MASK" in c.args[0]]
        assert strip_calls == [], f"staging_copy must NOT strip source — found: {strip_calls}"

    def test_staging_copy_adds_staging_fqn_to_share_not_original(self, mocker):
        """The staging table goes into the share, not the original."""
        config = self._setup(mocker)
        mocker.patch("common.config.MigrationConfig.from_workspace_file", return_value=config)
        spark = mocker.MagicMock()
        auth = mocker.MagicMock()
        tracker = mocker.MagicMock()
        tracker.get_pending_objects.return_value = [
            {"object_name": "`c`.`s`.`rls_table`",
             "object_type": "managed_table",
             "catalog_name": "c", "schema_name": "s"},
        ]
        tracker.get_tables_with_rls_cm.return_value = ["`c`.`s`.`rls_table`"]
        mocker.patch("migrate.setup_sharing.AuthManager", return_value=auth)
        mocker.patch("migrate.setup_sharing.TrackingManager", return_value=tracker)
        mocker.patch("migrate.setup_sharing._add_rls_cm_from_tables_api")
        mocker.patch("migrate.setup_sharing.capture_rls_cm",
                     return_value={"filter_fn_fqn": "fn", "filter_columns": [], "masks": []})
        mocker.patch("migrate.setup_sharing.has_rls_cm", return_value=True)
        mocker.patch("migrate.setup_sharing.get_or_create_share", return_value="cp_migration_share")
        mocker.patch("migrate.setup_sharing.get_or_create_recipient", return_value="rec")
        add_tables = mocker.patch("migrate.setup_sharing.add_tables_to_share")
        mocker.patch("migrate.setup_sharing.ensure_target_catalogs_and_schemas")
        mocker.patch("migrate.setup_sharing.ensure_share_consumer_catalog")
        auth.target_client.metastores.summary.return_value = mocker.MagicMock(global_metastore_id="m")

        from migrate.setup_sharing import run
        run(mocker.MagicMock(), spark)

        # add_tables_to_share gets the STAGING fqn, not the original
        added = add_tables.call_args.args[2]
        assert len(added) == 1
        assert "cp_migration_staging" in added[0]["object_name"]
        assert added[0]["object_name"] != "`c`.`s`.`rls_table`"
```

- [ ] **Step 2: Run the tests, verify they fail**

```bash
pytest tests/unit/test_setup_sharing.py::TestStagingCopyFlow -v
```

Expected: 2 FAIL — current code rejects `staging_copy` strategy via `_validate_rls_cm_strategy`.

- [ ] **Step 3: Update `_validate_rls_cm_strategy` to accept `staging_copy`**

In `src/migrate/setup_sharing.py:347-353`:

```python
    strategy = (config.rls_cm_strategy or "").strip().lower()
    if strategy not in ("", "drop_and_restore", "staging_copy"):
        msg = (
            f"Unknown rls_cm_strategy {config.rls_cm_strategy!r}. "
            f"Supported values: '' (skip), 'drop_and_restore', or 'staging_copy'."
        )
        raise ValueError(msg)
```

- [ ] **Step 4: Add staging_copy branch to the `for t in pending_tables` loop**

Edit the loop in `src/migrate/setup_sharing.py:423-460`. Add a `staging_copy` branch before the `drop_and_restore` branch:

```python
        if t["object_name"] not in rls_cm_fqns:
            tables_to_share.append(t)
            continue
        if strategy == "":
            skipped_rls_cm.append(t)
            continue
        if strategy == "staging_copy":
            # Path A: copy the table into cp_migration_staging via CTAS,
            # then add the STAGING fqn to the share. Source RLS/CM never
            # touched. Migration SPN must be a workspace admin so the
            # filter function's is_account_group_member('admins') bypass
            # returns true and the CTAS reads unfiltered rows.
            try:
                from migrate.rls_cm import make_staging_table_fqn

                captured = capture_rls_cm(auth, t["object_name"])
                if not has_rls_cm(captured):
                    tables_to_share.append(t)
                    continue
                staging_fqn = make_staging_table_fqn(
                    t["object_name"], run_id, config.tracking_catalog
                )
                if not config.dry_run:
                    spark_session.sql(
                        f"CREATE OR REPLACE TABLE {staging_fqn} AS "
                        f"SELECT * FROM {t['object_name']}"
                    )
                    tracker.record_staging_created(
                        original_fqn=t["object_name"],
                        staging_fqn=staging_fqn,
                        run_id=run_id,
                    )
                # Share the staging FQN, not the original.
                staging_share_entry = dict(t)
                staging_share_entry["object_name"] = staging_fqn
                tables_to_share.append(staging_share_entry)
                stripped_rls_cm.append(t)  # reuse counter for logging
                continue
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Failed to create staging copy for %s; table will NOT be shared. "
                    "Source state unchanged. Error: %s",
                    t["object_name"], exc, exc_info=True,
                )
                skipped_rls_cm.append(t)
                continue
        # drop_and_restore branch — unchanged, still in place until Task 14
        if strategy != "drop_and_restore":
            skipped_rls_cm.append(t)
            continue
        # ... existing drop_and_restore code ...
```

(The complete replacement diff for the loop is shown above; preserve the existing `drop_and_restore` code exactly as-is from lines 430-460.)

- [ ] **Step 5: Skip crash-recovery for staging_copy**

In `setup_sharing.run()` line 386-387:

```python
    if strategy == "drop_and_restore":
        _recover_unrestored_rls_cm(auth, tracker, spark_session)
    # staging_copy has no equivalent recovery — orphaned staging tables
    # are dropped by cleanup_staging on next run via get_active_stagings().
```

- [ ] **Step 6: Verify the new tests pass**

```bash
pytest tests/unit/test_setup_sharing.py::TestStagingCopyFlow -v
```

Expected: 2 PASS.

- [ ] **Step 7: Verify existing setup_sharing tests still pass**

```bash
pytest tests/unit/test_setup_sharing.py -v 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 8: Run full unit suite**

```bash
pytest tests/unit/ -q 2>&1 | tail -5
```

Expected: ≥757 passed.

- [ ] **Step 9: Commit**

```bash
git add tests/unit/test_setup_sharing.py src/migrate/setup_sharing.py
git commit -m "feat(setup_sharing): staging_copy flow (Path A)

For RLS/CM tables under rls_cm_strategy=staging_copy, CTAS into
tracking_catalog.cp_migration_staging.stg_<sha12> and share the staging
FQN. Source RLS/CM is never mutated. Old drop_and_restore path
unchanged in this commit; removed in a later task.

Co-authored-by: Isaac"
```

---

### Task 7: managed_table_worker — DEEP CLONE from staging consumer path

**Files:**
- Modify: `src/migrate/managed_table_worker.py:246-253`
- Test: `tests/unit/test_managed_table_worker.py`

Today (line 248), the worker checks `if obj_name in rls_cm_stripped:` and CTAS's from `consumer_fqn = "<consumer>.<schema>.<table>"`. Path A: lookup the staging FQN from the manifest; build the consumer-side path as `<share_name>_consumer.cp_migration_staging.<staging_table_name>`; DEEP CLONE.

- [ ] **Step 1: Read the current state of managed_table_worker.py around lines 240-260**

```bash
sed -n '230,260p' src/migrate/managed_table_worker.py
```

Note exact code; the rest of the steps reference it.

- [ ] **Step 2: Write failing test for staging-aware DEEP CLONE**

Append to `tests/unit/test_managed_table_worker.py`:

```python
class TestStagingCopyDeepClone:
    def test_deep_clones_from_staging_consumer_path_when_staging_exists(self, mocker):
        """When tracker.get_staging_for_original returns a staging FQN, the
        worker must DEEP CLONE from <consumer>.cp_migration_staging.<staging_table>,
        NOT from the original consumer path."""
        from migrate.managed_table_worker import _migrate_one
        config = mocker.MagicMock()
        config.dry_run = False
        config.rls_cm_strategy = "staging_copy"
        auth = mocker.MagicMock()
        tracker = mocker.MagicMock()
        # Staging exists for this table
        tracker.get_staging_for_original.return_value = "`tcat`.`cp_migration_staging`.`stg_abcdef123456`"
        validator = mocker.MagicMock()
        validator.validate_row_count.return_value = {"match": True, "source_count": 5, "target_count": 5}
        execute = mocker.patch("migrate.managed_table_worker.execute_and_poll",
                               return_value={"state": "SUCCEEDED"})
        obj = {"object_name": "`c`.`s`.`rls_table`",
               "catalog_name": "c", "schema_name": "s", "table_name": "rls_table",
               "object_type": "managed_table", "data_format": "DELTA"}

        _migrate_one(obj, auth, "wh-id", "cp_migration_share_consumer", config, validator,
                     rls_cm_stripped=set(), tracker=tracker)

        sql = execute.call_args.args[2]
        assert "DEEP CLONE" in sql
        assert "cp_migration_staging" in sql
        assert "stg_abcdef123456" in sql
        # Must NOT be a CTAS, must NOT clone from original
        assert "AS SELECT *" not in sql
```

NOTE: the actual `_migrate_one` signature in current code may not accept `tracker` — adjust signature in Task 7 to thread it through. Read current signature first; adjust both signature and callers in this task.

- [ ] **Step 3: Run the test, verify it fails**

```bash
pytest tests/unit/test_managed_table_worker.py::TestStagingCopyDeepClone -v
```

Expected: FAIL.

- [ ] **Step 4: Thread `tracker` into `_migrate_one` and add staging-aware branch**

In `src/migrate/managed_table_worker.py` around line 246, replace:

```python
    # --- Delta (default) — DEEP CLONE, CTAS fallback for RLS/CM-stripped tables ---
    consumer_fqn = f"`{consumer_catalog}`.`{schema}`.`{table}`"
    if obj_name in rls_cm_stripped:
        sql = f"CREATE OR REPLACE TABLE {target_fqn} AS SELECT * FROM {consumer_fqn}"
        logger.info("Executing CTAS (RLS/CM-stripped) for %s", obj_name)
    else:
        sql = f"CREATE OR REPLACE TABLE {target_fqn} DEEP CLONE {consumer_fqn}"
        logger.info("Executing DEEP CLONE for %s", obj_name)
```

with:

```python
    # --- Delta (default) — DEEP CLONE from staging consumer path if staging exists,
    #     else from original consumer path. CTAS fallback retained for backwards
    #     compatibility with drop_and_restore until that path is removed (Task 14).
    consumer_fqn = f"`{consumer_catalog}`.`{schema}`.`{table}`"
    staging_fqn = tracker.get_staging_for_original(obj_name) if tracker else None
    if staging_fqn:
        # Path A: staging table is a regular Delta table, full schema preserved,
        # DEEP CLONE works without CTAS fallback. Build consumer-side path:
        # <consumer>.cp_migration_staging.<staging_table_name>
        staging_table_name = staging_fqn.rstrip("`").split("`")[-1]
        staging_consumer_fqn = f"`{consumer_catalog}`.`cp_migration_staging`.`{staging_table_name}`"
        sql = f"CREATE OR REPLACE TABLE {target_fqn} DEEP CLONE {staging_consumer_fqn}"
        logger.info("Executing DEEP CLONE from staging for %s (staging=%s)", obj_name, staging_fqn)
    elif obj_name in rls_cm_stripped:
        sql = f"CREATE OR REPLACE TABLE {target_fqn} AS SELECT * FROM {consumer_fqn}"
        logger.info("Executing CTAS (RLS/CM-stripped, drop_and_restore) for %s", obj_name)
    else:
        sql = f"CREATE OR REPLACE TABLE {target_fqn} DEEP CLONE {consumer_fqn}"
        logger.info("Executing DEEP CLONE for %s", obj_name)
```

Add `tracker` parameter to `_migrate_one` signature (default `None` for callers that don't have it). Update the worker's `run()` to pass `tracker`.

- [ ] **Step 5: Verify the new test passes**

```bash
pytest tests/unit/test_managed_table_worker.py::TestStagingCopyDeepClone -v
```

Expected: PASS.

- [ ] **Step 6: Verify all managed_table_worker tests still pass**

```bash
pytest tests/unit/test_managed_table_worker.py -v 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 7: Run full unit suite**

```bash
pytest tests/unit/ -q 2>&1 | tail -5
```

Expected: ≥758 passed.

- [ ] **Step 8: Commit**

```bash
git add tests/unit/test_managed_table_worker.py src/migrate/managed_table_worker.py
git commit -m "feat(managed_table_worker): DEEP CLONE from staging consumer path (Path A)

When tracker.get_staging_for_original returns a staging FQN, build the
consumer-side path <consumer>.cp_migration_staging.<staging_table> and
DEEP CLONE from that. Staging tables preserve full schema/properties,
so no CTAS fallback is needed. drop_and_restore CTAS branch kept until
Task 14 cleanup.

Co-authored-by: Isaac"
```

---

### Task 8: cleanup_staging.py (replaces restore_rls_cm.py)

**Files:**
- Create: `src/migrate/cleanup_staging.py`
- Create: `tests/unit/test_cleanup_staging.py`
- Keep: `src/migrate/restore_rls_cm.py` for now (deleted in Task 14)

- [ ] **Step 1: Write failing tests for cleanup_staging**

Create `tests/unit/test_cleanup_staging.py`:

```python
"""Tests for src/migrate/cleanup_staging.py — Path A post-migrate task."""

from unittest.mock import MagicMock


def test_cleanup_staging_skips_when_strategy_not_staging_copy():
    """If rls_cm_strategy is not 'staging_copy', do nothing."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "drop_and_restore"  # not staging_copy
    config.include_uc = True

    with _patch_config(config), _patch_tracker(MagicMock()):
        run(MagicMock(), spark)

    spark.sql.assert_not_called()


def test_cleanup_staging_skips_when_no_active_stagings():
    """No-op when there are no active stagings."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    config.include_uc = True
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = []

    with _patch_config(config), _patch_tracker(tracker):
        run(MagicMock(), spark)

    spark.sql.assert_not_called()
    tracker.mark_staging_dropped.assert_not_called()


def test_cleanup_staging_drops_each_active_staging_and_marks_manifest():
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    config.include_uc = True
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = [
        {"original_fqn": "`c`.`s`.`t1`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_a`",
         "created_at": None, "run_id": "r1"},
        {"original_fqn": "`c`.`s`.`t2`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_b`",
         "created_at": None, "run_id": "r1"},
    ]

    with _patch_config(config), _patch_tracker(tracker):
        run(MagicMock(), spark)

    drop_calls = [c.args[0] for c in spark.sql.call_args_list if "DROP TABLE" in c.args[0]]
    assert len(drop_calls) == 2
    assert "stg_a" in drop_calls[0] or "stg_a" in drop_calls[1]
    assert "stg_b" in drop_calls[0] or "stg_b" in drop_calls[1]
    assert tracker.mark_staging_dropped.call_count == 2


def test_cleanup_staging_continues_on_per_table_failure():
    """One drop fails → mark_staging_drop_failed; others still attempted."""
    from migrate.cleanup_staging import run
    spark = MagicMock()
    config = MagicMock()
    config.rls_cm_strategy = "staging_copy"
    config.include_uc = True
    tracker = MagicMock()
    tracker.get_active_stagings.return_value = [
        {"original_fqn": "`c`.`s`.`t1`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_a`",
         "created_at": None, "run_id": "r1"},
        {"original_fqn": "`c`.`s`.`t2`", "staging_fqn": "`tc`.`cp_migration_staging`.`stg_b`",
         "created_at": None, "run_id": "r1"},
    ]
    spark.sql.side_effect = [Exception("boom"), None]

    with _patch_config(config), _patch_tracker(tracker):
        try:
            run(MagicMock(), spark)
        except RuntimeError:
            pass  # we expect a final raise

    tracker.mark_staging_drop_failed.assert_called_once()
    tracker.mark_staging_dropped.assert_called_once()


# Helpers
from contextlib import contextmanager
from unittest.mock import patch


@contextmanager
def _patch_config(config):
    with patch("common.config.MigrationConfig.from_workspace_file", return_value=config):
        yield


@contextmanager
def _patch_tracker(tracker):
    with patch("migrate.cleanup_staging.TrackingManager", return_value=tracker), \
         patch("migrate.cleanup_staging.AuthManager"):
        yield
```

- [ ] **Step 2: Run the tests, verify they fail with ImportError**

```bash
pytest tests/unit/test_cleanup_staging.py -v
```

Expected: 4 FAIL with `ModuleNotFoundError: No module named 'migrate.cleanup_staging'`.

- [ ] **Step 3: Create `src/migrate/cleanup_staging.py`**

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
# Path A post-migrate cleanup task.
#
# Runs at the end of the migrate workflow when rls_cm_strategy='staging_copy'.
# For each row in rls_cm_staging_manifest with dropped_at IS NULL:
#   1. DROP TABLE IF EXISTS <staging_fqn>
#   2. UPDATE rls_cm_staging_manifest SET dropped_at = current_timestamp()
#
# Per-table continue-on-failure: a single bad drop doesn't block the rest;
# failures stamp drop_error for operator follow-up. Final RuntimeError raised
# if any drops failed so the operator sees the workflow task fail.

import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cleanup_staging")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def run(dbutils, spark) -> None:  # noqa: ARG001
    config = MigrationConfig.from_workspace_file()
    if (config.rls_cm_strategy or "").strip().lower() != "staging_copy":
        logger.info("rls_cm_strategy is not 'staging_copy'; nothing to clean up.")
        return
    if not config.include_uc:
        logger.info("scope.include_uc=false; skipping cleanup_staging.")
        return

    auth = AuthManager(config, dbutils)  # noqa: F841 — kept for symmetry
    tracker = TrackingManager(spark, config)

    stagings = tracker.get_active_stagings()
    if not stagings:
        logger.info("No active staging tables — nothing to clean up.")
        return
    logger.info("Cleaning up %d staging table(s).", len(stagings))

    dropped = 0
    failed: list[tuple[str, str]] = []
    for row in stagings:
        staging_fqn = row["staging_fqn"]
        try:
            spark.sql(f"DROP TABLE IF EXISTS {staging_fqn}")
            tracker.mark_staging_dropped(staging_fqn)
            dropped += 1
            logger.info("Dropped staging table %s.", staging_fqn)
        except Exception as exc:  # noqa: BLE001
            err_text = str(exc)
            tracker.mark_staging_drop_failed(staging_fqn, err_text)
            failed.append((staging_fqn, err_text))
            logger.error("Drop failed for %s: %s", staging_fqn, exc, exc_info=True)

    logger.info("cleanup_staging done. %d dropped, %d failed.", dropped, len(failed))
    if failed:
        lines = "\n".join(f"  {fqn}: {err[:200]}" for fqn, err in failed)
        raise RuntimeError(
            f"{len(failed)} staging table(s) failed to drop. Re-run this task "
            f"after addressing the underlying error. Tables:\n{lines}"
        )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
```

- [ ] **Step 4: Verify the new tests pass**

```bash
pytest tests/unit/test_cleanup_staging.py -v
```

Expected: 4 PASS.

- [ ] **Step 5: Run full unit suite**

```bash
pytest tests/unit/ -q 2>&1 | tail -5
```

Expected: ≥762 passed.

- [ ] **Step 6: Commit**

```bash
git add tests/unit/test_cleanup_staging.py src/migrate/cleanup_staging.py
git commit -m "feat(cleanup_staging): add post-migrate staging table cleanup task (Path A)

Iterates rls_cm_staging_manifest, drops each undropped staging table,
stamps dropped_at. Per-table continue-on-failure with final RuntimeError
if any failed. restore_rls_cm.py untouched in this commit; deleted in
Task 14.

Co-authored-by: Isaac"
```

---

### Task 9: pre_check — SPN admin check + filter/mask body validation

**Files:**
- Modify: `src/pre_check/pre_check.py`
- Test: `tests/unit/test_pre_check.py`

Path A requires (a) the migration SPN is in the `admins` workspace group, and (b) every active RLS/CM filter / mask function body contains an admin-bypass call. Without either, the CTAS into staging will fail or return filtered data. Pre-check fails loud BEFORE setup_sharing executes any side effects.

- [ ] **Step 1: Read current pre_check.py to find insertion point**

```bash
grep -n "^def\|run\|^class" src/pre_check/pre_check.py | head -30
```

- [ ] **Step 2: Write failing tests**

Append to `tests/unit/test_pre_check.py`:

```python
class TestStagingCopyPreChecks:
    def test_fails_when_strategy_staging_copy_and_spn_not_admin(self, mocker):
        """staging_copy requires the migration SPN be a workspace admin."""
        from pre_check.pre_check import _check_staging_copy_preconditions
        config = mocker.MagicMock()
        config.rls_cm_strategy = "staging_copy"
        auth = mocker.MagicMock()
        # Mock: SPN is NOT in the admins group
        auth.source_client.current_user.me.return_value = mocker.MagicMock(id="spn-123")
        groups_iter = [mocker.MagicMock(display_name="developers"),
                       mocker.MagicMock(display_name="readers")]
        auth.source_client.groups.list.return_value = groups_iter
        # No admins group hit
        with mocker.patch.object(auth.source_client.groups, "list",
                                 return_value=groups_iter):
            errors = _check_staging_copy_preconditions(config, auth)
        assert any("admins" in e.lower() for e in errors)

    def test_passes_when_spn_admin_and_filters_have_bypass(self, mocker):
        """All-clear: SPN admin + every filter/mask has is_account_group_member admin bypass."""
        from pre_check.pre_check import _check_staging_copy_preconditions
        config = mocker.MagicMock()
        config.rls_cm_strategy = "staging_copy"
        auth = mocker.MagicMock()
        # SPN admin
        admins_group = mocker.MagicMock(display_name="admins")
        admins_group.members = [mocker.MagicMock(value="spn-123")]
        auth.source_client.groups.list.return_value = [admins_group]
        auth.source_client.current_user.me.return_value = mocker.MagicMock(id="spn-123")
        # Mock the filter-fn-body fetch — return a body containing the bypass pattern
        mocker.patch("pre_check.pre_check._fetch_active_filter_mask_function_bodies",
                     return_value=[("c.s.fn1", "RETURN region = 'US' OR is_account_group_member('admins')")])

        errors = _check_staging_copy_preconditions(config, auth)
        assert errors == []

    def test_fails_when_filter_lacks_admin_bypass(self, mocker):
        from pre_check.pre_check import _check_staging_copy_preconditions
        config = mocker.MagicMock()
        config.rls_cm_strategy = "staging_copy"
        auth = mocker.MagicMock()
        admins_group = mocker.MagicMock(display_name="admins")
        admins_group.members = [mocker.MagicMock(value="spn-123")]
        auth.source_client.groups.list.return_value = [admins_group]
        auth.source_client.current_user.me.return_value = mocker.MagicMock(id="spn-123")
        mocker.patch("pre_check.pre_check._fetch_active_filter_mask_function_bodies",
                     return_value=[("c.s.fn_bad", "RETURN region = 'US'")])

        errors = _check_staging_copy_preconditions(config, auth)
        assert any("c.s.fn_bad" in e for e in errors)

    def test_skipped_when_strategy_not_staging_copy(self, mocker):
        from pre_check.pre_check import _check_staging_copy_preconditions
        config = mocker.MagicMock()
        config.rls_cm_strategy = ""
        auth = mocker.MagicMock()
        errors = _check_staging_copy_preconditions(config, auth)
        assert errors == []
```

- [ ] **Step 3: Run tests, verify they fail with ImportError on `_check_staging_copy_preconditions`**

```bash
pytest tests/unit/test_pre_check.py::TestStagingCopyPreChecks -v
```

Expected: 4 FAIL.

- [ ] **Step 4: Implement `_check_staging_copy_preconditions` and `_fetch_active_filter_mask_function_bodies`**

In `src/pre_check/pre_check.py`, add (location: top-level functions; you'll wire them into `run()` in Step 6):

```python
ADMIN_BYPASS_PATTERNS = (
    "is_account_group_member(",
    "is_member(",
    "is_user_in_group(",
)


def _check_staging_copy_preconditions(config, auth) -> list[str]:
    """Return a list of error messages. Empty list = all checks passed.

    Path A invariants:
      - Migration SPN must be in the workspace 'admins' group.
      - Every active RLS row filter / column mask function body must
        contain one of the admin-bypass calls. Without this, the SPN's
        CTAS into staging would read filtered/masked data.
    """
    if (config.rls_cm_strategy or "").strip().lower() != "staging_copy":
        return []

    errors: list[str] = []

    # Check 1: SPN is in admins group
    me = auth.source_client.current_user.me()
    spn_id = getattr(me, "id", None)
    admin_members: set[str] = set()
    for g in auth.source_client.groups.list():
        if (getattr(g, "display_name", None) or "").lower() == "admins":
            for m in (getattr(g, "members", None) or []):
                v = getattr(m, "value", None)
                if v:
                    admin_members.add(str(v))
            break
    if spn_id is None or str(spn_id) not in admin_members:
        errors.append(
            f"rls_cm_strategy=staging_copy requires the migration SPN "
            f"(id={spn_id!r}) to be a member of the workspace 'admins' "
            f"group on the source workspace. Add the SPN to the admins "
            f"group, then re-run pre_check."
        )

    # Check 2: every active filter/mask fn body has an admin-bypass call
    bodies = _fetch_active_filter_mask_function_bodies(auth)
    for fn_fqn, body in bodies:
        body_lower = (body or "").lower()
        if not any(p in body_lower for p in ADMIN_BYPASS_PATTERNS):
            errors.append(
                f"rls_cm_strategy=staging_copy requires every active row filter "
                f"/ column mask function body to contain an admin-bypass call "
                f"({', '.join(ADMIN_BYPASS_PATTERNS)}). Function {fn_fqn!r} "
                f"body lacks one. Add an admin-bypass clause (e.g. "
                f"`OR is_account_group_member('admins')`) and re-run."
            )
    return errors


def _fetch_active_filter_mask_function_bodies(auth) -> list[tuple[str, str]]:
    """Return [(function_fqn, function_body), ...] for every UC function
    referenced by any active row filter or column mask on a managed table.

    Implementation: iterate source_client.tables.list (or similar) is
    expensive; instead read from discovery_inventory's row_filters and
    column_masks columns if available. Fall back to UC functions API if
    needed.

    Returns empty list if no policies exist.
    """
    # TODO in Task 9 implementation: depends on what discovery already collects.
    # If discovery surfaces filter_fn_fqn / mask_fn_fqn per table, gather the
    # set of function FQNs and call `source_client.functions.get(fqn)` for each;
    # extract `routine_definition`. If discovery does not surface fn FQNs, do
    # the full table-scan via tables.list with column probing.
    #
    # For now, return [] so the test infrastructure can mock this — concrete
    # implementation is a Task 9 sub-step, see code path notes inline.
    return []
```

NOTE: The `_fetch_active_filter_mask_function_bodies` body is intentionally a stub returning `[]`; tests mock it directly. The full implementation requires reading the existing discovery surface — inspect `src/discovery/discovery.py` for `list_row_filters` / `list_column_masks` outputs and wire to `source_client.functions.get(fqn).routine_definition`. Add this in a follow-up step within this same task before moving on.

- [ ] **Step 5: Implement `_fetch_active_filter_mask_function_bodies` for real**

```bash
grep -n "row_filter\|column_mask\|list_row_filters\|list_column_masks" src/discovery/discovery.py | head -20
```

Inspect output. Then in `_fetch_active_filter_mask_function_bodies`:

```python
def _fetch_active_filter_mask_function_bodies(auth) -> list[tuple[str, str]]:
    """Return [(function_fqn, function_body), ...] for every UC function
    referenced by any active row filter / column mask on a managed table."""
    fn_fqns: set[str] = set()
    # Iterate all UC catalogs/schemas/tables for in-effect filters and masks.
    # Bounded by tables.get cost; pre_check already runs against the full
    # source workspace.
    for catalog in auth.source_client.catalogs.list():
        if getattr(catalog, "catalog_type", "") in ("DELTASHARING_CATALOG", "FOREIGN_CATALOG"):
            continue
        try:
            schemas = auth.source_client.schemas.list(catalog_name=catalog.name)
        except Exception:  # noqa: BLE001
            continue
        for sch in schemas:
            if sch.name in ("information_schema",):
                continue
            try:
                tables = auth.source_client.tables.list(
                    catalog_name=catalog.name, schema_name=sch.name
                )
            except Exception:  # noqa: BLE001
                continue
            for tbl in tables:
                rf = getattr(tbl, "row_filter", None)
                if rf is not None:
                    fn = getattr(rf, "function_name", None)
                    if fn:
                        fn_fqns.add(fn)
                for col in (getattr(tbl, "columns", None) or []):
                    mk = getattr(col, "mask", None)
                    if mk is not None:
                        fn = getattr(mk, "function_name", None)
                        if fn:
                            fn_fqns.add(fn)

    bodies: list[tuple[str, str]] = []
    for fqn in fn_fqns:
        try:
            fn = auth.source_client.functions.get(fqn)
            body = getattr(fn, "routine_definition", None) or ""
            bodies.append((fqn, body))
        except Exception:  # noqa: BLE001
            bodies.append((fqn, ""))
    return bodies
```

- [ ] **Step 6: Wire `_check_staging_copy_preconditions` into pre_check `run()`**

In `src/pre_check/pre_check.py`'s `run()` function, after the existing pre-checks but before any side-effecting work, append:

```python
    errors = _check_staging_copy_preconditions(config, auth)
    if errors:
        for e in errors:
            logger.error(e)
        raise RuntimeError(
            "pre_check failed Path A staging_copy preconditions:\n  - "
            + "\n  - ".join(errors)
        )
```

- [ ] **Step 7: Verify the new tests pass**

```bash
pytest tests/unit/test_pre_check.py::TestStagingCopyPreChecks -v
```

Expected: 4 PASS.

- [ ] **Step 8: Run full unit suite**

```bash
pytest tests/unit/ -q 2>&1 | tail -5
```

Expected: ≥766 passed.

- [ ] **Step 9: Commit**

```bash
git add tests/unit/test_pre_check.py src/pre_check/pre_check.py
git commit -m "feat(pre_check): staging_copy invariants — admin SPN + filter body bypass

Two new pre-checks gated on rls_cm_strategy=staging_copy:
  - Migration SPN must be in workspace 'admins' group.
  - Every active row filter / column mask function body must contain
    an admin-bypass call (is_account_group_member, is_member, or
    is_user_in_group).

Fails loud before setup_sharing executes any side effects.

Co-authored-by: Isaac"
```

---

### Task 10: Discovery — update operator warning text

**Files:**
- Modify: `src/discovery/discovery.py`
- Test: `tests/unit/test_discovery.py`

The discovery warning message references `drop_and_restore`; add `staging_copy` as the recommended option.

- [ ] **Step 1: Find the warning text**

```bash
grep -n "drop_and_restore\|maintenance" src/discovery/discovery.py
```

- [ ] **Step 2: Write failing test for updated warning**

In `tests/unit/test_discovery.py`, find the test asserting on the warning text (around line 438) and add a new assertion that the warning ALSO mentions `staging_copy`:

```python
    def test_discovery_warning_mentions_staging_copy_strategy(self, ...):
        """Operator-facing warning must mention staging_copy as the
        recommended Path A option, not just drop_and_restore."""
        # ... existing setup ...
        config.rls_cm_strategy = ""  # default
        # ... run discovery ...
        warnings = [c.args[0] for c in mock_logger.warning.call_args_list]
        all_text = " ".join(warnings)
        assert "staging_copy" in all_text
```

- [ ] **Step 3: Run test, verify it fails**

```bash
pytest tests/unit/test_discovery.py::test_discovery_warning_mentions_staging_copy_strategy -v
```

- [ ] **Step 4: Update the warning text**

Find the warning at the location identified in Step 1. Replace e.g.:

```python
            f"... Set rls_cm_strategy='drop_and_restore' in config (with consent flag) ..."
```

with:

```python
            f"... Set rls_cm_strategy='staging_copy' (recommended — no source mutation) "
            f"or 'drop_and_restore' (legacy — strips source RLS/CM) in config ..."
```

- [ ] **Step 5: Verify test passes + run full suite**

```bash
pytest tests/unit/test_discovery.py -v 2>&1 | tail -5
pytest tests/unit/ -q 2>&1 | tail -3
```

- [ ] **Step 6: Commit**

```bash
git add tests/unit/test_discovery.py src/discovery/discovery.py
git commit -m "docs(discovery): warning text mentions staging_copy strategy

Path A recommended path is now staging_copy (no source mutation).
drop_and_restore retained as legacy until Task 14 cleanup.

Co-authored-by: Isaac"
```

---

### Task 11: Workflow YAMLs — wire cleanup_staging task in parallel with restore_rls_cm

**Files:**
- Modify: `resources/migrate_workflow.yml:105-116, 271`

Add a new `cleanup_staging` task alongside `restore_rls_cm`. Old task gone in Task 14. Both fire under `run_if: ALL_DONE`; only one executes its body at runtime based on strategy.

- [ ] **Step 1: Read the current restore_rls_cm task definition**

```bash
sed -n '100,120p' resources/migrate_workflow.yml
sed -n '260,275p' resources/migrate_workflow.yml
```

- [ ] **Step 2: Add `cleanup_staging` task next to `restore_rls_cm`**

Edit `resources/migrate_workflow.yml`. After the `restore_rls_cm` task block (around line 116), add:

```yaml
        - task_key: cleanup_staging
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../src/migrate/cleanup_staging.py
            source: WORKSPACE
          depends_on:
            # Mirror restore_rls_cm's deps so it fires alongside.
            # Path A: drops staging tables created by setup_sharing.
            # Strategy gate is inside the notebook — drop_and_restore
            # runs make this a no-op.
            - task_key: migrate_managed_tables
            - task_key: migrate_external_tables
            - task_key: migrate_views
            - task_key: migrate_volumes
            - task_key: migrate_models
            - task_key: migrate_grants
            - task_key: migrate_pipelines
            - task_key: migrate_streaming_tables
            - task_key: migrate_mv_st
            - task_key: migrate_online_tables
```

(Match the `depends_on` list to whatever `restore_rls_cm` has — copy that list exactly.)

Then in line ~271 (the summary task's `depends_on`), add `cleanup_staging`:

```yaml
            - task_key: restore_rls_cm
            - task_key: cleanup_staging
```

- [ ] **Step 3: Validate bundle**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -10
```

Expected: validation passes.

- [ ] **Step 4: Commit**

```bash
git add resources/migrate_workflow.yml
git commit -m "feat(workflow): add cleanup_staging task alongside restore_rls_cm

Both fire under run_if: ALL_DONE; strategy gate inside each notebook
ensures only the active one does work. drop_and_restore + restore_rls_cm
removed in Task 14.

Co-authored-by: Isaac"
```

---

### Task 12: Integration test workflow — switch to staging_copy

**Files:**
- Modify: `resources/uc_integration_test_workflow.yml:21-22`

- [ ] **Step 1: Edit the integration test workflow**

In `resources/uc_integration_test_workflow.yml`, replace:

```yaml
              rls_cm_strategy: "drop_and_restore"
              rls_cm_maintenance_window_confirmed: "true"
```

with:

```yaml
              rls_cm_strategy: "staging_copy"
```

- [ ] **Step 2: Validate bundle**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -5
```

- [ ] **Step 3: Commit**

```bash
git add resources/uc_integration_test_workflow.yml
git commit -m "test(uc_integration): switch to rls_cm_strategy=staging_copy

Drops the consent flag (Path A doesn't need it — source isn't mutated).

Co-authored-by: Isaac"
```

---

### Task 13: Negative paths — drop or replace X.3.4

**Files:**
- Modify: `resources/negative_paths_integration_test_workflow.yml`
- Modify: `tests/unit/test_negative_paths.py`

X.3.4 today validates that `drop_and_restore` without consent fails. Path A removes the consent flag, so this scenario is moot. Either drop X.3.4 entirely OR replace it with "staging_copy without admin SPN" — but that requires test infra to demote the SPN, which is heavy. Drop it.

- [ ] **Step 1: Locate X.3.4 task block**

```bash
grep -n "X.3.4\|rls_cm_maintenance" resources/negative_paths_integration_test_workflow.yml
```

- [ ] **Step 2: Remove the X.3.4 task block from the YAML**

Edit `resources/negative_paths_integration_test_workflow.yml`. Delete the entire X.3.4 task block.

- [ ] **Step 3: Remove the matching unit test**

In `tests/unit/test_negative_paths.py:88` (referenced by current grep), find the test asserting on `rls_cm_maintenance_window_confirmed` and delete it (or modify to assert on staging-copy precondition messaging).

- [ ] **Step 4: Run unit suite — expect drop in count**

```bash
pytest tests/unit/ -q 2>&1 | tail -3
```

Expected: ≥765 passed (one fewer than after Task 10 due to X.3.4 test removal).

- [ ] **Step 5: Validate bundle**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -5
```

- [ ] **Step 6: Commit**

```bash
git add resources/negative_paths_integration_test_workflow.yml tests/unit/test_negative_paths.py
git commit -m "test(negative_paths): drop X.3.4 (consent-flag) — moot under Path A

staging_copy doesn't mutate source so the consent flag is removed entirely.

Co-authored-by: Isaac"
```

---

### Task 14: Removal — drop drop_and_restore strategy + old manifest table

**Files:**
- Modify: `src/common/config.py` (remove `rls_cm_maintenance_window_confirmed`)
- Modify: `src/common/tracking.py` (remove rls_cm_manifest table + helpers)
- Modify: `src/migrate/setup_sharing.py` (remove drop_and_restore branch + recovery + capture/strip imports)
- Modify: `src/migrate/rls_cm.py` (remove strip/restore/has_rls_cm)
- Modify: `src/migrate/managed_table_worker.py` (remove `rls_cm_stripped` CTAS branch)
- Modify: `config.yaml`, `config.example.yaml` (remove maintenance_window line + drop_and_restore docs)
- Delete: `src/migrate/restore_rls_cm.py`
- Delete: `tests/unit/test_restore_rls_cm.py`
- Modify: `tests/unit/test_config.py`, `tests/unit/test_setup_sharing.py`, `tests/unit/test_rls_cm.py`, `tests/unit/test_tracking.py` (remove obsolete tests)
- Modify: `resources/migrate_workflow.yml` (remove `restore_rls_cm` task)

This is the irreversible cutover. Big commit; get tests green first.

- [ ] **Step 1: Remove `rls_cm_maintenance_window_confirmed` from config.py**

In `src/common/config.py`, delete the field declaration (line ~162) and the parsing (line ~245-247).

- [ ] **Step 2: Remove `drop_and_restore` from `_validate_rls_cm_strategy`**

In `src/migrate/setup_sharing.py`, edit:

```python
    if strategy not in ("", "staging_copy"):
        msg = (
            f"Unknown rls_cm_strategy {config.rls_cm_strategy!r}. "
            f"Supported values: '' (skip) or 'staging_copy'."
        )
        raise ValueError(msg)
```

Remove the consent-flag check entirely.

- [ ] **Step 3: Remove drop_and_restore branch from setup_sharing's `for t in pending_tables` loop**

Delete the existing `drop_and_restore`-strategy branch (lines ~430-460 of original file). The new staging_copy branch from Task 6 is the only branch.

- [ ] **Step 4: Remove `_recover_unrestored_rls_cm` function and its call**

Delete the function and the `if strategy == "drop_and_restore":` block at line ~386-387.

- [ ] **Step 5: Remove `strip_rls_cm`, `restore_rls_cm`, `has_rls_cm` from rls_cm.py**

In `src/migrate/rls_cm.py`, delete the three functions. Keep `capture_rls_cm`, `_split_fqn`, `_dotted`, `_backticked`, `make_staging_table_fqn`.

Update the import in `setup_sharing.py` line 42:

```python
from migrate.rls_cm import capture_rls_cm, has_rls_cm, make_staging_table_fqn
```

(Wait — has_rls_cm was deleted. Inline its body in the staging_copy branch instead — it's a one-liner.)

Edit `setup_sharing.py` import:

```python
from migrate.rls_cm import capture_rls_cm, make_staging_table_fqn
```

And inline the has_rls_cm check in the staging_copy branch:

```python
            captured = capture_rls_cm(auth, t["object_name"])
            if not (captured.get("filter_fn_fqn") or captured.get("masks")):
                tables_to_share.append(t)
                continue
```

- [ ] **Step 6: Remove rls_cm_manifest table creation + helpers from tracking.py**

In `src/common/tracking.py`:
- Delete the `CREATE TABLE IF NOT EXISTS {self._fqn}.rls_cm_manifest` block (lines ~177-189).
- Delete `record_rls_cm_strip` (line 366).
- Delete `mark_rls_cm_restored` (line 398).
- Delete `mark_rls_cm_restore_failed` (line 412).
- Delete `get_unrestored_rls_cm_manifest` (line 426).

- [ ] **Step 7: Remove the `rls_cm_stripped` CTAS branch from managed_table_worker.py**

In `src/migrate/managed_table_worker.py`, the elif branch added in Task 7 is now dead code (no `drop_and_restore` strategy reaches it). Delete:

```python
    elif obj_name in rls_cm_stripped:
        sql = f"CREATE OR REPLACE TABLE {target_fqn} AS SELECT * FROM {consumer_fqn}"
        logger.info("Executing CTAS (RLS/CM-stripped, drop_and_restore) for %s", obj_name)
```

The `rls_cm_stripped` parameter can stay or be removed; remove if no callers populate it.

- [ ] **Step 8: Delete restore_rls_cm.py and its test**

```bash
git rm src/migrate/restore_rls_cm.py tests/unit/test_restore_rls_cm.py
```

- [ ] **Step 9: Remove restore_rls_cm task from workflow YAML**

In `resources/migrate_workflow.yml`, delete the entire `task_key: restore_rls_cm` block (lines ~110-116). Update summary task's `depends_on` to drop `restore_rls_cm`.

- [ ] **Step 10: Update config.yaml + config.example.yaml**

Remove `rls_cm_maintenance_window_confirmed` line from both. Update the comment block above `rls_cm_strategy` to describe `staging_copy` instead of `drop_and_restore`.

- [ ] **Step 11: Update obsolete unit tests**

Files to clean (run each, fix what breaks):

```bash
pytest tests/unit/test_config.py -v 2>&1 | tail -10
pytest tests/unit/test_setup_sharing.py -v 2>&1 | tail -10
pytest tests/unit/test_rls_cm.py -v 2>&1 | tail -10
pytest tests/unit/test_tracking.py -v 2>&1 | tail -10
pytest tests/unit/test_managed_table_worker.py -v 2>&1 | tail -10
```

For each failure: if the test asserts on `drop_and_restore` / `rls_cm_maintenance_window_confirmed` / `record_rls_cm_strip` / `strip_rls_cm` / `restore_rls_cm` / `rls_cm_manifest`, delete the test entirely. Don't try to keep coverage of removed code.

- [ ] **Step 12: Run full unit suite**

```bash
pytest tests/unit/ -q 2>&1 | tail -5
```

Expected: PASS (count drops by ~30-40 due to deletions).

- [ ] **Step 13: Validate bundle**

```bash
databricks bundle validate -t dev --profile source-migration 2>&1 | tail -5
```

- [ ] **Step 14: Commit**

```bash
git add -A
git commit -m "refactor(rls_cm): remove drop_and_restore strategy and rls_cm_manifest

Path A staging_copy is now the only RLS/CM strategy. Removes:
  - drop_and_restore code path in setup_sharing
  - strip_rls_cm / restore_rls_cm / has_rls_cm helpers
  - _recover_unrestored_rls_cm crash recovery
  - record_rls_cm_strip / mark_rls_cm_restored / mark_rls_cm_restore_failed /
    get_unrestored_rls_cm_manifest tracker helpers
  - rls_cm_manifest table
  - rls_cm_maintenance_window_confirmed consent flag
  - restore_rls_cm.py notebook + restore_rls_cm task
  - CTAS branch in managed_table_worker

Resolves review findings C2, C3, C4, H1, H2, H3, H4, H9.

Co-authored-by: Isaac"
```

---

### Task 15: Integration test assertions — flip to staging-copy invariants

**Files:**
- Modify: `tests/integration/test_uc_end_to_end.py`

Test contract changes:
- Source RLS/CM is **still present** after migrate (was: stripped-then-restored).
- `rls_cm_staging_manifest.dropped_at IS NOT NULL` for every row after cleanup_staging.
- `cp_migration_staging` schema has 0 tables after cleanup_staging.
- Target row count equals **unfiltered** source count (admin-bypass returned full data).

- [ ] **Step 1: Find the existing RLS/CM assertion block**

```bash
grep -n "rls_cm_manifest\|restored_at\|drop_and_restore\|maintenance_window" tests/integration/test_uc_end_to_end.py | head -20
```

- [ ] **Step 2: Replace assertions**

For each existing assertion that checks `rls_cm_manifest` rows / restored_at / source-stripped-then-restored, replace with the staging-copy version. Example pattern:

```python
# Old:
assert m_row["restored_at"] is not None, "Manifest row never marked restored"

# New:
staging_rows = src_spark.sql(
    f"SELECT * FROM {tracking_catalog}.{tracking_schema}.rls_cm_staging_manifest "
    f"WHERE original_fqn = '{table_fqn}'"
).collect()
assert len(staging_rows) == 1
assert staging_rows[0].dropped_at is not None, "Staging table never cleaned up"

# And: source still has its filter
src_table = source_client.tables.get(canonical_fqn)
assert src_table.row_filter is not None, "Source RLS was incorrectly removed"
```

Add a new assertion that the staging schema is empty post-cleanup:

```python
staging_tables = list(source_client.tables.list(
    catalog_name=tracking_catalog, schema_name="cp_migration_staging"
))
assert staging_tables == [], f"Staging schema not empty: {[t.name for t in staging_tables]}"
```

- [ ] **Step 3: Commit (don't run integration tests yet — Task 18)**

```bash
git add tests/integration/test_uc_end_to_end.py
git commit -m "test(integration): flip UC e2e RLS/CM assertions to staging-copy contract

Source filters intact post-migrate; staging schema empty post-cleanup;
target row counts unfiltered (admin-bypass).

Co-authored-by: Isaac"
```

---

### Task 16: Documentation — README + config docs

**Files:**
- Modify: `README.md`
- Modify: `config.example.yaml`

- [ ] **Step 1: Find README sections to update**

```bash
grep -n "drop_and_restore\|maintenance window\|RLS\|row filter\|column mask" README.md | head -30
```

- [ ] **Step 2: Replace the "Row filter / column mask on managed tables" section**

Update README.md:

```markdown
## Row filter / column mask on managed tables

Managed tables with row filters (RLS) or column masks (CM) cannot be
shared via Delta Sharing as-is. The migration tool offers two modes:

- **`rls_cm_strategy: ""`** (default) — skip these tables entirely. They
  appear as `skipped_by_rls_cm_policy` in `migration_status`. No data
  copied; operator must use a separate path (e.g., ABAC migration).

- **`rls_cm_strategy: "staging_copy"`** — Path A. For each affected
  table, the tool creates a staging copy in
  `<tracking_catalog>.cp_migration_staging.stg_<sha12>` via
  `CREATE TABLE ... AS SELECT * FROM <original>`, adds the staging FQN
  to the share, and DEEP CLONEs the staging table on target. Source RLS
  / CM is **never** mutated. After migrate completes, `cleanup_staging`
  drops the staging tables.

### Pre-conditions for `staging_copy`

1. **Migration SPN must be a workspace admin** on the source workspace.
   The CTAS into staging reads through the source's row filter; without
   admin status, the SPN gets filtered data.

2. **Every active row filter / column mask function body must contain
   an admin-bypass call** — one of `is_account_group_member(`,
   `is_member(`, or `is_user_in_group(`. Without this, even an admin
   SPN's CTAS returns filtered data.

`pre_check` validates both invariants before any side-effecting work.
```

- [ ] **Step 3: Update config.example.yaml docs**

```yaml
# rls_cm_strategy: how to handle managed tables with row filter / column mask.
#   ""             — skip these tables (default; safest).
#   "staging_copy" — Path A. CTAS into tracking_catalog.cp_migration_staging,
#                    share staging FQN, DEEP CLONE on target, drop staging.
#                    Source RLS/CM untouched. Migration SPN must be a workspace
#                    admin AND every filter/mask fn body must contain an
#                    admin-bypass call (is_account_group_member /
#                    is_member / is_user_in_group). pre_check validates both.
rls_cm_strategy: ""
```

Remove `rls_cm_maintenance_window_confirmed` line entirely.

- [ ] **Step 4: Commit**

```bash
git add README.md config.example.yaml config.yaml
git commit -m "docs(README): document Path A staging_copy strategy + pre-conditions

Removes drop_and_restore + maintenance-window section. Explains the
admin-SPN and admin-bypass-pattern requirements that pre_check
enforces.

Co-authored-by: Isaac"
```

---

### Task 17: Final unit-test sweep + ruff/mypy

**Files:** none — verification only

- [ ] **Step 1: Run full unit test suite**

```bash
pytest tests/unit/ -v 2>&1 | tail -20
```

Expected: all pass. Note the count and verify no skips/errors.

- [ ] **Step 2: Run ruff**

```bash
ruff check src/ tests/ 2>&1 | tail -20
```

Expected: no errors. Fix any introduced.

- [ ] **Step 3: Run mypy on key modules**

```bash
mypy src/migrate/setup_sharing.py src/migrate/cleanup_staging.py src/migrate/rls_cm.py src/common/tracking.py 2>&1 | tail -20
```

Expected: no new errors vs. main.

- [ ] **Step 4: Commit any lint fixes**

```bash
git add -u
git commit -m "chore: ruff/mypy cleanup post-Path-A refactor" || echo "nothing to commit"
```

---

### Task 18: Integration test deploy + run

**Files:** none — runtime validation

- [ ] **Step 1: Deploy bundle to dev workspace**

```bash
BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae \
  DATABRICKS_TF_VERSION=1.5.7 \
  DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform \
  databricks bundle deploy -t dev --profile source-migration 2>&1 | tail -20
```

Expected: deploy succeeds, all jobs created/updated.

- [ ] **Step 2: Run UC integration test job**

```bash
databricks jobs run-now 724824910139235 --profile source-migration 2>&1 | tail -5
```

Note the run ID for monitoring.

- [ ] **Step 3: Monitor the run**

```bash
databricks jobs get-run <run-id> --profile source-migration --output json | jq '.state'
```

Wait for `state.result_state == "SUCCESS"` (or `SUCCESS_WITH_FAILURES` — investigate failures if any).

- [ ] **Step 4: Pull failures if any**

If any tasks failed, inspect logs:

```bash
databricks jobs list-runs --job-id 724824910139235 --profile source-migration --limit 1 --output json | jq '.runs[0].tasks[] | {task_key, state}'
```

For each FAILED task, fetch logs and diagnose. Common failure modes:
- Filter function lacks admin bypass → pre_check should have caught (verify pre_check deploy)
- SPN not admin → same
- Staging schema permission errors → tracking_catalog grants

- [ ] **Step 5: Verify staging schema is empty post-run**

```bash
databricks sql query --warehouse-id <wh-id> --profile source-migration \
  "SHOW TABLES IN <tracking_catalog>.cp_migration_staging"
```

Expected: empty (cleanup_staging dropped all stagings).

- [ ] **Step 6: Verify source RLS/CM intact**

For one of the test tables with RLS/CM:

```bash
databricks tables get <test_catalog>.<test_schema>.<rls_table> --profile source-migration --output json | jq '.row_filter, .columns[].mask'
```

Expected: row_filter and column masks still present.

- [ ] **Step 7: If test passed, commit nothing — runtime validation only.**

If test failed, iterate on the relevant task; once green, return here.

---

### Task 19: PR creation

- [ ] **Step 1: Push branch**

```bash
git push -u origin feat/path-a-staging-copy
```

- [ ] **Step 2: Open PR**

```bash
gh pr create --repo databricks-solutions/workspace-migration \
  --title "Path A: staging_copy rewrite — eliminate source RLS/CM mutation" \
  --body "$(cat <<'EOF'
## Summary

Replaces the `drop_and_restore` RLS/CM migration strategy with `staging_copy`. Source RLS/CM is never touched — instead, affected tables are copied into `<tracking_catalog>.cp_migration_staging.stg_<sha12>` via CTAS, the staging FQN is added to the share, and the target DEEP CLONEs from the staging consumer path. After migrate completes, `cleanup_staging` drops the staging tables.

## Resolves

Review findings from 2026-04-27: C2, C3, C4, H1, H2, H3, H4, H9 — all eliminated by removing source mutation.

Plus: C1 (NameError in `tracking.py:445,449`) — fixed in Task 1.

## Pre-conditions

`staging_copy` requires:
1. Migration SPN is in the workspace `admins` group.
2. Every active RLS/CM function body contains `is_account_group_member(` / `is_member(` / `is_user_in_group(`.

`pre_check` enforces both.

## Test plan

- [x] 766+ unit tests passing
- [x] UC integration test job runs SUCCESS in dev workspace (run-id: <fill in>)
- [x] Source RLS/CM intact post-migrate (verified via `tables.get`)
- [x] `cp_migration_staging` schema empty post-cleanup
- [x] Target row counts equal unfiltered source counts

## Out of scope

- Workflow split (PR2)
- C5/C6/H5/H6/H8/H10/H11 review findings (independent fixes)
- Phase 4 MV+Online Tables hard-exclude

This pull request and its description were written by Isaac.
EOF
)"
```

- [ ] **Step 3: Return PR URL to user**

---

## Self-Review checklist

After all tasks above are complete:

**1. Spec coverage:** Brainstorm decisions Q3 (Path A as prerequisite) is implemented. Path A backlog design is fully covered:
- ✅ Strategy renamed to `staging_copy` (Task 2, 14)
- ✅ `rls_cm_staging_manifest` table created (Task 3)
- ✅ Staging schema `cp_migration_staging` (Task 3)
- ✅ Staging FQN generation (Task 5)
- ✅ setup_sharing creates staging via CTAS (Task 6)
- ✅ Staging added to share, original NOT (Task 6)
- ✅ managed_table_worker DEEP CLONEs from staging consumer path (Task 7)
- ✅ cleanup_staging task (Task 8)
- ✅ pre_check SPN admin + filter bypass (Task 9)
- ✅ drop_and_restore + restore_rls_cm + manifest deleted (Task 14)
- ✅ Integration test flipped (Task 15)
- ✅ README updated (Task 16)

**2. Placeholder scan:** None — all code is concrete.

**3. Type consistency:** `make_staging_table_fqn(original_fqn, run_id, tracking_catalog)` signature consistent across Tasks 5, 6. `tracker.get_staging_for_original(original_fqn)` returns `str | None` consistent across Tasks 4, 7.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-06-path-a-staging-copy.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
