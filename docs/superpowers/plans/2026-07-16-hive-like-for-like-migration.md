# Hive Like-for-Like Migration (HMS→HMS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retarget the tool's Hive path so `hive_metastore` content migrates like-for-like into the target workspace's own `hive_metastore` (same db/table names, same storage), retiring the UC-upgrade behavior entirely.

**Architecture:** Discovery classifies each Hive table by storage type and emits `/mnt` prerequisite markers; pre_check guards the target (DBFS-root enabled, mounts present, staging reachable); the orchestrator ensures target databases exist in `hive_metastore`; DDL-replay workers recreate external/managed-non-DBFS/view/function objects as-is (no namespace rewrite, same LOCATION, no data copy); the DBFS-root worker copies data via a two-hop shared `abfss://` staging path into the target's own DBFS root; and the grants worker replays grants + ownership into `hive_metastore` with grant-before-transfer + skip-if-already-owned idempotency.

**Tech Stack:** Python, Databricks DAB, PySpark, pytest, ruff

## Global Constraints
- Like-for-like into `hive_metastore` is the ONLY Hive mode; the UC-upgrade path (catalog creation, namespace rewrite, DBFS→cloud rehome) is retired.
- No UC catalog is created and no `hive_metastore.` → `<catalog>.` namespace rewrite is performed; replayed DDL keeps its `hive_metastore` namespace (target FQN == source FQN).
- DBFS-root managed tables move via a two-hop shared `abfss://` staging path (source writes staging; target reads staging and writes a MANAGED table into its own DBFS root). No UC involvement anywhere.
- `/mnt`-backed tables are reported as prerequisites (operator recreates the mount first); pre_check verifies each required mount exists on the target before migrating any `/mnt`-backed table.
- Grants + ownership are replayed into `hive_metastore` with grant-before-transfer (`GRANT USAGE, CREATE ON SCHEMA … TO <spn>` before `ALTER SCHEMA … OWNER TO <original>`) and skip-transfer-if-already-owned; built-in `hive_metastore` catalog ownership is never transferred (#14 moot).
- Config key rename: `hive_dbfs_target_path` → `hive_dbfs_staging_path` (old key kept as a deprecated alias that maps to the new field with a warning); `hive_target_catalog` removed; `migrate_hive_dbfs_root` kept.
- Keep the #12 anti-join in `hive_orchestrator.py` (matches on `object_name` only) — already fixed; DO NOT change it.

---

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `src/common/config.py` | Modify | Remove `hive_target_catalog`; rename `hive_dbfs_target_path`→`hive_dbfs_staging_path` with deprecated alias + warning. |
| `src/migrate/hive_common.py` | Modify | Make `rewrite_hive_namespace`/`rewrite_hive_fqn` identity; replace `ensure_target_catalog_and_schema` with `ensure_target_database`. |
| `src/pre_check/collision_detection.py` | Modify | Drop `hive_target_catalog`; `_rewrite_hive_fqn` becomes identity (target FQN in `hive_metastore`). |
| `src/migrate/hive_orchestrator.py` | Modify | Replace `CREATE CATALOG`/`CREATE SCHEMA` with `CREATE DATABASE IF NOT EXISTS hive_metastore.<db>`; keep #12 anti-join. |
| `src/migrate/hive_external_worker.py` | Modify | `target_fqn = source_fqn`; drop namespace rewrite; keep `IF NOT EXISTS`. |
| `src/migrate/hive_managed_nondbfs_worker.py` | Modify | Drop rewrite; keep in `hive_metastore`; keep LOCATION-clause logic. |
| `src/migrate/hive_views_worker.py` | Modify | Drop rewrite; replay view DDL into `hive_metastore` (target FQN == source FQN). |
| `src/migrate/hive_functions_worker.py` | Modify | Drop rewrite; replay function DDL into `hive_metastore`. |
| `src/migrate/hive_managed_dbfs_worker.py` | Modify | Two-hop staging copy: source→`hive_dbfs_staging_path`→target MANAGED table in DBFS root. |
| `src/migrate/hive_grants_worker.py` | Modify | Grant-before-transfer + skip-if-already-owned; drop catalog-ownership; target FQN identity. |
| `src/discovery/discovery.py` | Modify | Emit `mount_prerequisite` markers for `/mnt/`-backed Hive tables. |
| `src/pre_check/pre_check.py` | Modify | Add target-DBFS-root-enabled, required-`/mnt`-mounts-exist, staging-path-reachable checks; rename config key in check 12. |
| `dashboards/migration_dashboard.lvdash.json` | Modify | Add datasets + a panel for mount prerequisites, DBFS-root copies, and skipped/failed with reasons. |
| `tests/integration/coverage_manifest.py` | Modify | Move `migrate_hive` from `RERUN_EXEMPT` into `RERUN_COVERED_JOBS`. |
| `tests/integration/test_hive_end_to_end.py` | Modify | Add a migrate_hive re-run (idempotency) leg via `assert_migrate_idempotent`. |
| `docs/user_guide.md` | Modify | Add "SPN permissions on `hive_metastore`" subsection + "what changed vs UC-upgrade" note; update §3.1/§5 wording. |
| `tests/unit/test_config.py` | Modify (Test) | New alias/removal assertions. |
| `tests/unit/test_hive_common.py` | Modify (Test) | Identity-rewrite + `ensure_target_database` assertions. |
| `tests/unit/test_collision_detection.py` | Modify (Test) | Identity target-FQN assertions. |
| `tests/unit/test_hive_orchestrator.py` | Modify (Test) | `CREATE DATABASE` (not `CREATE CATALOG`) source-guard. |
| `tests/unit/test_hive_workers.py` | Modify (Test) | Identity-target + two-hop + grants unit tests. |
| `tests/unit/test_discovery_hive_markers.py` | Create (Test) | Mount-prerequisite marker unit tests. |
| `tests/unit/test_pre_check_hive_guards.py` | Create (Test) | Pure guard-helper unit tests. |
| `tests/unit/test_dashboard_smoke.py` | Modify (Test) | New dataset/panel presence assertions. |
| `tests/unit/test_int_coverage_guard.py` | (unchanged, enforces) | Passes once `migrate_hive` is in `RERUN_COVERED_JOBS`. |

---

## Phase P1 — Core like-for-like retarget

### Task 1: Config — rename `hive_dbfs_target_path`→`hive_dbfs_staging_path` with deprecated alias; remove `hive_target_catalog`
**Files:** Modify `src/common/config.py` (field decls lines 205-207; parse lines 292-294); Test `tests/unit/test_config.py` (defaults lines 31-33; full-file lines 69-83)
**Interfaces:**
- Consumes: YAML mapping keys `migrate_hive_dbfs_root`, `hive_dbfs_staging_path` (or deprecated `hive_dbfs_target_path`).
- Produces: `MigrationConfig.hive_dbfs_staging_path: str`; new module helper `_coerce_hive_staging_path(raw: dict) -> str`.

- [ ] Step 1: Write the failing test — append to `tests/unit/test_config.py`:
```python
class TestHiveStagingPathAlias:
    def test_default_staging_path_empty_and_no_target_catalog(self):
        config = MigrationConfig(
            source_workspace_url="https://src.azuredatabricks.net",
            target_workspace_url="https://tgt.azuredatabricks.net",
            spn_client_id="client-id",
            spn_secret_scope="scope",
            spn_secret_key="key",
        )
        assert config.hive_dbfs_staging_path == ""
        assert not hasattr(config, "hive_target_catalog")

    def test_new_key_parses(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
migrate_hive_dbfs_root: true
hive_dbfs_staging_path: abfss://stage@acct.dfs.core.windows.net/hive/
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.hive_dbfs_staging_path == "abfss://stage@acct.dfs.core.windows.net/hive/"

    def test_deprecated_alias_maps_with_warning(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
hive_dbfs_target_path: abfss://legacy@acct.dfs.core.windows.net/old/
""",
        )
        with pytest.warns(DeprecationWarning, match="hive_dbfs_target_path"):
            config = MigrationConfig.from_workspace_file(str(path))
        assert config.hive_dbfs_staging_path == "abfss://legacy@acct.dfs.core.windows.net/old/"

    def test_new_key_wins_over_deprecated_alias(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
hive_dbfs_staging_path: abfss://new@acct.dfs.core.windows.net/n/
hive_dbfs_target_path: abfss://old@acct.dfs.core.windows.net/o/
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.hive_dbfs_staging_path == "abfss://new@acct.dfs.core.windows.net/n/"
```
Also EDIT the two existing assertions that will now fail: in `test_defaults` (lines 31-33) replace the three hive lines with:
```python
        assert config.migrate_hive_dbfs_root is False
        assert config.hive_dbfs_staging_path == ""
```
and in `test_from_workspace_file_full` replace the YAML lines `hive_dbfs_target_path: ...` / `hive_target_catalog: legacy_hive` (lines 70-71) with `hive_dbfs_staging_path: abfss://hive@acct.dfs.core.windows.net/upgraded/` and replace assertions (lines 82-83) with:
```python
        assert config.hive_dbfs_staging_path.startswith("abfss://")
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_config.py -v`  Expected: FAIL — `MigrationConfig` still has `hive_target_catalog`; no `hive_dbfs_staging_path`; no `DeprecationWarning`.)
- [ ] Step 3: Write minimal implementation — in `src/common/config.py`:

Add near the top of the module (after the `from pathlib import Path` line 4):
```python
import warnings
```
Add the helper alongside the other `_coerce_*` helpers (after `_coerce_bool`, ~line 56):
```python
def _coerce_hive_staging_path(raw: dict) -> str:
    """Resolve the shared abfss staging path for the DBFS-root two-hop copy.

    Prefers the current ``hive_dbfs_staging_path`` key. Falls back to the
    deprecated ``hive_dbfs_target_path`` (the old UC-rehome key) with a
    DeprecationWarning so existing config.yaml files keep working through the
    like-for-like rename. Returns "" when neither is set.
    """
    new = raw.get("hive_dbfs_staging_path")
    if new:
        return str(new)
    old = raw.get("hive_dbfs_target_path")
    if old:
        warnings.warn(
            "hive_dbfs_target_path is deprecated; rename it to "
            "hive_dbfs_staging_path (shared abfss staging for the DBFS-root "
            "two-hop copy). The old key is still honoured for now.",
            DeprecationWarning,
            stacklevel=2,
        )
        return str(old)
    return ""
```
Replace the field declarations (current lines 205-207) with:
```python
    migrate_hive_dbfs_root: bool = False
    hive_dbfs_staging_path: str = ""
```
Replace the parse lines (current lines 292-294) with:
```python
            migrate_hive_dbfs_root=_coerce_bool(raw.get("migrate_hive_dbfs_root")),
            hive_dbfs_staging_path=_coerce_hive_staging_path(raw),
```
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_config.py -v`  Expected: PASS. Also run `uv run ruff check src/common/config.py tests/unit/test_config.py`.)
- [ ] Step 5: Commit
```bash
git add src/common/config.py tests/unit/test_config.py
git commit -m "config: rename hive_dbfs_target_path->hive_dbfs_staging_path, drop hive_target_catalog

Deprecated alias maps the old key with a DeprecationWarning. Removes the
UC-upgrade target-catalog concept for the like-for-like Hive path.

Co-authored-by: Isaac"
```

---

### Task 1b: Retarget test-support fixtures + teardown to `hive_metastore` (config-removal blast radius)

**Files:** Modify `tests/integration/_config_override.py`, `tests/integration/setup_test_config.py`, `tests/integration/seed_hive_test_data.py`, `tests/integration/teardown_hive.py`; Test `tests/unit/test_setup_test_config.py`, `tests/unit/test_teardown_notebooks.py`, `tests/unit/test_idempotency_audit.py`
**Interfaces:**
- Consumes: `MigrationConfig.hive_dbfs_staging_path` (Task 1); `hive_target_catalog` no longer exists.
- Produces: test-support code + their unit guards reference only `hive_dbfs_staging_path`; teardown drops the migrated target `hive_metastore` **database(s)** instead of a UC catalog.

*Why this task exists: Task 1 removes `hive_target_catalog` and renames `hive_dbfs_target_path`. Every test-support module and its unit guard that referenced those keys breaks immediately. Do this right after Task 1 so the tree returns to green before the behavioral work. Run `uv run pytest tests/unit -q` first to enumerate the exact failures.*

- [ ] Step 1: Enumerate the breakage (this is the failing signal)

Run: `uv run pytest tests/unit -q`
Expected: FAIL — `AttributeError: 'MigrationConfig' object has no attribute 'hive_target_catalog'` and key-name assertion failures in `test_setup_test_config.py`, `test_teardown_notebooks.py`, `test_idempotency_audit.py`.
Also enumerate references: `grep -rn "hive_target_catalog\|hive_dbfs_target_path" tests/ src/` and confirm the only remaining hits are the ones this task fixes (source files were handled in later tasks; test-support here).

- [ ] Step 2: Update the unit guards to the new contract

- In `tests/unit/test_setup_test_config.py`: replace every `hive_target_catalog` expectation and any `hive_dbfs_target_path` YAML/assert with `hive_dbfs_staging_path`; drop assertions that a UC target catalog key is emitted.
- In `tests/unit/test_teardown_notebooks.py`: replace the assertion that teardown references/drops `config.hive_target_catalog` with an assertion that `tests/integration/teardown_hive.py` drops the target `hive_metastore` test database — i.e. its source contains `DROP DATABASE IF EXISTS \`hive_metastore\`` (and no `DROP CATALOG`). Concretely:
```python
    def test_teardown_hive_drops_target_hive_metastore_database(self):
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2]
            / "tests" / "integration" / "teardown_hive.py"
        ).read_text()
        assert "DROP DATABASE IF EXISTS `hive_metastore`" in src
        assert "DROP CATALOG" not in src
        assert "hive_target_catalog" not in src
```
- In `tests/unit/test_idempotency_audit.py`: replace `hive_target_catalog` references with `hive_dbfs_staging_path` (or remove the assertion if it only checked the retired rewrite behavior).

- [ ] Step 3: Run guards to verify they fail  (Run: `uv run pytest tests/unit/test_setup_test_config.py tests/unit/test_teardown_notebooks.py tests/unit/test_idempotency_audit.py -v`  Expected: FAIL — source still uses old keys / teardown still drops a catalog.)

- [ ] Step 4: Update the test-support source

- `tests/integration/_config_override.py` + `tests/integration/setup_test_config.py`: rename any `hive_dbfs_target_path` override key → `hive_dbfs_staging_path`; delete any `hive_target_catalog` key. Read each file first; these are dict/YAML builders — change the key strings only.
- `tests/integration/seed_hive_test_data.py`: it uses `config.hive_target_catalog` only to reference where migrated objects LAND for assertions; since the target is now `hive_metastore`, replace those references with the literal `hive_metastore` (the seed itself creates SOURCE `hive_metastore` objects — verify it isn't creating a UC catalog anywhere; if it is, delete that).
- `tests/integration/teardown_hive.py`: **behavior change.** Replace the UC-catalog drop (currently `DROP CATALOG IF EXISTS \`{config.hive_target_catalog}\` CASCADE`) with a drop of the migrated target `hive_metastore` test database(s). The integration test uses the `integration_test_hive` database; drop it on the target via the target warehouse:
```python
# Like-for-like teardown: the migration lands objects in the TARGET
# hive_metastore under the same database names — drop the test database
# there (no UC catalog is created anymore).
for _db in ("integration_test_hive",):
    execute_and_poll(auth, wh_id, f"DROP DATABASE IF EXISTS `hive_metastore`.`{_db}` CASCADE")
```
(Read `teardown_hive.py` for the exact `auth`/`wh_id` variables in scope and match them; keep the source-side hive_metastore cleanup it already does.)

- [ ] Step 5: Run to verify pass  (Run: `uv run pytest tests/unit -q`  Expected: PASS — full unit suite green again. Also `uv run ruff check tests/integration/_config_override.py tests/integration/setup_test_config.py tests/integration/seed_hive_test_data.py tests/integration/teardown_hive.py`.)

- [ ] Step 6: Commit
```bash
git add tests/integration/_config_override.py tests/integration/setup_test_config.py tests/integration/seed_hive_test_data.py tests/integration/teardown_hive.py tests/unit/test_setup_test_config.py tests/unit/test_teardown_notebooks.py tests/unit/test_idempotency_audit.py
git commit -m "test-support: retarget Hive fixtures + teardown to hive_metastore (like-for-like)

Drop hive_target_catalog; rename to hive_dbfs_staging_path; teardown_hive
now drops the migrated target hive_metastore database instead of a UC catalog.

Co-authored-by: Isaac"
```

---

### Task 2: `hive_common` — identity rewrite + `ensure_target_database`
**Files:** Modify `src/migrate/hive_common.py` (functions lines 67-96); Test `tests/unit/test_hive_common.py` (classes lines 22-79)
**Interfaces:**
- Produces: `rewrite_hive_namespace(sql: str, target_catalog: str = "hive_metastore") -> str` (identity), `rewrite_hive_fqn(fqn: str, target_catalog: str = "hive_metastore") -> str` (identity), `ensure_target_database(spark, schema: str) -> None`.
- Consumed by: all hive workers (Tasks 4-10), collision_detection (Task 3 keeps its own local copy).

- [ ] Step 1: Write the failing test — replace `TestRewriteHiveNamespace`, `TestRewriteHiveFqn`, and `TestEnsureTargetCatalogAndSchema` (lines 22-79) in `tests/unit/test_hive_common.py` with:
```python
class TestRewriteHiveNamespaceIsIdentity:
    """Like-for-like: DDL is replayed into hive_metastore as-is, so the
    namespace rewrite must be a no-op (target namespace == source)."""

    def test_leaves_hive_metastore_references_untouched(self):
        sql = "CREATE VIEW `hive_metastore`.`s`.`v` AS SELECT * FROM `hive_metastore`.`x`.`y`"
        assert rewrite_hive_namespace(sql) == sql

    def test_unbackticked_references_untouched(self):
        sql = "SELECT * FROM hive_metastore.schema.table"
        assert rewrite_hive_namespace(sql) == sql

    def test_target_arg_ignored_when_passed(self):
        sql = "SELECT * FROM hive_metastore.s.t"
        assert rewrite_hive_namespace(sql, "anything") == sql


class TestRewriteHiveFqnIsIdentity:
    def test_backticked_fqn_unchanged(self):
        fqn = "`hive_metastore`.`s`.`t`"
        assert rewrite_hive_fqn(fqn) == fqn

    def test_dotted_fqn_unchanged(self):
        assert rewrite_hive_fqn("hive_metastore.s.t") == "hive_metastore.s.t"


class TestEnsureTargetDatabase:
    def test_issues_create_database_in_hive_metastore(self):
        spark = MagicMock()
        ensure_target_database(spark, "sch")
        calls = [c.args[0] for c in spark.sql.call_args_list]
        assert any("CREATE DATABASE IF NOT EXISTS `hive_metastore`.`sch`" in s for s in calls)
        assert not any("CREATE CATALOG" in s for s in calls)
```
Update the import block at the top of the file (lines 13-19) to:
```python
from migrate.hive_common import (
    HIVE_CATALOG,
    HIVE_TO_UC_PRIVILEGES,
    ensure_target_database,
    rewrite_hive_fqn,
    rewrite_hive_namespace,
)
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_hive_common.py -v`  Expected: FAIL — `ImportError: cannot import name 'ensure_target_database'`.)
- [ ] Step 3: Write minimal implementation — in `src/migrate/hive_common.py` replace functions at lines 67-96 with:
```python
def rewrite_hive_namespace(sql: str, target_catalog: str = HIVE_CATALOG) -> str:
    """Like-for-like migration replays DDL into hive_metastore unchanged, so
    this is now an identity function. Kept as a call-site seam (and for the
    ``target_catalog`` signature) while callers are migrated off the rewrite.
    """
    return sql


def rewrite_hive_fqn(fqn: str, target_catalog: str = HIVE_CATALOG) -> str:
    """Identity: the target FQN equals the source FQN (both in hive_metastore)."""
    return fqn


def ensure_target_database(spark, schema: str) -> None:
    """Idempotently create the target database in hive_metastore (like-for-like)."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{HIVE_CATALOG}`.`{schema}`")
```
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_hive_common.py -v`  Expected: PASS. Also `uv run ruff check src/migrate/hive_common.py tests/unit/test_hive_common.py`.)
- [ ] Step 5: Commit
```bash
git add src/migrate/hive_common.py tests/unit/test_hive_common.py
git commit -m "hive_common: identity rewrite + ensure_target_database (hive_metastore)

Retire the hive_metastore->catalog rewrite; replace catalog/schema creation
with CREATE DATABASE IF NOT EXISTS in hive_metastore for like-for-like.

Co-authored-by: Isaac"
```

---

### Task 3: `collision_detection` — identity Hive target FQN, drop `hive_target_catalog`
**Files:** Modify `src/pre_check/collision_detection.py` (`_rewrite_hive_fqn` lines 213-223; `detect_collisions` signature line 240 + body line 288); Modify `src/pre_check/pre_check.py` (call site line 452); Test `tests/unit/test_collision_detection.py` (class ~lines 287-345)
**Interfaces:**
- Produces: `detect_collisions(*, target_client, discovery_rows, existing_status_keys) -> list[dict]` (no `hive_target_catalog`); Hive `target_fqn` = source dotted FQN in `hive_metastore`.

- [ ] Step 1: Write the failing test — replace the Hive collision test class body (around lines 287-345) in `tests/unit/test_collision_detection.py` with a single-namespace version:
```python
class TestHiveCollisionsLikeForLike:
    """Hive source objects land on target in hive_metastore under the SAME
    db/table names — collision probing targets hive_metastore.<db>.<t>."""

    def test_hive_table_probed_in_hive_metastore(self):
        client = MagicMock()
        # tables.get succeeds -> the object already exists on target
        client.tables.get.return_value = object()
        rows = [{
            "object_name": "`hive_metastore`.`db`.`t`",
            "object_type": "hive_table",
            "source_type": "hive",
        }]
        collisions = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys=set(),
        )
        assert len(collisions) == 1
        assert collisions[0]["target_fqn"] == "hive_metastore.db.t"

    def test_detect_collisions_rejects_hive_target_catalog_kwarg(self):
        import inspect
        sig = inspect.signature(detect_collisions)
        assert "hive_target_catalog" not in sig.parameters
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_collision_detection.py -v`  Expected: FAIL — `detect_collisions` still accepts/uses `hive_target_catalog`; `target_fqn` maps to `hive_upgraded.db.t`.)
- [ ] Step 3: Write minimal implementation:

In `src/pre_check/collision_detection.py` replace `_rewrite_hive_fqn` (lines 213-223) with:
```python
def _hive_target_fqn(fqn: str) -> str:
    """Like-for-like: the target FQN is the same hive_metastore.db.t as the
    source (dotted form for the UC SDK ``*.get`` endpoints)."""
    parts = _fqn_to_parts(fqn)
    return ".".join(parts)
```
Change `detect_collisions` signature (line 240) — remove the `hive_target_catalog: str = "hive_upgraded"` parameter — and its Hive branch (line 288) from `target_fqn = _rewrite_hive_fqn(object_name, hive_target_catalog)` to:
```python
            target_fqn = _hive_target_fqn(object_name)
```
Update the docstring (remove the `hive_target_catalog` argument note ~lines 269-270) and the module comment at lines 194-195 to say "land on target in hive_metastore under the same names".

In `src/pre_check/pre_check.py` change the `detect_collisions(...)` call (lines 448-453) to drop the `hive_target_catalog=config.hive_target_catalog,` line (line 452).
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_collision_detection.py tests/unit/test_pre_check.py -v` if the latter exists, else just the first  Expected: PASS. Also `uv run ruff check src/pre_check/collision_detection.py src/pre_check/pre_check.py`.)
- [ ] Step 5: Commit
```bash
git add src/pre_check/collision_detection.py src/pre_check/pre_check.py tests/unit/test_collision_detection.py
git commit -m "collision_detection: probe Hive collisions in hive_metastore (like-for-like)

Drop hive_target_catalog; target FQN == source FQN (hive_metastore.db.t).

Co-authored-by: Isaac"
```

---

### Task 4: `hive_orchestrator` — CREATE DATABASE in hive_metastore (not CREATE CATALOG)
**Files:** Modify `src/migrate/hive_orchestrator.py` (lines 85-107); Test `tests/unit/test_hive_orchestrator.py` (add class) + `tests/unit/test_hive_workers.py` (`TestHiveOrchestratorBatching.test_creates_target_catalog_before_category_batches` lines 447-464)
**Interfaces:**
- Consumes: `inventory_rows` (with `schema_name`), target warehouse via `find_warehouse` + `execute_and_poll`.
- Produces: `CREATE DATABASE IF NOT EXISTS \`hive_metastore\`.\`<schema>\`` per target schema (via target warehouse). #12 anti-join unchanged.

*Notebook module — the main block runs at import gated by `_is_notebook()`, so this uses the source-level guard pattern (see the existing `_source_text()` tests).*

- [ ] Step 1: Write the failing test — append to `tests/unit/test_hive_orchestrator.py`:
```python
class TestHiveOrchestratorCreatesDatabase:
    """Like-for-like: the orchestrator ensures target DATABASES exist in
    hive_metastore, and must NOT create a UC catalog."""

    def test_creates_database_in_hive_metastore(self):
        src = _source_text()
        assert "CREATE DATABASE IF NOT EXISTS `hive_metastore`" in src

    def test_does_not_create_catalog(self):
        src = _source_text()
        assert "CREATE CATALOG" not in src

    def test_no_reference_to_hive_target_catalog(self):
        src = _source_text()
        assert "hive_target_catalog" not in src
```
Also EDIT the now-obsolete `test_creates_target_catalog_before_category_batches` in `tests/unit/test_hive_workers.py` (lines 447-464): replace its body so it asserts database-before-batches ordering:
```python
    def test_creates_target_database_before_category_batches(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_orchestrator.py").read_text()
        create_idx = src.find("CREATE DATABASE IF NOT EXISTS `hive_metastore`")
        last_batch_idx = src.rfind('("hive_external", "hive_managed_nondbfs", "hive_managed_dbfs_root")')
        assert create_idx != -1, "Orchestrator must ensure target databases exist"
        assert last_batch_idx != -1, "Category-iteration tuple not found in orchestrator"
        assert create_idx < last_batch_idx, (
            "Target database creation must precede the populate-path category "
            "iteration — otherwise downstream workers hit NO_SUCH_DATABASE."
        )
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_hive_orchestrator.py tests/unit/test_hive_workers.py -v -k "Database or database or catalog"`  Expected: FAIL — source still contains `CREATE CATALOG` / `hive_target_catalog`.)
- [ ] Step 3: Write minimal implementation — in `src/migrate/hive_orchestrator.py` replace the block at lines 85-107 with:
```python
    # Ensure the target DATABASES exist on the TARGET workspace's hive_metastore
    # via its SQL warehouse (not source spark, which would create them in the
    # wrong metastore). Like-for-like: no UC catalog is created.
    auth = AuthManager(config, dbutils)  # type: ignore[name-defined] # noqa: F821
    wh_id = find_warehouse(auth)
    target_schemas = {r.schema_name for r in inventory_rows if r.schema_name}

    for sch in target_schemas:
        db_sql = f"CREATE DATABASE IF NOT EXISTS `hive_metastore`.`{sch}`"
        res = execute_and_poll(auth, wh_id, db_sql)
        if res["state"] != "SUCCEEDED":
            raise RuntimeError(f"Failed to create target database {sch}: {res.get('error')}")

    logger.info(
        "Target hive_metastore ready with %d database(s).",
        len(target_schemas),
    )
```
Also update the header comment at lines 21-22 (remove the "sets up the target catalog (hive_target_catalog)" sentence, replace with "ensures target databases exist in hive_metastore").
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_hive_orchestrator.py tests/unit/test_hive_workers.py -v`  Expected: PASS. Also `uv run ruff check src/migrate/hive_orchestrator.py`.)
- [ ] Step 5: Commit
```bash
git add src/migrate/hive_orchestrator.py tests/unit/test_hive_orchestrator.py tests/unit/test_hive_workers.py
git commit -m "hive_orchestrator: ensure target databases in hive_metastore (no CREATE CATALOG)

Like-for-like retarget. #12 object_name anti-join left untouched.

Co-authored-by: Isaac"
```

---

### Task 5: `hive_external_worker` — replay DDL into hive_metastore (identity target)
**Files:** Modify `src/migrate/hive_external_worker.py` (imports line 39; lines 82-83, 113-117, header comment lines 20-22); Test `tests/unit/test_hive_workers.py` (`TestHiveExternalWorker`)
**Interfaces:**
- `migrate_hive_external_table(table_info, *, config, auth, explorer, wh_id, tracking_fqn, job_run_id, status_wh_id) -> dict` (signature unchanged); now `target_fqn == source_fqn` and the replayed DDL keeps its `hive_metastore` namespace.

- [ ] Step 1: Write the failing test — replace `TestHiveExternalWorker` (lines 111-125) in `tests/unit/test_hive_workers.py` with a behavioral test:
```python
class TestHiveExternalWorker:
    """Like-for-like: the external table is recreated in hive_metastore with
    the SAME FQN and the replayed DDL keeps its hive_metastore namespace."""

    @patch("migrate.hive_external_worker.append_migration_status_via_warehouse")
    @patch("migrate.hive_external_worker.warehouse_table_count")
    @patch("migrate.hive_external_worker.time")
    @patch("migrate.hive_external_worker.execute_and_poll")
    def test_replays_ddl_into_hive_metastore_unchanged(
        self, mock_exec, mock_time, mock_wh_count, mock_append
    ):
        from migrate.hive_external_worker import migrate_hive_external_table

        mock_time.time.side_effect = [100.0, 105.0]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_wh_count.return_value = 7

        explorer = MagicMock()
        explorer.get_create_statement.return_value = (
            "CREATE TABLE hive_metastore.db.ext (id INT) USING delta "
            "LOCATION 'abfss://ext@acct.dfs.core.windows.net/ext'"
        )
        explorer.get_table_row_count.return_value = 7

        res = migrate_hive_external_table(
            {"object_name": "`hive_metastore`.`db`.`ext`"},
            config=_config_mock(),
            auth=MagicMock(),
            explorer=explorer,
            wh_id="wh",
            tracking_fqn="migration_tracking.cp_migration",
            job_run_id="jr-1",
            status_wh_id="wh-src",
        )

        replayed = mock_exec.call_args[0][2]
        # No namespace rewrite: hive_metastore stays, no hive_upgraded leak.
        assert "hive_metastore.db.ext" in replayed
        assert "hive_upgraded" not in replayed
        # IF NOT EXISTS still injected for resumability.
        assert "CREATE TABLE IF NOT EXISTS" in replayed
        assert res["status"] == "validated"
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_hive_workers.py::TestHiveExternalWorker -v`  Expected: FAIL — worker still rewrites the namespace to `hive_upgraded` via `config.hive_target_catalog` (MagicMock returns a Mock → the rewrite call raises or leaks).)
- [ ] Step 3: Write minimal implementation — in `src/migrate/hive_external_worker.py`:

Change the import (line 39) to drop the rewrites:
```python
from migrate.hive_common import configure_adls_account_key
```
Replace lines 82-83 with:
```python
    source_fqn = table_info["object_name"]
    target_fqn = source_fqn  # like-for-like: same FQN in hive_metastore
```
Replace lines 113-117 (the rewrite + IF NOT EXISTS) with:
```python
    # Like-for-like: replay the DDL as-is into hive_metastore (no rewrite).
    ddl = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")
```
Update the header comment lines 20-22 to describe recreating external tables in the target `hive_metastore` (drop the `{hive_target_catalog}` / "as UC external" wording).
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_hive_workers.py -v`  Expected: PASS. Also `uv run ruff check src/migrate/hive_external_worker.py`.)
- [ ] Step 5: Commit
```bash
git add src/migrate/hive_external_worker.py tests/unit/test_hive_workers.py
git commit -m "hive_external_worker: replay external-table DDL into hive_metastore as-is

Drop namespace rewrite; target FQN == source FQN; keep IF NOT EXISTS.

Co-authored-by: Isaac"
```

---

### Task 6: `hive_managed_nondbfs_worker` — replay into hive_metastore, keep LOCATION clause
**Files:** Modify `src/migrate/hive_managed_nondbfs_worker.py` (imports line 40; lines 105-108, 139-144); Test `tests/unit/test_hive_workers.py` (`TestHiveManagedNondbfsWorker`)
**Interfaces:** `migrate_hive_managed_nondbfs(record, *, config, auth, explorer, wh_id, tracking_fqn, job_run_id, status_wh_id) -> dict` unchanged; target FQN == source FQN; `_ensure_location_clause` retained.

- [ ] Step 1: Write the failing test — add to `TestHiveManagedNondbfsWorker` in `tests/unit/test_hive_workers.py`:
```python
    @patch("migrate.hive_managed_nondbfs_worker.append_migration_status_via_warehouse")
    @patch("migrate.hive_managed_nondbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_nondbfs_worker.time")
    @patch("migrate.hive_managed_nondbfs_worker.execute_and_poll")
    def test_replays_into_hive_metastore_and_keeps_location(
        self, mock_exec, mock_time, mock_wh_count, mock_append
    ):
        from migrate.hive_managed_nondbfs_worker import migrate_hive_managed_nondbfs

        mock_time.time.side_effect = [100.0, 105.0]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_wh_count.return_value = 3

        rec = self._orchestrator_record()
        explorer = MagicMock()
        # SHOW CREATE with no LOCATION -> worker must inject storage_location.
        explorer.get_create_statement.return_value = (
            "CREATE TABLE hive_metastore.integration_test_hive.nondbfs_sales (id INT) USING delta"
        )
        explorer.get_table_row_count.return_value = 3

        res = migrate_hive_managed_nondbfs(
            rec, config=_config_mock(), auth=MagicMock(), explorer=explorer,
            wh_id="wh", tracking_fqn="migration_tracking.cp_migration",
            job_run_id="jr-1", status_wh_id="wh-src",
        )
        replayed = mock_exec.call_args[0][2]
        assert "hive_metastore.integration_test_hive.nondbfs_sales" in replayed
        assert "hive_upgraded" not in replayed
        assert "LOCATION 'abfss://ext@acct.dfs.core.windows.net/nondbfs_sales'" in replayed
        assert res["status"] == "validated"
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest "tests/unit/test_hive_workers.py::TestHiveManagedNondbfsWorker::test_replays_into_hive_metastore_and_keeps_location" -v`  Expected: FAIL — worker rewrites namespace via `config.hive_target_catalog`.)
- [ ] Step 3: Write minimal implementation — in `src/migrate/hive_managed_nondbfs_worker.py`:

Change the import (line 40) to:
```python
from migrate.hive_common import configure_adls_account_key
```
Replace lines 105-108 with:
```python
    source_fqn = record["object_name"]
    storage_location = record.get("storage_location", "")
    provider = (record.get("provider") or "").lower()
    target_fqn = source_fqn  # like-for-like: same FQN in hive_metastore
```
Replace lines 139-144 with (drop the namespace rewrite, keep IF NOT EXISTS + location clause):
```python
    # Like-for-like: replay as-is into hive_metastore (no namespace rewrite).
    ddl = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")
    # Force a LOCATION so the managed source lands as a located table on target.
    ddl = _ensure_location_clause(ddl, storage_location)
```
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_hive_workers.py -v`  Expected: PASS. Also `uv run ruff check src/migrate/hive_managed_nondbfs_worker.py`.)
- [ ] Step 5: Commit
```bash
git add src/migrate/hive_managed_nondbfs_worker.py tests/unit/test_hive_workers.py
git commit -m "hive_managed_nondbfs_worker: replay into hive_metastore, keep LOCATION clause

Drop namespace rewrite; target FQN == source FQN; location-forcing retained.

Co-authored-by: Isaac"
```

---

### Task 7: `hive_views_worker` — replay view DDL into hive_metastore
**Files:** Modify `src/migrate/hive_views_worker.py` (import line 33; lines 111, 196-198); Test `tests/unit/test_hive_workers.py` (`TestHiveViewsWorker`)
**Interfaces:** `migrate_hive_view(view_info, ddl, *, config, auth, wh_id) -> dict` unchanged; target view FQN == source FQN in hive_metastore; view body replayed unchanged.

- [ ] Step 1: Write the failing test — replace `test_rewrites_hive_metastore_references_to_target_catalog` (lines 38-63) in `tests/unit/test_hive_workers.py` with:
```python
    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_replays_view_ddl_into_hive_metastore_unchanged(self, mock_execute, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        ddl = (
            "CREATE OR REPLACE VIEW `hive_metastore`.`integration_test_hive`.`big_orders` AS "
            "SELECT * FROM hive_metastore.integration_test_hive.managed_orders WHERE amount > 15"
        )
        cfg = _config_mock()
        migrate_hive_view(
            {"object_name": "`hive_metastore`.`integration_test_hive`.`big_orders`"},
            ddl, config=cfg, auth=MagicMock(), wh_id="wh-hv",
        )
        replayed = mock_execute.call_args[0][2]
        assert "hive_upgraded" not in replayed
        assert "hive_metastore.integration_test_hive.managed_orders" in replayed
        assert replayed.startswith("CREATE OR REPLACE VIEW")
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_hive_workers.py::TestHiveViewsWorker -v`  Expected: FAIL — worker calls `rewrite_hive_namespace(ddl, config.hive_target_catalog)` with a MagicMock catalog, leaking a Mock into the DDL.)
- [ ] Step 3: Write minimal implementation — in `src/migrate/hive_views_worker.py`:

Remove the `rewrite_hive_namespace` import (line 33) — the module no longer needs it (drop the whole `from migrate.hive_common import rewrite_hive_namespace` line).
Replace line 111 with:
```python
    rewritten = ddl  # like-for-like: replay view DDL into hive_metastore as-is
```
Replace the target-header construction at lines 196-198 (inside `run()`'s `for v in views_raw` loop) with the hive_metastore identity form:
```python
            parts = source_fqn.strip("`").split("`.`")
            target_fqn = f"`hive_metastore`.`{parts[1]}`.`{parts[2]}`"
            ddls[source_fqn] = f"CREATE OR REPLACE VIEW {target_fqn} AS {body}"
```
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_hive_workers.py -v`  Expected: PASS. Also `uv run ruff check src/migrate/hive_views_worker.py`.)
- [ ] Step 5: Commit
```bash
git add src/migrate/hive_views_worker.py tests/unit/test_hive_workers.py
git commit -m "hive_views_worker: replay view DDL into hive_metastore (no rewrite)

Co-authored-by: Isaac"
```

---

### Task 8: `hive_functions_worker` — replay function DDL into hive_metastore
**Files:** Modify `src/migrate/hive_functions_worker.py` (import line 33; lines 143-144); Test `tests/unit/test_hive_workers.py` (`TestHiveFunctionsWorker`)
**Interfaces:** `migrate_hive_function(func_info, *, config, auth, tracker, spark, wh_id) -> dict` unchanged; DDL replayed unchanged into hive_metastore.

- [ ] Step 1: Write the failing test — replace `TestHiveFunctionsWorker` (lines 133-137) in `tests/unit/test_hive_workers.py` with:
```python
class TestHiveFunctionsWorker:
    def test_module_imports_cleanly(self):
        from migrate import hive_functions_worker

        assert hasattr(hive_functions_worker, "run")

    @patch("migrate.hive_functions_worker.get_hive_function_ddl")
    @patch("migrate.hive_functions_worker.time")
    @patch("migrate.hive_functions_worker.execute_and_poll")
    def test_replays_function_ddl_into_hive_metastore_unchanged(
        self, mock_execute, mock_time, mock_ddl
    ):
        from migrate.hive_functions_worker import migrate_hive_function

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_ddl.return_value = (
            "CREATE FUNCTION hive_metastore.db.triple(x DOUBLE) RETURNS DOUBLE RETURN x * 3"
        )

        res = migrate_hive_function(
            {"object_name": "`hive_metastore`.`db`.`triple`"},
            config=_config_mock(), auth=MagicMock(), tracker=MagicMock(),
            spark=MagicMock(), wh_id="wh",
        )
        replayed = mock_execute.call_args[0][2]
        assert "hive_metastore.db.triple" in replayed
        assert "hive_upgraded" not in replayed
        assert replayed.startswith("CREATE OR REPLACE FUNCTION")
        assert res["status"] == "validated"
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_hive_workers.py::TestHiveFunctionsWorker -v`  Expected: FAIL — worker calls `rewrite_hive_namespace(ddl, config.hive_target_catalog)`.)
- [ ] Step 3: Write minimal implementation — in `src/migrate/hive_functions_worker.py`:

Remove the `from migrate.hive_common import rewrite_hive_namespace` import (line 33).
Replace lines 143-144 with:
```python
    # Like-for-like: replay the function DDL into hive_metastore unchanged.
```
(i.e. delete the `ddl = rewrite_hive_namespace(...)` line; the next line `ddl = rewrite_ddl(ddl, r"CREATE\s+FUNCTION\b", "CREATE OR REPLACE FUNCTION")` at line 147 stays.)
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_hive_workers.py -v`  Expected: PASS. Also `uv run ruff check src/migrate/hive_functions_worker.py`.)
- [ ] Step 5: Commit
```bash
git add src/migrate/hive_functions_worker.py tests/unit/test_hive_workers.py
git commit -m "hive_functions_worker: replay function DDL into hive_metastore (no rewrite)

Co-authored-by: Isaac"
```

---

## Phase P2 — DBFS-root two-hop staging copy

### Task 9: `hive_managed_dbfs_worker` — two-hop staging into target's DBFS root (MANAGED)
**Files:** Modify `src/migrate/hive_managed_dbfs_worker.py` (header lines 19-22; `migrate_hive_managed_dbfs` lines 83-237); Test `tests/unit/test_hive_workers.py` (`TestHiveManagedDbfsWorker`)
**Interfaces:**
- `migrate_hive_managed_dbfs(table_info, *, config, auth, tracker, spark, wh_id) -> dict` (signature unchanged).
- New pure helper `_staging_ctas_sql(db: str, table: str, staging_path: str, partition_cols: list[str]) -> str` — builds the target-side `CREATE TABLE hive_metastore.\`db\`.\`table\` USING DELTA [PARTITIONED BY (...)] AS SELECT * FROM delta.\`<staging_path>\`` (managed, NO LOCATION).
- Gating: `migrate_hive_dbfs_root` + `hive_dbfs_staging_path`. STAGE 1 = source `df.write` to `hive_dbfs_staging_path/db/table/`; STAGE 2 = target CTAS via warehouse. Validation compares source count to target managed count (`warehouse_table_count`).

- [ ] Step 1: Write the failing test — add these tests (and update `_dbfs_config` + `test_module_uses_hive_dbfs_target_path_config`) in `tests/unit/test_hive_workers.py`. First replace the module-source test (lines 189-195) and `_dbfs_config` (lines 233-238):
```python
    def test_module_uses_staging_path_config(self):
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_managed_dbfs_worker.py"
        ).read_text()
        assert "hive_dbfs_staging_path" in src
        assert "hive_dbfs_target_path" not in src

    def test_target_table_is_managed_no_location(self):
        """STAGE 2 lands a MANAGED table in the target DBFS root — the target
        CREATE TABLE must NOT carry a LOCATION clause (that would make it
        external and defeat the DBFS-root rehome)."""
        from migrate.hive_managed_dbfs_worker import _staging_ctas_sql

        sql = _staging_ctas_sql("db", "t", "abfss://stage@a.dfs.core.windows.net/hive", [])
        assert "LOCATION" not in sql.upper()
        assert sql.startswith("CREATE TABLE `hive_metastore`.`db`.`t`")
        assert "USING DELTA" in sql.upper()
        assert "delta.`abfss://stage@a.dfs.core.windows.net/hive/db/t/`" in sql

    def test_ctas_sql_preserves_partitions(self):
        from migrate.hive_managed_dbfs_worker import _staging_ctas_sql

        sql = _staging_ctas_sql("db", "t", "abfss://s@a.dfs.core.windows.net/h", ["country", "yr"])
        assert "PARTITIONED BY (`country`, `yr`)" in sql

    def _dbfs_config(self):
        cfg = _config_mock()
        cfg.migrate_hive_dbfs_root = True
        cfg.hive_dbfs_staging_path = "abfss://stage@acct.dfs.core.windows.net/hive_stage"
        return cfg
```
Then replace the three end-to-end DBFS tests (partitioned/unpartitioned/validation, lines 240-320) with two-hop versions that assert STAGE 1 writes to staging and STAGE 2 CTAS runs via the warehouse:
```python
    @patch("migrate.hive_managed_dbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_dbfs_worker.time")
    @patch("migrate.hive_managed_dbfs_worker.execute_and_poll")
    def test_two_hop_stage_then_target_managed_ctas(self, mock_exec, mock_time, mock_wh_count):
        from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_wh_count.return_value = 5  # target managed count (via target warehouse)

        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 5
        spark.read.table.return_value = df
        spark.sql.return_value.collect.return_value = self._describe_rows(("id", "int"))

        res = migrate_hive_managed_dbfs(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=self._dbfs_config(), auth=MagicMock(), tracker=MagicMock(),
            spark=spark, wh_id="wh",
        )

        # STAGE 1: wrote df to the shared staging path (not the final home).
        staged = df.write.mode.return_value.format.return_value.save.call_args[0][0]
        assert staged == "abfss://stage@acct.dfs.core.windows.net/hive_stage/db/t/"
        # STAGE 2: target-side managed CTAS ran via the warehouse.
        ctas = mock_exec.call_args[0][2]
        assert ctas.startswith("CREATE TABLE `hive_metastore`.`db`.`t`")
        assert "LOCATION" not in ctas.upper()
        assert res["status"] == "validated"
        assert res["target_row_count"] == 5

    @patch("migrate.hive_managed_dbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_dbfs_worker.time")
    @patch("migrate.hive_managed_dbfs_worker.execute_and_poll")
    def test_target_count_mismatch_is_validation_failed(self, mock_exec, mock_time, mock_wh_count):
        from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_wh_count.return_value = 3  # target has fewer rows than source

        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 5
        spark.read.table.return_value = df
        spark.sql.return_value.collect.return_value = self._describe_rows(("id", "int"))

        res = migrate_hive_managed_dbfs(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=self._dbfs_config(), auth=MagicMock(), tracker=MagicMock(),
            spark=spark, wh_id="wh",
        )
        assert res["status"] == "validation_failed"
        assert res["source_row_count"] == 5
        assert res["target_row_count"] == 3

    @patch("migrate.hive_managed_dbfs_worker.time")
    def test_missing_staging_path_fails_fast(self, mock_time):
        from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs

        mock_time.time.side_effect = [100.0, 100.0]
        cfg = self._dbfs_config()
        cfg.hive_dbfs_staging_path = ""
        res = migrate_hive_managed_dbfs(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=cfg, auth=MagicMock(), tracker=MagicMock(),
            spark=MagicMock(), wh_id="wh",
        )
        assert res["status"] == "failed"
        assert "hive_dbfs_staging_path" in res["error_message"]
```
Add `from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs` imports inline as shown; ensure `warehouse_table_count` is importable in the worker (Step 3 adds it).
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_hive_workers.py::TestHiveManagedDbfsWorker -v`  Expected: FAIL — `_staging_ctas_sql` doesn't exist; worker still writes to `hive_dbfs_target_path` and registers a UC external table via LOCATION.)
- [ ] Step 3: Write minimal implementation — in `src/migrate/hive_managed_dbfs_worker.py`:

Update the header comment (lines 19-22) to describe the two-hop staging flow. Add `warehouse_table_count` to the `common.sql_utils` import (line 30):
```python
from common.sql_utils import execute_and_poll, find_warehouse, warehouse_table_count
```
Add the pure helper after `_source_partition_columns` (after line 76):
```python
def _staging_ctas_sql(db: str, table: str, staging_path: str, partition_cols: list[str]) -> str:
    """Target-side CTAS that lands a MANAGED table in the target DBFS root.

    Reads the two-hop staging Delta directory and writes a managed table (NO
    LOCATION) in hive_metastore so it lands in the target's own DBFS root.
    Partition columns are preserved via ``PARTITIONED BY``.
    """
    src = f"{staging_path.rstrip('/')}/{db}/{table}/"
    parts = ""
    if partition_cols:
        cols = ", ".join(f"`{c}`" for c in partition_cols)
        parts = f" PARTITIONED BY ({cols})"
    return (
        f"CREATE TABLE `hive_metastore`.`{db}`.`{table}` USING DELTA{parts} "
        f"AS SELECT * FROM delta.`{src}`"
    )
```
Rewrite the body of `migrate_hive_managed_dbfs` (lines 92-237). Keep sections A (opt-out `migrate_hive_dbfs_root`), the in_progress tracker row, dry_run handling, and obj_name parsing. Change section B to gate on `hive_dbfs_staging_path`; replace sections C/D/E:
```python
    """Two-hop staging copy of a Hive DBFS-root managed table into the target's
    own DBFS root (like-for-like: stays managed in hive_metastore)."""
    obj_name = table_info["object_name"]

    # A. Opt-out check
    if not config.migrate_hive_dbfs_root:
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "skipped_by_config",
            "error_message": "migrate_hive_dbfs_root=false",
            "duration_seconds": 0.0,
        }

    # B. Config validation (defensive — pre-check should catch this)
    if not config.hive_dbfs_staging_path:
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": "hive_dbfs_staging_path required but not set",
            "duration_seconds": 0.0,
        }

    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "hive_managed_dbfs_root",
                "status": "in_progress",
                "error_message": None,
                "job_run_id": None,
                "task_run_id": None,
                "source_row_count": None,
                "target_row_count": None,
                "duration_seconds": None,
            }
        ]
    )

    start = time.time()

    try:
        parts = obj_name.strip("`").split("`.`")
        if len(parts) != 3:
            raise ValueError(f"Expected 3-part name, got {len(parts)} parts: {obj_name}")
        _, db, table = parts
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": f"Failed to parse object_name: {exc}",
            "duration_seconds": duration,
        }

    staging_path = f"{config.hive_dbfs_staging_path.rstrip('/')}/{db}/{table}/"

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would stage %s to %s then CTAS into target DBFS root", obj_name, staging_path)
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    # STAGE 1: source-side write of table data to the shared abfss staging path
    # (reachable by both workspaces). Preserve partition layout (finding #4).
    try:
        logger.info("Reading source table %s", obj_name)
        df = spark.read.table(f"hive_metastore.`{db}`.`{table}`")
        source_row_count = df.count()
        partition_cols = _source_partition_columns(spark, db, table)
        writer = df.write.mode("overwrite").format("delta")
        if partition_cols:
            logger.info("Preserving partition columns %s for %s", partition_cols, obj_name)
            writer = writer.partitionBy(*partition_cols)
        logger.info("STAGE 1: writing %d rows to staging %s", source_row_count, staging_path)
        writer.save(staging_path)
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": f"Staging write failed: {exc}",
            "duration_seconds": duration,
        }

    # STAGE 2: target-side CTAS that reads staging and writes a MANAGED table
    # into the target's own DBFS root (no LOCATION), via the TARGET warehouse.
    target_fqn = f"`hive_metastore`.`{db}`.`{table}`"
    ctas_sql = _staging_ctas_sql(db, table, config.hive_dbfs_staging_path, partition_cols)
    logger.info("STAGE 2: creating managed target table %s from staging", target_fqn)
    result = execute_and_poll(auth, wh_id, ctas_sql)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "source_row_count": source_row_count,
            "duration_seconds": duration,
        }

    # Validate: compare the source row count to the TARGET managed table count
    # (read through the target warehouse — the target metastore isn't visible
    # to this worker's spark session).
    target_row_count = None
    try:
        target_row_count = warehouse_table_count(auth, wh_id, target_fqn)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read target count for %s: %s", target_fqn, exc)

    if isinstance(target_row_count, int) and target_row_count != source_row_count:
        return {
            "object_name": obj_name,
            "object_type": "hive_managed_dbfs_root",
            "status": "validation_failed",
            "error_message": (
                f"Row count mismatch after target CTAS: source {source_row_count}, "
                f"target managed table has {target_row_count}"
            ),
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "duration_seconds": duration,
        }

    return {
        "object_name": obj_name,
        "object_type": "hive_managed_dbfs_root",
        "status": "validated",
        "error_message": None,
        "source_row_count": source_row_count,
        "target_row_count": target_row_count if isinstance(target_row_count, int) else source_row_count,
        "duration_seconds": duration,
    }
```
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_hive_workers.py -v`  Expected: PASS. Also `uv run ruff check src/migrate/hive_managed_dbfs_worker.py`.)
- [ ] Step 5: Commit
```bash
git add src/migrate/hive_managed_dbfs_worker.py tests/unit/test_hive_workers.py
git commit -m "hive_managed_dbfs_worker: two-hop staging into target DBFS root (managed)

STAGE 1 source-writes to shared abfss staging; STAGE 2 target CTAS lands a
MANAGED table (no LOCATION) in the target DBFS root. Validate against the
target managed count. Gated on migrate_hive_dbfs_root + hive_dbfs_staging_path.

Co-authored-by: Isaac"
```

---

## Phase P3 — Grants + ownership

### Task 10: `hive_grants_worker` — grant-before-transfer, skip-if-already-owned, drop catalog ownership
**Files:** Modify `src/migrate/hive_grants_worker.py` (import line 31; `_emit_grant` lines 71-137; `_process_show_grants_rows` lines 189-240; `run` lines 247-339); Test `tests/unit/test_hive_workers.py` (`TestHiveGrantsWorker`)
**Interfaces:**
- New pure helper `_should_skip_owner_transfer(current_owner: str | None, target_principal: str) -> bool`.
- New helper `_current_owner(auth, wh_id, securable_keyword: str, target_fqn: str) -> str | None` (reads owner via warehouse `DESCRIBE ... EXTENDED`).
- `_emit_grant(..., spn_client_id: str = "")` gains a pre-transfer `GRANT USAGE, CREATE ON SCHEMA … TO <spn>` for SCHEMA OWN rows and a skip-if-owned short-circuit; `run()` skips OWN at CATALOG level and uses `spn_client_id`.

- [ ] Step 1: Write the failing test — replace `TestHiveGrantsWorker` (lines 145-175) in `tests/unit/test_hive_workers.py` with:
```python
class TestHiveGrantsWorker:
    def test_source_uses_hive_to_uc_privileges_map(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_grants_worker.py").read_text()
        assert "HIVE_TO_UC_PRIVILEGES" in src

    def test_object_type_map_covers_all_hive_table_categories(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_grants_worker.py").read_text()
        for hive_type in (
            "hive_external", "hive_managed_dbfs_root", "hive_managed_nondbfs",
            "hive_view", "hive_function",
        ):
            assert f'"{hive_type}"' in src

    def test_skip_decision_when_already_owned(self):
        from migrate.hive_grants_worker import _should_skip_owner_transfer

        assert _should_skip_owner_transfer("alice@corp.com", "alice@corp.com") is True
        assert _should_skip_owner_transfer("ALICE@corp.com", "alice@corp.com") is True
        assert _should_skip_owner_transfer("bob@corp.com", "alice@corp.com") is False
        assert _should_skip_owner_transfer(None, "alice@corp.com") is False

    @patch("migrate.hive_grants_worker._current_owner")
    @patch("migrate.hive_grants_worker.time")
    @patch("migrate.hive_grants_worker.execute_and_poll")
    def test_schema_own_grants_before_transfer(self, mock_exec, mock_time, mock_owner):
        """SCHEMA OWN must GRANT USAGE, CREATE to the SPN BEFORE ALTER OWNER."""
        from migrate.hive_grants_worker import _emit_grant

        mock_time.time.side_effect = [100.0, 100.1, 100.2, 100.3]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        mock_owner.return_value = "someoneelse@corp.com"  # not yet owned by target

        _emit_grant(
            action_type="OWN", securable_keyword="SCHEMA",
            target_fqn="`hive_metastore`.`db`", principal="alice@corp.com",
            auth=MagicMock(), wh_id="wh", dry_run=False,
            transfer_ownership=True, spn_client_id="spn-123",
        )
        executed = [c.args[2] for c in mock_exec.call_args_list]
        grant_idx = next(i for i, s in enumerate(executed) if s.startswith("GRANT USAGE, CREATE ON SCHEMA"))
        owner_idx = next(i for i, s in enumerate(executed) if s.startswith("ALTER SCHEMA"))
        assert "`hive_metastore`.`db`" in executed[grant_idx]
        assert "`spn-123`" in executed[grant_idx]
        assert grant_idx < owner_idx, "GRANT USAGE, CREATE must precede ALTER OWNER"

    @patch("migrate.hive_grants_worker._current_owner")
    @patch("migrate.hive_grants_worker.time")
    @patch("migrate.hive_grants_worker.execute_and_poll")
    def test_owner_transfer_skipped_when_already_owned(self, mock_exec, mock_time, mock_owner):
        from migrate.hive_grants_worker import _emit_grant

        mock_time.time.side_effect = [100.0, 100.1]
        mock_owner.return_value = "alice@corp.com"  # target already owns
        res = _emit_grant(
            action_type="OWN", securable_keyword="SCHEMA",
            target_fqn="`hive_metastore`.`db`", principal="alice@corp.com",
            auth=MagicMock(), wh_id="wh", dry_run=False,
            transfer_ownership=True, spn_client_id="spn-123",
        )
        assert res["status"] == "skipped"
        assert "already owned" in res["error_message"].lower()
        assert not any(c.args[2].startswith("ALTER SCHEMA") for c in mock_exec.call_args_list)

    def test_run_skips_own_at_catalog_level(self):
        """The built-in hive_metastore catalog ownership is never transferred."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_grants_worker.py").read_text()
        # No hive_target_catalog anywhere; catalog branch does not transfer OWN.
        assert "hive_target_catalog" not in src
        assert "hive_metastore catalog ownership not transferred" in src
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_hive_workers.py::TestHiveGrantsWorker -v`  Expected: FAIL — `_should_skip_owner_transfer` / `_current_owner` / `spn_client_id` param / catalog-skip do not exist yet; `hive_target_catalog` still referenced.)
- [ ] Step 3: Write minimal implementation — in `src/migrate/hive_grants_worker.py`:

Change the import (line 31) to drop `rewrite_hive_fqn` (target FQN == source FQN now):
```python
from migrate.hive_common import HIVE_TO_UC_PRIVILEGES
```
Add the two helpers before `_emit_grant` (after line 69):
```python
def _should_skip_owner_transfer(current_owner: str | None, target_principal: str) -> bool:
    """Skip the ALTER … OWNER TO when the target already owns the securable —
    makes re-runs idempotent (finding #13: no re-transfer failure)."""
    if not current_owner:
        return False
    return current_owner.strip().lower() == target_principal.strip().lower()


_OWNER_DESCRIBE = {
    "CATALOG": "DESCRIBE CATALOG EXTENDED {fqn}",
    "SCHEMA": "DESCRIBE SCHEMA EXTENDED {fqn}",
    "TABLE": "DESCRIBE TABLE EXTENDED {fqn}",
    "VIEW": "DESCRIBE TABLE EXTENDED {fqn}",
    "FUNCTION": "DESCRIBE FUNCTION EXTENDED {fqn}",
}


def _current_owner(auth: AuthManager, wh_id: str, securable_keyword: str, target_fqn: str) -> str | None:
    """Read the current owner of a securable via the target warehouse.

    Parses the ``Owner`` row from ``DESCRIBE … EXTENDED``. Best-effort: any
    failure returns None (caller then proceeds with the transfer).
    """
    tmpl = _OWNER_DESCRIBE.get(securable_keyword)
    if not tmpl:
        return None
    res = execute_and_poll(auth, wh_id, tmpl.format(fqn=target_fqn))
    if res.get("state") != "SUCCEEDED":
        return None
    for row in res.get("rows", []) or []:
        cells = [str(c) if c is not None else "" for c in row]
        for i, c in enumerate(cells):
            if c.strip().lower() in ("owner", "table owner") and i + 1 < len(cells):
                return cells[i + 1].strip() or None
    return None
```
In `_emit_grant` (signature line 71-81) add `spn_client_id: str = ""` to the keyword args. Replace the OWN branch (lines 87-137) so that, for `transfer_ownership=True`:
1. If `securable_keyword == "CATALOG"`: return a `skipped` row with message "hive_metastore catalog ownership not transferred (#14 moot)" and do nothing.
2. Query `owner = _current_owner(auth, wh_id, securable_keyword, target_fqn)`; if `_should_skip_owner_transfer(owner, principal)` return a `skipped` row with error_message `"already owned by target principal"`.
3. If `securable_keyword == "SCHEMA"` and `spn_client_id`: emit `GRANT USAGE, CREATE ON SCHEMA {target_fqn} TO \`{spn_client_id}\`` via `execute_and_poll` (grant-before-transfer) before the `ALTER`.
4. Then run the existing `ALTER {securable_keyword} {target_fqn} OWNER TO \`{principal}\`` and return validated/failed as today. (Keep the dry_run branch: log both the GRANT and the ALTER.)

Concretely, replace lines 87-137 with:
```python
    if action_type.upper() == "OWN":
        if not transfer_ownership:
            logger.info(
                "transfer_ownership=False: leaving %s %s owned by the migration SPN.",
                securable_keyword, target_fqn,
            )
            return {
                "object_name": obj_key, "object_type": "hive_grant",
                "status": "skipped", "error_message": "transfer_ownership disabled",
                "duration_seconds": 0.0,
            }
        owner_obj_key = f"OWNER_{securable_keyword}_{target_fqn}_{principal}"
        # #14 moot: never transfer ownership of the built-in hive_metastore catalog.
        if securable_keyword == "CATALOG":
            return {
                "object_name": owner_obj_key, "object_type": "hive_grant",
                "status": "skipped",
                "error_message": "hive_metastore catalog ownership not transferred (built-in)",
                "duration_seconds": 0.0,
            }
        owner_sql = f"ALTER {securable_keyword} {target_fqn} OWNER TO `{principal}`"
        grant_sql = None
        if securable_keyword == "SCHEMA" and spn_client_id:
            grant_sql = f"GRANT USAGE, CREATE ON SCHEMA {target_fqn} TO `{spn_client_id}`"
        if dry_run:
            if grant_sql:
                logger.info("[DRY RUN] Would grant-before-transfer: %s", grant_sql)
            logger.info("[DRY RUN] Would transfer ownership: %s", owner_sql)
            return {
                "object_name": owner_obj_key, "object_type": "hive_grant",
                "status": "skipped", "error_message": "dry_run", "duration_seconds": 0.0,
            }
        start = time.time()
        # skip-if-already-owned (finding #13): idempotent re-runs.
        current = _current_owner(auth, wh_id, securable_keyword, target_fqn)
        if _should_skip_owner_transfer(current, principal):
            return {
                "object_name": owner_obj_key, "object_type": "hive_grant",
                "status": "skipped",
                "error_message": f"already owned by target principal {principal!r}",
                "duration_seconds": time.time() - start,
            }
        # grant-before-transfer: keep SPN CREATE on the schema after handing
        # ownership back to the original owner (finding #13 lockout).
        if grant_sql:
            logger.info("Grant-before-transfer: %s", grant_sql)
            g_res = execute_and_poll(auth, wh_id, grant_sql)
            if g_res["state"] != "SUCCEEDED":
                return {
                    "object_name": owner_obj_key, "object_type": "hive_grant",
                    "status": "failed",
                    "error_message": f"grant-before-transfer failed: {g_res.get('error', g_res['state'])}",
                    "duration_seconds": time.time() - start,
                }
        logger.info("Transferring ownership: %s", owner_sql)
        result = execute_and_poll(auth, wh_id, owner_sql)
        duration = time.time() - start
        if result["state"] == "SUCCEEDED":
            return {
                "object_name": owner_obj_key, "object_type": "hive_grant",
                "status": "validated", "error_message": None, "duration_seconds": duration,
            }
        return {
            "object_name": owner_obj_key, "object_type": "hive_grant",
            "status": "failed",
            "error_message": (
                f"Ownership transfer to '{principal}' failed "
                f"(does the principal exist on target?): {result.get('error', result['state'])}"
            ),
            "duration_seconds": duration,
        }
```
In `_process_show_grants_rows` (lines 189-240) add `spn_client_id: str = ""` to the signature and thread it through both `_emit_grant(...)` calls (the non-OWN loop and the deferred_own loop).
In `run()` (lines 247-339): remove `target_catalog = config.hive_target_catalog` (line 255); set `spn_client_id = config.spn_client_id`. In the catalog-level block (lines 261-276) set `target_fqn = "\`hive_metastore\`"` and pass `spn_client_id=spn_client_id` (the OWN row is skipped inside `_emit_grant` for CATALOG). In the schema block (lines 298-310) set `target_schema_fqn = f"\`hive_metastore\`.\`{schema_name}\`"` and pass `transfer_ownership=transfer_ownership, spn_client_id=spn_client_id`. In the object block (lines 324-337) set `target_obj_fqn = object_name` (identity) and pass `spn_client_id=spn_client_id`.
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_hive_workers.py -v`  Expected: PASS. Also `uv run ruff check src/migrate/hive_grants_worker.py`.)
- [ ] Step 5: Commit
```bash
git add src/migrate/hive_grants_worker.py tests/unit/test_hive_workers.py
git commit -m "hive_grants_worker: grant-before-transfer + skip-if-owned; drop catalog ownership

Replays grants/ownership into hive_metastore. GRANT USAGE, CREATE to the SPN
before ALTER SCHEMA OWNER (findings #13); skip transfer when target already
owns; never transfer the built-in hive_metastore catalog (#14 moot).

Co-authored-by: Isaac"
```

---

## Phase P4 — Discovery, pre_check guards, dashboard

### Task 11: Discovery — `/mnt` mount-prerequisite markers
**Files:** Modify `src/discovery/discovery.py` (`_discover_hive` lines 461-512); Test `tests/unit/test_discovery_hive_markers.py` (new)
**Interfaces:**
- New pure helper `mount_name_from_location(storage_location: str | None) -> str | None` (returns the `/mnt/<name>` mount name, else None).
- `_discover_hive` emits one `discovery_row(object_type="mount_prerequisite", ...)` per distinct required mount, with `metadata={"mount": <name>, "tables": [...]}`.

- [ ] Step 1: Write the failing test — create `tests/unit/test_discovery_hive_markers.py`:
```python
"""Unit tests for /mnt mount-prerequisite markers emitted by discovery."""

from __future__ import annotations

from unittest.mock import MagicMock

from discovery.discovery import _discover_hive, mount_name_from_location


class TestMountNameFromLocation:
    def test_extracts_mount_name(self):
        assert mount_name_from_location("dbfs:/mnt/salesraw/tbl") == "salesraw"
        assert mount_name_from_location("dbfs:/mnt/salesraw") == "salesraw"

    def test_none_for_non_mount(self):
        for loc in (None, "", "abfss://c@a.dfs.core.windows.net/x", "dbfs:/user/hive/warehouse/t"):
            assert mount_name_from_location(loc) is None


class TestDiscoverHiveMountMarkers:
    def _explorer(self):
        explorer = MagicMock()
        explorer.list_hive_databases.return_value = ["db1"]
        explorer.classify_hive_tables.return_value = [
            {
                "fqn": "`hive_metastore`.`db1`.`mnt_tbl`",
                "object_type": "hive_table",
                "table_type": "EXTERNAL",
                "storage_location": "dbfs:/mnt/salesraw/mnt_tbl",
                "provider": "delta",
                "data_category": "hive_external",
            },
        ]
        explorer.list_hive_functions.return_value = []
        explorer.get_table_row_count.return_value = 0
        explorer.get_table_size_bytes.return_value = 0
        return explorer

    def test_emits_mount_prerequisite_marker(self):
        rows = _discover_hive(config=MagicMock(), explorer=self._explorer(), now="2026-07-16")
        markers = [r for r in rows if r["object_type"] == "mount_prerequisite"]
        assert len(markers) == 1
        import json
        meta = json.loads(markers[0]["metadata_json"])
        assert meta["mount"] == "salesraw"
        assert "`hive_metastore`.`db1`.`mnt_tbl`" in meta["tables"]

    def test_no_marker_when_no_mount_tables(self):
        explorer = self._explorer()
        explorer.classify_hive_tables.return_value[0]["storage_location"] = (
            "abfss://c@a.dfs.core.windows.net/x"
        )
        rows = _discover_hive(config=MagicMock(), explorer=explorer, now="2026-07-16")
        assert not any(r["object_type"] == "mount_prerequisite" for r in rows)
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_discovery_hive_markers.py -v`  Expected: FAIL — `mount_name_from_location` doesn't exist; no marker rows.)
- [ ] Step 3: Write minimal implementation — in `src/discovery/discovery.py`:

Add the pure helper above `_discover_hive` (before line 461):
```python
def mount_name_from_location(storage_location: str | None) -> str | None:
    """Return the ``/mnt/<name>`` mount name for a DBFS-mount-backed table, else None.

    ``/mnt``-backed Hive tables can only be recreated on the target once the
    operator recreates the mount there — the tool never touches mount
    credentials. Discovery surfaces each required mount as a prerequisite.
    """
    loc = (storage_location or "").lower()
    prefix = "dbfs:/mnt/"
    if not loc.startswith(prefix):
        return None
    rest = loc[len(prefix):]
    name = rest.split("/", 1)[0].strip()
    return name or None
```
In `_discover_hive`, accumulate mount → tables while iterating (inside the `for tbl in ...` loop, after the `rows.append(...)` at lines 478-493) and emit markers after the database loop (before `return rows` at line 512):
```python
    # (declare near the top of _discover_hive, after `rows: list[dict] = []`)
    mount_tables: dict[str, list[str]] = {}
    ...
            # inside the table loop, after building the row:
            _mnt = mount_name_from_location(tbl["storage_location"])
            if _mnt:
                mount_tables.setdefault(_mnt, []).append(tbl["fqn"])
    ...
    # after the database loop, before `return rows`:
    for mount, tables in sorted(mount_tables.items()):
        rows.append(
            discovery_row(
                source_type="hive",
                object_type="mount_prerequisite",
                object_name=f"mnt:{mount}",
                catalog_name="hive_metastore",
                schema_name=None,
                discovered_at=now,
                data_category="mount_prerequisite",
                storage_location=f"dbfs:/mnt/{mount}",
                metadata={"mount": mount, "tables": sorted(tables)},
            )
        )
    return rows
```
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_discovery_hive_markers.py -v`  Expected: PASS. Also `uv run ruff check src/discovery/discovery.py`.)
- [ ] Step 5: Commit
```bash
git add src/discovery/discovery.py tests/unit/test_discovery_hive_markers.py
git commit -m "discovery: emit mount_prerequisite markers for /mnt-backed Hive tables

One marker per required mount (name + affected tables) for the pre_check
guard and the dashboard.

Co-authored-by: Isaac"
```

---

### Task 12: pre_check — target-DBFS-root-enabled, required-mounts-exist, staging-reachable guards
**Files:** Modify `src/pre_check/pre_check.py` (check 12 lines 296-361; add new checks before "Persist results" line 542); Test `tests/unit/test_pre_check_hive_guards.py` (new)
**Interfaces:**
- New pure helpers (module-level, unit-testable): `missing_required_mounts(target_mounts: list[str], required_mounts: list[str]) -> list[str]`; `staging_reachable_status(staging_path: str, dbfs_root_in_scope: bool) -> str` (returns `"PASS"|"WARN"|"skip"` decision hints — actual `dbutils.fs.ls` probe stays in `run()`).
- `run()` gains three `_add(...)` checks; check 12 is updated to reference `hive_dbfs_staging_path`.

- [ ] Step 1: Write the failing test — create `tests/unit/test_pre_check_hive_guards.py`:
```python
"""Unit tests for the like-for-like Hive pre_check guard helpers."""

from __future__ import annotations

from pre_check.pre_check import missing_required_mounts


class TestMissingRequiredMounts:
    def test_flags_absent_mounts(self):
        target = ["/mnt/present", "/mnt/other"]
        required = ["present", "salesraw"]
        assert missing_required_mounts(target, required) == ["salesraw"]

    def test_all_present(self):
        assert missing_required_mounts(["/mnt/a", "/mnt/b"], ["a", "b"]) == []

    def test_handles_bare_and_prefixed_target_forms(self):
        # Target mounts may come back as "/mnt/x" or "x" depending on SDK shape.
        assert missing_required_mounts(["x"], ["x"]) == []
        assert missing_required_mounts(["/mnt/x/"], ["x"]) == []

    def test_empty_required_is_no_missing(self):
        assert missing_required_mounts([], []) == []


class TestPreCheckReferencesStagingKey:
    def test_source_uses_staging_path_not_target_path(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "pre_check" / "pre_check.py").read_text()
        assert "hive_dbfs_staging_path" in src
        assert "hive_dbfs_target_path" not in src
        assert "check_target_mounts" in src
        assert "check_staging_path_reachable" in src
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_pre_check_hive_guards.py -v`  Expected: FAIL — `missing_required_mounts` doesn't exist; source still references `hive_dbfs_target_path`; new checks absent.)
- [ ] Step 3: Write minimal implementation — in `src/pre_check/pre_check.py`:

Add the pure helper near the top (after `_is_notebook`, ~line 49):
```python
def missing_required_mounts(target_mounts: list[str], required_mounts: list[str]) -> list[str]:
    """Return required mount names absent from the target's mounts.

    Normalizes target entries to their bare ``<name>`` (accepts ``/mnt/x``,
    ``/mnt/x/`` or ``x``). /mnt-backed Hive tables can't be recreated on the
    target until the operator recreates the mount there.
    """
    norm = set()
    for m in target_mounts:
        s = (m or "").strip().strip("/")
        if s.startswith("mnt/"):
            s = s[len("mnt/"):]
        s = s.split("/", 1)[0]
        if s:
            norm.add(s)
    return [r for r in required_mounts if r not in norm]
```
Update check 12 (lines 296-361) to use `config.hive_dbfs_staging_path` in place of `config.hive_dbfs_target_path` everywhere (field access, messages, and the write-probe path), and reword to "staging path" (the DBFS-root two-hop staging area).
Add three new checks right before `# Persist results` (line 542):
```python
    # 14b. check_target_dbfs_root — the DBFS-root two-hop lands MANAGED tables in
    # the TARGET DBFS root, so the target must have DBFS-root writes enabled when
    # any DBFS-root table is in scope.
    try:
        dbfs_in_scope = config.migrate_hive_dbfs_root
        if not dbfs_in_scope:
            _add("check_target_dbfs_root", "PASS", "No DBFS-root migration requested (migrate_hive_dbfs_root=false).")
        else:
            probe = f"dbfs:/tmp/.wsm_dbfs_root_probe_{__import__('time').strftime('%H%M%S')}"
            try:
                dbutils.fs.put(probe, "x", True)  # type: ignore[attr-defined]  # noqa: F821
                dbutils.fs.rm(probe, True)  # type: ignore[attr-defined]  # noqa: F821
                _add("check_target_dbfs_root", "PASS", "Target DBFS root is writable.")
            except Exception as pe:  # noqa: BLE001
                _add(
                    "check_target_dbfs_root", "FAIL",
                    f"Target DBFS root not writable: {pe}",
                    "Enable DBFS-root writes on the target workspace (required for the "
                    "DBFS-root two-hop copy) or set migrate_hive_dbfs_root=false.",
                )
    except Exception as e:  # noqa: BLE001
        _add("check_target_dbfs_root", "WARN", f"Could not probe target DBFS root: {e}")

    # 14c. check_target_mounts — every /mnt mount required by a /mnt-backed Hive
    # table (mount_prerequisite markers from discovery) must already exist on the
    # target; the tool never recreates mounts or touches mount credentials.
    try:
        marker_rows = spark.sql(
            f"""
            SELECT metadata_json FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory
            WHERE object_type = 'mount_prerequisite'
            """
        ).collect()
        import json as _json
        required = sorted({_json.loads(r.metadata_json).get("mount") for r in marker_rows if r.metadata_json})
        required = [m for m in required if m]
        if not required:
            _add("check_target_mounts", "PASS", "No /mnt-backed Hive tables require a target mount.")
        else:
            target_mounts = [m.mountPoint for m in dbutils.fs.mounts()]  # type: ignore[attr-defined]  # noqa: F821
            missing = missing_required_mounts(target_mounts, required)
            if missing:
                _add(
                    "check_target_mounts", "FAIL",
                    f"Required target mount(s) missing: {missing}",
                    "Recreate the listed /mnt mount(s) on the target workspace before "
                    "migrating the /mnt-backed Hive tables (see the dashboard's mount "
                    "prerequisites panel).",
                )
            else:
                _add("check_target_mounts", "PASS", f"All {len(required)} required target mount(s) present.")
    except Exception as e:  # noqa: BLE001
        _add(
            "check_target_mounts", "WARN",
            f"Could not verify target mounts: {e}",
            "Run discovery first so mount_prerequisite markers exist, then re-run pre_check.",
        )

    # 14d. check_staging_path_reachable — best-effort ls of the shared abfss
    # staging area used by the DBFS-root two-hop copy.
    try:
        if not config.migrate_hive_dbfs_root:
            _add("check_staging_path_reachable", "PASS", "DBFS-root migration disabled; staging path not needed.")
        elif not config.hive_dbfs_staging_path:
            _add(
                "check_staging_path_reachable", "FAIL",
                "hive_dbfs_staging_path is empty but DBFS-root migration is enabled.",
                "Set hive_dbfs_staging_path to a shared abfss:// location both workspaces can reach.",
            )
        else:
            try:
                dbutils.fs.ls(config.hive_dbfs_staging_path)  # type: ignore[attr-defined]  # noqa: F821
                _add("check_staging_path_reachable", "PASS", f"Staging path reachable: {config.hive_dbfs_staging_path}")
            except Exception as se:  # noqa: BLE001
                _add(
                    "check_staging_path_reachable", "WARN",
                    f"Could not list staging path {config.hive_dbfs_staging_path}: {se}",
                    "Verify the SPN can read/write the shared staging container from both workspaces.",
                )
    except Exception as e:  # noqa: BLE001
        _add("check_staging_path_reachable", "WARN", f"Staging path check skipped: {e}")
```
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_pre_check_hive_guards.py -v`  Expected: PASS. Also `uv run ruff check src/pre_check/pre_check.py`.)
- [ ] Step 5: Commit
```bash
git add src/pre_check/pre_check.py tests/unit/test_pre_check_hive_guards.py
git commit -m "pre_check: target DBFS-root, required /mnt mounts, staging-reachable guards

Also rename check 12's config reference to hive_dbfs_staging_path.

Co-authored-by: Isaac"
```

---

### Task 13: Dashboard — mount prerequisites + DBFS-root copies + skipped/failed panel
**Files:** Modify `dashboards/migration_dashboard.lvdash.json` (add 3 datasets + a page/widgets); Test `tests/unit/test_dashboard_smoke.py` (add presence assertions)
**Interfaces:** New datasets `mount_prerequisites`, `dbfs_root_copies`, `hive_skipped_failed`, each a query over the tracking tables; new widgets on the `detail` page referencing them. All referenced columns must exist in the tracking schema (enforced by the existing smoke test).

- [ ] Step 1: Write the failing test — append to `tests/unit/test_dashboard_smoke.py`:
```python
class TestHiveLikeForLikePanels:
    """The like-for-like Hive migration adds dashboard visibility for mount
    prerequisites, DBFS-root copies, and skipped/failed Hive objects."""

    def test_new_datasets_present(self):
        d = _load_dashboard()
        names = {ds["name"] for ds in d["datasets"]}
        assert {"mount_prerequisites", "dbfs_root_copies", "hive_skipped_failed"} <= names

    def test_new_datasets_have_queries_and_widgets(self):
        d = _load_dashboard()
        for ds in d["datasets"]:
            if ds["name"] in ("mount_prerequisites", "dbfs_root_copies", "hive_skipped_failed"):
                assert ds.get("queryLines"), f"{ds['name']} has no query"
        widget_datasets = set()
        for page in d["pages"]:
            for le in page.get("layout", []):
                for q in le.get("widget", {}).get("queries", []) or []:
                    ref = q.get("query", {}).get("datasetName")
                    if ref:
                        widget_datasets.add(ref)
        assert {"mount_prerequisites", "dbfs_root_copies", "hive_skipped_failed"} <= widget_datasets
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_dashboard_smoke.py::TestHiveLikeForLikePanels -v`  Expected: FAIL — datasets/widgets absent.)
- [ ] Step 3: Write minimal implementation — in `dashboards/migration_dashboard.lvdash.json`:

Add three datasets to the `datasets` array (mirror the `policy_protected_tables` dataset shape — `name`, `displayName`, `queryLines`). Use only columns present in the tracking schema (`discovery_inventory`: `object_name`, `object_type`, `metadata_json`, `storage_location`, `schema_name`; `migration_status`: `object_name`, `object_type`, `status`, `error_message`, `source_row_count`, `target_row_count`, `migrated_at`):
```json
{
  "name": "mount_prerequisites",
  "displayName": "Mount Prerequisites (/mnt)",
  "queryLines": [
    "SELECT object_name, storage_location, metadata_json\n",
    "FROM migration_tracking.cp_migration.discovery_inventory\n",
    "WHERE object_type = 'mount_prerequisite'"
  ]
},
{
  "name": "dbfs_root_copies",
  "displayName": "DBFS-root Tables Copied",
  "queryLines": [
    "SELECT object_name, status, source_row_count, target_row_count, migrated_at\n",
    "FROM (\n",
    "  SELECT *, ROW_NUMBER() OVER (PARTITION BY object_name, object_type ORDER BY migrated_at DESC) AS rn\n",
    "  FROM migration_tracking.cp_migration.migration_status\n",
    "  WHERE object_type = 'hive_managed_dbfs_root'\n",
    ") WHERE rn = 1"
  ]
},
{
  "name": "hive_skipped_failed",
  "displayName": "Hive Skipped / Failed",
  "queryLines": [
    "SELECT object_name, object_type, status, error_message, migrated_at\n",
    "FROM (\n",
    "  SELECT *, ROW_NUMBER() OVER (PARTITION BY object_name, object_type ORDER BY migrated_at DESC) AS rn\n",
    "  FROM migration_tracking.cp_migration.migration_status\n",
    "  WHERE object_type LIKE 'hive_%'\n",
    ") WHERE rn = 1 AND status IN ('failed', 'validation_failed', 'skipped', 'skipped_by_config')"
  ]
}
```
Add three `table` widgets on the `detail` page's `layout` array (copy the `policy_protected_widget` widget structure — `widget.name`, `queries[0].query.datasetName` + `fields`, `spec.widgetType="table"`, `frame.title`), positioning them below the existing widgets (e.g. `y: 22`, `y: 30`, `y: 38`; `width: 6`, `height: 8`). Each widget's `fields` list must name the columns its dataset SELECTs (e.g. `object_name`, `status`, …) so the field-expression smoke test resolves them.
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_dashboard_smoke.py -v`  Expected: PASS — includes the existing column-resolution + widget-ref checks.)
- [ ] Step 5: Commit
```bash
git add dashboards/migration_dashboard.lvdash.json tests/unit/test_dashboard_smoke.py
git commit -m "dashboard: mount prerequisites, DBFS-root copies, hive skipped/failed panels

Co-authored-by: Isaac"
```

---

## Phase P5 — Integration re-run leg + user_guide

### Task 14: Integration — migrate_hive re-run leg + coverage manifest move
**Files:** Modify `tests/integration/coverage_manifest.py` (lines 87-96); Modify `tests/integration/test_hive_end_to_end.py` (add a re-run COMMAND block); Test `tests/unit/test_int_coverage_guard.py` (existing — enforces the manifest)
**Interfaces:** `assert_migrate_idempotent(workspace_client, job_id, error_messages, *, label=None) -> bool` (from `_assertion_helpers.py`); the migrate_hive job is re-run once and asserted clean.

- [ ] Step 1: Write the failing test — edit `tests/integration/coverage_manifest.py`: move `migrate_hive` from `RERUN_EXEMPT` (delete line 94) into `RERUN_COVERED_JOBS` (line 87):
```python
RERUN_COVERED_JOBS: frozenset[str] = frozenset({"migrate_hive"})
RERUN_JOBS_IN_SCOPE: frozenset[str] = frozenset(
    {"discovery", "migrate_uc", "migrate_hive", "migrate_governance"}
)
RERUN_EXEMPT: dict[str, str] = {
    "discovery": "pending #8 re-run leg (dedup MERGE fixed in code; live re-run assert pending)",
    "migrate_uc": "pending #20 re-run leg (setup_sharing idempotency)",
    "migrate_governance": "pending re-run leg",
}
```
The guard `tests/unit/test_int_coverage_guard.py` enforces every `RERUN_JOBS_IN_SCOPE` is covered-or-exempt AND that a covered job is not also exempt — this edit keeps it green while asserting the manifest change. (If the guard also greps the int-test source for a re-run marker, the Step-3 block satisfies it.)
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_int_coverage_guard.py -v`  Expected: FAIL if the guard cross-checks that `RERUN_COVERED_JOBS` members are not simultaneously in `RERUN_EXEMPT` and appear in the int suite — confirm the exact failing assertion, then satisfy it in Step 3. If the guard passes on the manifest edit alone, this step documents that the re-run leg is still required by the design and proceeds.)
- [ ] Step 3: Write minimal implementation — append a re-run COMMAND cell to `tests/integration/test_hive_end_to_end.py` (before the final `if error_messages:` block at line 424), gated so it only runs when the harness supplies the job id:
```python
# COMMAND ----------
# --- migrate_hive idempotency re-run leg (finding #12) ---
# Re-run the migrate_hive job once and assert a clean terminal state with no
# LOCATION_OVERLAP / ALREADY_EXISTS signature — proves the object_name anti-join
# + grant-before-transfer + skip-if-owned make re-runs safe (RERUN_COVERED_JOBS).
from tests.integration._assertion_helpers import assert_migrate_idempotent  # type: ignore[import-not-found]

_migrate_hive_job_id = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="migrate_hive_job_id", debugValue=""
)
if _migrate_hive_job_id:
    from databricks.sdk import WorkspaceClient

    assert_migrate_idempotent(
        WorkspaceClient(),
        int(_migrate_hive_job_id),
        error_messages,
        label="migrate_hive re-run",
    )
    print("migrate_hive re-run leg executed.")
else:
    print("migrate_hive re-run leg skipped: no migrate_hive_job_id task value supplied.")
```
(The harness/workflow must publish `migrate_hive_job_id` as a `seed_hive` task value — note this in the workflow YAML `resources/integration_tests/hive_integration_test_workflow.yml` as a follow-up wiring step; the assertion is a no-op skip until then, so the suite stays green in environments that don't supply it.)
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_int_coverage_guard.py -v`  Expected: PASS. The integration notebook itself runs only in the Databricks int-test job, not pytest.)
- [ ] Step 5: Commit
```bash
git add tests/integration/coverage_manifest.py tests/integration/test_hive_end_to_end.py
git commit -m "int: migrate_hive re-run leg; move migrate_hive to RERUN_COVERED_JOBS

Asserts idempotency via assert_migrate_idempotent (finding #12 live leg).

Co-authored-by: Isaac"
```

---

### Task 15: user_guide — SPN permissions on hive_metastore + what-changed note
**Files:** Modify `docs/user_guide.md` (§3.1 lines 94-100; §5 Step 5 lines 402-406; add a new subsection); Test `tests/unit/test_user_guide_hive.py` (new — a lightweight presence guard)
**Interfaces:** Documentation only. The presence test locks in that the like-for-like content ships (mirrors the repo's other doc-guard tests).

- [ ] Step 1: Write the failing test — create `tests/unit/test_user_guide_hive.py`:
```python
"""Guard that the user_guide documents the like-for-like Hive SPN permissions."""

from __future__ import annotations

import pathlib

_GUIDE = (pathlib.Path(__file__).resolve().parents[2] / "docs" / "user_guide.md").read_text()


class TestUserGuideHiveLikeForLike:
    def test_has_spn_permissions_section(self):
        assert "SPN permissions on hive_metastore" in _GUIDE

    def test_documents_key_requirements(self):
        for token in (
            "hive_dbfs_staging_path",
            "shared staging",
            "DBFS root enabled",
            "/mnt",
            "like-for-like",
        ):
            assert token in _GUIDE, f"user_guide missing {token!r}"

    def test_drops_uc_upgrade_wording(self):
        # The retired UC-upgrade Hive config keys must not linger in the guide.
        assert "hive_target_catalog" not in _GUIDE
        assert "hive_dbfs_target_path" not in _GUIDE
```
- [ ] Step 2: Run test to verify it fails  (Run: `uv run pytest tests/unit/test_user_guide_hive.py -v`  Expected: FAIL — the guide still says `hive_target_catalog` / `hive_dbfs_target_path`; no SPN-permissions section.)
- [ ] Step 3: Write minimal implementation — in `docs/user_guide.md`:

Rewrite §3.1 (lines 94-100) so "This tool" describes the two-hop staging copy into the target's own DBFS root (managed, like-for-like), gated on `migrate_hive_dbfs_root` + `hive_dbfs_staging_path` (no `hive_target_catalog`).
Update §5 Step 5 (lines 402-406): the Hive path writes like-for-like into the target `hive_metastore` (same db/table names), not a UC catalog.
Update the config-snippet comment (line 345) to `migrate_hive_dbfs_root: false  # true + hive_dbfs_staging_path for DBFS-root Hive (two-hop staging)`.
Add a new subsection (place it under the Hive object-class section, after §3.x) copied from the spec's "SPN permissions on hive_metastore" section:
```markdown
### SPN permissions on `hive_metastore` (like-for-like Hive path)

The Hive path migrates `hive_metastore` content **like-for-like** into the
target workspace's own `hive_metastore` (same database/table names, same
storage). The SPN needs:

**Source workspace (read):**
- Legacy Hive `SELECT` + `READ_METADATA` on the migrated `hive_metastore`
  databases/tables (for `SHOW CREATE TABLE`, `SHOW GRANTS`, and reading rows for
  the DBFS-root staging copy).
- Source DBFS-root read access (runs on a classic cluster; workspace-level).
- ADLS storage account key (secret) for ADLS-backed HMS external/non-DBFS tables
  (legacy `fs.azure.account.key`; UC vending doesn't cover HMS `LOCATION`s).
- **Write** access to the shared staging container (`hive_dbfs_staging_path`).

**Target workspace (write):**
- Legacy Hive `CREATE` on `hive_metastore` (create databases) and on each target
  database (create tables).
- Target **DBFS root enabled** + write access.
- **Read** on the shared staging container.
- Storage access to the same cloud paths for external tables (so replayed
  external tables resolve).
- Required `/mnt` mounts pre-existing (recreate them first — the tool never
  touches mount credentials; pre_check verifies each required mount exists).

**What changed vs the UC-upgrade path — no longer needs:**
- `CREATE CATALOG` on the metastore.
- UC `CREATE SCHEMA` / `CREATE TABLE` / `USE CATALOG` in a UC catalog.
- Delta Sharing privileges (`CREATE SHARE`, `CREATE RECIPIENT`) for the Hive path.
- UC external-location grants (`CREATE EXTERNAL TABLE` / `READ FILES`) for the
  Hive path.

**Now needs:** legacy Hive `CREATE` on target `hive_metastore` + databases,
target DBFS root enabled + write access, and shared-staging container access.
```
- [ ] Step 4: Run to verify pass  (Run: `uv run pytest tests/unit/test_user_guide_hive.py -v`  Expected: PASS.)
- [ ] Step 5: Commit
```bash
git add docs/user_guide.md tests/unit/test_user_guide_hive.py
git commit -m "user_guide: SPN permissions on hive_metastore (like-for-like) + what-changed note

Co-authored-by: Isaac"
```

---

## Self-review notes

Spec requirements and where each is covered:

- "Only mode is like-for-like / remove UC-upgrade code" — Tasks 1-8, 10 (config, hive_common, orchestrator, all DDL workers, grants).
- "DBFS-root two-hop shared staging" — Task 9.
- "/mnt reported-as-prereq + existence check" — Task 11 (markers) + Task 12 (`check_target_mounts`).
- "Grants: grant-before-transfer + skip-if-already-owned; drop catalog ownership (#14 moot)" — Task 10.
- "pre_check guards (DBFS-root enabled, mounts exist, staging reachable)" — Task 12.
- "Discovery storage-type classification" — the four storage categories (external-cloud / mnt / dbfs-root / managed-nondbfs) are already produced by `catalog_utils.categorize_hive_table`; Task 11 only ADDS the `mount_prerequisite` marker layered on top. **No task re-implements the base classification** because it already exists and the spec says it is "already largely present via data_category". Flagging in case the reviewer expected an explicit new classifier.
- "Dashboard panel" — Task 13.
- "Integration re-run leg + coverage manifest move; user_guide update" — Tasks 14, 15.
- "Keep the #12 anti-join" — Task 4 explicitly preserves it (guarded by existing + new source tests).
- "Config alias with warning" — Task 1 (`DeprecationWarning`).

**Items I could not fully map to a self-contained TDD task (need reviewer attention):**

1. **Live wiring of the migrate_hive re-run leg (Task 14).** The re-run assertion depends on the int-test workflow publishing a `migrate_hive_job_id` task value from `seed_hive`. That YAML wiring in `resources/integration_tests/hive_integration_test_workflow.yml` is an environment/DAB change that can't be exercised by `uv run pytest`; Task 14 leaves it as a documented no-op skip until wired. The `test_int_coverage_guard.py` behavior on the manifest edit alone must be confirmed at Step 2 (the plan instructs verifying the exact assertion) — if that guard greps the int source for a marker string, adjust the Step-3 block's comment to match.

2. **RESOLVED — now Task 1b.** The consumers of the removed `hive_target_catalog` / renamed `hive_dbfs_target_path` in test-support code (`_config_override.py`, `setup_test_config.py`, `seed_hive_test_data.py`, `teardown_hive.py`, and their unit guards `test_setup_test_config.py`, `test_teardown_notebooks.py`, `test_idempotency_audit.py`) are handled by the new **Task 1b**, which runs right after Task 1 to return the tree to green — including the `teardown_hive.py` behavior change (drop the migrated target `hive_metastore` database instead of a UC catalog).

3. **`docs/reference/idempotency_audit.md`** (lines 315, 509) documents the old rewrite-to-`hive_target_catalog` behavior. It is reference documentation, not covered by a task; recommend updating it for consistency.
