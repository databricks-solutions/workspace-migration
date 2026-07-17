# Hive Dependency Skip Cascade (#9) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a Hive object is not migrated (config-skipped DBFS-root, or failed), its dependent views and its per-object grants record `skipped_dependency_not_migrated` instead of hard-failing with raw `TABLE_OR_VIEW_NOT_FOUND` / permission errors.

**Architecture:** Workers read the "not-validated" object set from `migration_status` (the Hive workflow runs tables → views → grants, so outcomes are already persisted when views/grants run). Views scan their DDL textually for any not-validated object name (the same substring technique the existing topological sort uses); grants check whether their own target object is validated. A new terminal status `skipped_dependency_not_migrated` keeps these out of the `failed` bucket.

**Tech Stack:** Python, Databricks notebooks (DAB), PySpark, pytest, ruff.

## Global Constraints

- New terminal status string, used verbatim everywhere: `skipped_dependency_not_migrated`.
- Unit tests run with **no Spark** — mock `spark`/`execute_and_poll`; assert SQL strings and status dicts, never execute Spark SQL.
- Hive object names are namespace-unique (`hive_metastore`.<db>.<table>); the not-validated set is scoped to `source_type='hive'` for tidiness.
- Every code step shows the full code. Run `uv run pytest` and `uv run ruff check <files>` per task.
- Deploy/run context (unchanged): serverless env v5, `databricks-sdk==0.120.0`.

---

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `src/common/tracking.py` | Modify | Add `_TERMINAL_STATUSES` entry + `not_validated_object_names()` method. |
| `src/migrate/hive_views_worker.py` | Modify | Add pure `view_dependency_skip()`; gate view migration on it in `run()`. |
| `src/migrate/hive_grants_worker.py` | Modify | Skip per-object grants whose target is not validated. |
| `tests/unit/test_tracking.py` | Modify (Test) | `not_validated_object_names` SQL contract + terminal-status membership. |
| `tests/unit/test_hive_workers.py` | Modify (Test) | `view_dependency_skip` pure tests + views/grants worker cascade tests. |

---

## Task 1: `not_validated_object_names` + terminal status registration

**Files:**
- Modify: `src/common/tracking.py` (`_TERMINAL_STATUSES` at lines 97-137; add method after `get_latest_migration_status` ~line 371)
- Test: `tests/unit/test_tracking.py`

**Interfaces:**
- Produces: `TrackingManager.not_validated_object_names(source_type: str | None = None) -> set[str]` — object names whose LATEST `migration_status` is not `'validated'`. When `source_type` is given, joins `discovery_inventory` to scope to that source.
- Produces: `"skipped_dependency_not_migrated"` added to `_TERMINAL_STATUSES`.

- [ ] **Step 1: Write the failing tests** — append to `tests/unit/test_tracking.py`:

```python
class TestNotValidatedObjectNames:
    def test_sql_uses_latest_window_and_excludes_validated(self):
        from common.tracking import TrackingManager

        spark = MagicMock()
        config = MagicMock()
        config.tracking_catalog = "migration_tracking"
        config.tracking_schema = "cp_migration"
        # Two rows come back from the (mocked) query.
        row_a = MagicMock(); row_a.object_name = "`hive_metastore`.`db`.`t1`"
        row_b = MagicMock(); row_b.object_name = "`hive_metastore`.`db`.`t2`"
        spark.sql.return_value.collect.return_value = [row_a, row_b]

        tracker = TrackingManager(spark, config)
        result = tracker.not_validated_object_names(source_type="hive")

        sql = spark.sql.call_args[0][0].upper()
        assert "ROW_NUMBER()" in sql
        assert "PARTITION BY OBJECT_NAME, OBJECT_TYPE" in sql
        assert "ORDER BY MIGRATED_AT DESC" in sql
        assert "!= 'VALIDATED'" in sql or "<> 'VALIDATED'" in sql
        assert "SOURCE_TYPE = 'HIVE'" in sql
        assert result == {"`hive_metastore`.`db`.`t1`", "`hive_metastore`.`db`.`t2`"}

    def test_no_source_type_filter_when_omitted(self):
        from common.tracking import TrackingManager

        spark = MagicMock()
        config = MagicMock()
        config.tracking_catalog = "migration_tracking"
        config.tracking_schema = "cp_migration"
        spark.sql.return_value.collect.return_value = []

        TrackingManager(spark, config).not_validated_object_names()
        sql = spark.sql.call_args[0][0].upper()
        assert "SOURCE_TYPE" not in sql


class TestDependencySkipStatusIsTerminal:
    def test_skipped_dependency_not_migrated_is_terminal(self):
        from common.tracking import _TERMINAL_STATUSES

        assert "skipped_dependency_not_migrated" in _TERMINAL_STATUSES
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_tracking.py::TestNotValidatedObjectNames tests/unit/test_tracking.py::TestDependencySkipStatusIsTerminal -v`
Expected: FAIL — `not_validated_object_names` doesn't exist; status not in `_TERMINAL_STATUSES`.

- [ ] **Step 3a: Register the terminal status** — in `src/common/tracking.py`, add to the `_TERMINAL_STATUSES` tuple (after `"skipped_policy_protected",` at line 107):

```python
    # A dependent object (a view on a skipped/failed table, or a grant whose
    # target object was not migrated) — recorded skipped, not failed, so the
    # skip cascade doesn't read as a tool failure (finding #9). Terminal so
    # re-runs don't reprocess it while its dependency stays unmigrated.
    "skipped_dependency_not_migrated",
```

- [ ] **Step 3b: Add the method** — in `src/common/tracking.py`, after `get_latest_migration_status` (before `get_pending_objects`, ~line 372):

```python
    def not_validated_object_names(self, source_type: str | None = None) -> set[str]:
        """Object names whose LATEST migration_status is NOT 'validated'.

        Used by the Hive views/grants workers to cascade-skip dependents of
        objects that were not migrated (finding #9): a config-skipped DBFS-root
        table, or a genuine failure. Reuses the latest-row window so a
        superseded in_progress row never counts. When ``source_type`` is given,
        joins discovery_inventory to scope to that source (e.g. 'hive').
        """
        where_src = ""
        args: dict = {}
        if source_type:
            where_src = "WHERE d.source_type = :src"
            args["src"] = source_type
        rows = self.spark.sql(
            f"""
            WITH latest_status AS (
                SELECT object_name, status
                FROM (
                    SELECT object_name, status,
                           ROW_NUMBER() OVER (
                               PARTITION BY object_name, object_type
                               ORDER BY migrated_at DESC
                           ) AS rn
                    FROM {self._fqn}.migration_status
                )
                WHERE rn = 1 AND status != 'validated'
            )
            SELECT DISTINCT s.object_name
            FROM latest_status s
            JOIN {self._fqn}.discovery_inventory d
              ON s.object_name = d.object_name
            {where_src}
            """,
            args=args,
        ).collect()
        return {r.object_name for r in rows}
```

Note: when `source_type` is None, the `JOIN discovery_inventory` with no WHERE still returns only names present in discovery — acceptable (every migrated object was discovered). The `test_no_source_type_filter_when_omitted` test only asserts no `SOURCE_TYPE` clause is emitted.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_tracking.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/common/tracking.py tests/unit/test_tracking.py
git commit -m "tracking: not_validated_object_names + skipped_dependency_not_migrated terminal status (#9)

Co-authored-by: Isaac"
```

---

## Task 2: Views worker cascade-skip

**Files:**
- Modify: `src/migrate/hive_views_worker.py` (add pure helper near `_sort_views_by_deps` ~line 55; gate in `run()` ~line 209 after topological sort, before the migrate loop)
- Test: `tests/unit/test_hive_workers.py` (`TestHiveViewsWorker` / new `TestHiveViewDependencySkip`)

**Interfaces:**
- Consumes: `TrackingManager.not_validated_object_names(source_type="hive")` (Task 1).
- Produces: `view_dependency_skip(ddl: str, not_migrated_names: set[str]) -> str | None` — returns the FQN of a not-migrated object the view DDL references (→ skip), else None (→ migrate).

- [ ] **Step 1: Write the failing tests** — append to `tests/unit/test_hive_workers.py`:

```python
class TestHiveViewDependencySkip:
    def test_flags_backticked_reference(self):
        from migrate.hive_views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `hive_metastore`.`db`.`v` AS SELECT * FROM `hive_metastore`.`db`.`dbfs_orders`"
        not_migrated = {"`hive_metastore`.`db`.`dbfs_orders`"}
        assert view_dependency_skip(ddl, not_migrated) == "`hive_metastore`.`db`.`dbfs_orders`"

    def test_flags_dotted_reference(self):
        from migrate.hive_views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW hive_metastore.db.v AS SELECT * FROM hive_metastore.db.dbfs_orders"
        not_migrated = {"`hive_metastore`.`db`.`dbfs_orders`"}
        assert view_dependency_skip(ddl, not_migrated) == "`hive_metastore`.`db`.`dbfs_orders`"

    def test_none_when_all_deps_validated(self):
        from migrate.hive_views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `hive_metastore`.`db`.`v` AS SELECT * FROM `hive_metastore`.`db`.`good`"
        assert view_dependency_skip(ddl, {"`hive_metastore`.`db`.`dbfs_orders`"}) is None

    def test_empty_not_migrated_never_skips(self):
        from migrate.hive_views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `hive_metastore`.`db`.`v` AS SELECT * FROM `hive_metastore`.`db`.`x`"
        assert view_dependency_skip(ddl, set()) is None

    def test_transitive_view_on_skipped_view(self):
        """A view on a view that was itself skipped is caught once the skipped
        view's FQN is added to the not-migrated set (same-run transitivity)."""
        from migrate.hive_views_worker import view_dependency_skip

        # v2 selects from v1; v1 was skipped this run and added to the set.
        ddl_v2 = "CREATE OR REPLACE VIEW `hive_metastore`.`db`.`v2` AS SELECT * FROM `hive_metastore`.`db`.`v1`"
        not_migrated = {"`hive_metastore`.`db`.`v1`"}
        assert view_dependency_skip(ddl_v2, not_migrated) == "`hive_metastore`.`db`.`v1`"


class TestHiveViewCascadeInMigrate:
    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_view_on_not_migrated_table_is_skipped_not_executed(self, mock_exec, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 100.1]
        ddl = "CREATE VIEW `hive_metastore`.`db`.`v_orders` AS SELECT * FROM `hive_metastore`.`db`.`dbfs_orders`"
        cfg = _config_mock()
        res = migrate_hive_view(
            {"object_name": "`hive_metastore`.`db`.`v_orders`"},
            ddl,
            config=cfg,
            auth=MagicMock(),
            wh_id="wh",
            not_migrated_names={"`hive_metastore`.`db`.`dbfs_orders`"},
        )
        assert res["status"] == "skipped_dependency_not_migrated"
        assert "dbfs_orders" in res["error_message"]
        mock_exec.assert_not_called()

    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_view_with_validated_deps_migrates(self, mock_exec, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 100.5]
        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        ddl = "CREATE VIEW `hive_metastore`.`db`.`v` AS SELECT * FROM `hive_metastore`.`db`.`good`"
        cfg = _config_mock()
        res = migrate_hive_view(
            {"object_name": "`hive_metastore`.`db`.`v`"},
            ddl,
            config=cfg,
            auth=MagicMock(),
            wh_id="wh",
            not_migrated_names=set(),
        )
        assert res["status"] == "validated"
        mock_exec.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_hive_workers.py::TestHiveViewDependencySkip tests/unit/test_hive_workers.py::TestHiveViewCascadeInMigrate -v`
Expected: FAIL — `view_dependency_skip` doesn't exist; `migrate_hive_view` has no `not_migrated_names` param.

- [ ] **Step 3a: Add the pure helper** — in `src/migrate/hive_views_worker.py`, before `_sort_views_by_deps` (~line 54):

```python
def view_dependency_skip(ddl: str, not_migrated_names: set[str]) -> str | None:
    """Return the FQN of a not-migrated object the view DDL references, else None.

    Uses the same textual match as ``_sort_views_by_deps``: an object is
    referenced if its backticked (`hive_metastore`.`db`.`t`) OR dotted
    (hive_metastore.db.t) form appears in the DDL. A view referencing any
    not-migrated object is cascade-skipped (finding #9).
    """
    for fqn in not_migrated_names:
        unquoted = fqn.strip("`").replace("`.`", ".")
        if fqn in ddl or unquoted in ddl:
            return fqn
    return None
```

- [ ] **Step 3b: Gate `migrate_hive_view`** — change its signature to accept the set and short-circuit. Replace the signature + body preamble (lines 98-111):

```python
def migrate_hive_view(
    view_info: dict,
    ddl: str,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    wh_id: str,
    not_migrated_names: set[str] | None = None,
) -> dict:
    """Replay a single Hive view DDL into `hive_metastore` unchanged.

    If the view references an object that was not migrated (finding #9),
    record ``skipped_dependency_not_migrated`` and do not execute the DDL.
    """
    obj_name = view_info["object_name"]
    start = time.time()

    dep = view_dependency_skip(ddl, not_migrated_names or set())
    if dep is not None:
        return {
            "object_name": obj_name,
            "object_type": "hive_view",
            "status": "skipped_dependency_not_migrated",
            "error_message": f"depends on not-migrated object {dep}",
            "duration_seconds": time.time() - start,
        }

    rewritten = ddl  # like-for-like: replay view DDL into hive_metastore as-is
    rewritten = rewrite_ddl(rewritten, r"CREATE\s+VIEW\b", "CREATE OR REPLACE VIEW")
```

(Leave the rest of `migrate_hive_view` unchanged.)

- [ ] **Step 3c: Wire it in `run()`** — in `src/migrate/hive_views_worker.py` `run()`, load the set once and pass it to each `migrate_hive_view` call. After `wh_id = find_warehouse(auth)` (~line 166) add:

```python
    not_migrated_names = tracker.not_validated_object_names(source_type="hive")
```

Then find the `migrate_hive_view(...)` call inside the migrate loop (after the topological sort) and add the kwarg, plus make the cascade fully transitive by feeding a just-skipped view back into the set (views run in topo order, so a downstream view-on-a-skipped-view is then caught):

```python
        result = migrate_hive_view(
            view_lookup[fqn],
            ddls[fqn],
            config=config,
            auth=auth,
            wh_id=wh_id,
            not_migrated_names=not_migrated_names,
        )
        # Transitive cascade: a view skipped now becomes a not-migrated
        # dependency for later views in topo order (finding #9).
        if result["status"] == "skipped_dependency_not_migrated":
            not_migrated_names.add(fqn)
```

(If the existing call uses positional/kwarg forms, keep them and just add `not_migrated_names=not_migrated_names`. Keep the existing `results.append(result)` / status-recording line that follows.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_hive_workers.py -v`
Expected: PASS. Also `uv run ruff check src/migrate/hive_views_worker.py tests/unit/test_hive_workers.py`.

- [ ] **Step 5: Commit**

```bash
git add src/migrate/hive_views_worker.py tests/unit/test_hive_workers.py
git commit -m "hive_views_worker: cascade-skip views that depend on not-migrated objects (#9)

Co-authored-by: Isaac"
```

---

## Task 3: Grants worker cascade-skip

**Files:**
- Modify: `src/migrate/hive_grants_worker.py` (`run()` per-object loop ~lines 387-409; load the set once ~line 320)
- Test: `tests/unit/test_hive_workers.py` (`TestHiveGrantsWorker`)

**Interfaces:**
- Consumes: `TrackingManager.not_validated_object_names(source_type="hive")` (Task 1).
- Produces: a `hive_grant` row with status `skipped_dependency_not_migrated` when a per-object securable's target is not validated; no grant SQL executed for it.

The grants worker's `run()` is notebook-guarded (`if _is_notebook():`-style entry via `run(dbutils, spark)`), so the cascade is best tested through a small extracted pure predicate to keep it unit-testable without Spark.

- [ ] **Step 1: Write the failing tests** — append to `TestHiveGrantsWorker` in `tests/unit/test_hive_workers.py`:

```python
    def test_grant_target_skip_predicate(self):
        from migrate.hive_grants_worker import _grant_target_not_migrated

        not_migrated = {"`hive_metastore`.`db`.`dbfs_orders`"}
        assert _grant_target_not_migrated("`hive_metastore`.`db`.`dbfs_orders`", not_migrated) is True
        assert _grant_target_not_migrated("`hive_metastore`.`db`.`good`", not_migrated) is False

    def test_skipped_grant_record_shape(self):
        from migrate.hive_grants_worker import _skipped_dependency_grant_row

        row = _skipped_dependency_grant_row("`hive_metastore`.`db`.`dbfs_orders`")
        assert row["object_type"] == "hive_grant"
        assert row["status"] == "skipped_dependency_not_migrated"
        assert "dbfs_orders" in row["error_message"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_hive_workers.py::TestHiveGrantsWorker -v`
Expected: FAIL — helpers don't exist.

- [ ] **Step 3a: Add the pure helpers** — in `src/migrate/hive_grants_worker.py`, after `_skip_principal` (~line 69):

```python
def _grant_target_not_migrated(object_name: str, not_migrated_names: set[str]) -> bool:
    """True when a per-object grant's target was not migrated (finding #9)."""
    return object_name in not_migrated_names


def _skipped_dependency_grant_row(object_name: str) -> dict:
    """migration_status row for a grant skipped because its target object was
    not migrated (finding #9) — recorded skipped, not failed."""
    return {
        "object_name": f"GRANT_SKIPPED_{object_name}",
        "object_type": "hive_grant",
        "status": "skipped_dependency_not_migrated",
        "error_message": f"target object {object_name} was not migrated; grants skipped",
        "duration_seconds": 0.0,
    }
```

- [ ] **Step 3b: Wire into `run()`** — in `src/migrate/hive_grants_worker.py` `run()`, load the set once (after `spn_client_id = config.spn_client_id`, ~line 320):

```python
    not_migrated_names = tracker.not_validated_object_names(source_type="hive")
```

Then in the per-object loop, gate before reading/emitting grants. Replace the block at lines 388-393:

```python
        # Object-level grants
        securable = _OBJECT_TYPE_TO_SECURABLE.get(object_type)
        if not securable:
            logger.info("Skipping unknown object_type %s for %s.", object_type, object_name)
            continue

        # Cascade skip (finding #9): don't grant on an object that wasn't migrated.
        if _grant_target_not_migrated(object_name, not_migrated_names):
            logger.info("Skipping grants for not-migrated object %s.", object_name)
            results.append(_skipped_dependency_grant_row(object_name))
            continue
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_hive_workers.py -v`
Expected: PASS. Also `uv run ruff check src/migrate/hive_grants_worker.py tests/unit/test_hive_workers.py`.

- [ ] **Step 5: Commit**

```bash
git add src/migrate/hive_grants_worker.py tests/unit/test_hive_workers.py
git commit -m "hive_grants_worker: cascade-skip grants on not-migrated objects (#9)

Co-authored-by: Isaac"
```

---

## Task 4: Full-suite verification + dashboard status recognition

**Files:**
- Modify (if needed): `dashboards/migration_dashboard.lvdash.json` (`hive_skipped_failed` dataset status filter)
- Test: `tests/unit/test_dashboard_smoke.py` (existing suite must stay green)

**Interfaces:** none new — this task verifies the new status surfaces in the existing skipped/failed panel and the whole suite is green.

- [ ] **Step 1: Check whether the dashboard panel already captures the new status**

Run: `grep -n "skipped_dependency_not_migrated\|status IN" dashboards/migration_dashboard.lvdash.json`
The `hive_skipped_failed` dataset (added earlier) filters `status IN ('failed', 'validation_failed', 'skipped', 'skipped_by_config')`.

- [ ] **Step 2: Add the new status to that filter** — in `dashboards/migration_dashboard.lvdash.json`, in the `hive_skipped_failed` dataset's `queryLines`, extend the `IN (...)` list to include `'skipped_dependency_not_migrated'`:

```
") WHERE rn = 1 AND status IN ('failed', 'validation_failed', 'skipped', 'skipped_by_config', 'skipped_dependency_not_migrated')"
```

- [ ] **Step 3: Run the dashboard smoke suite**

Run: `uv run pytest tests/unit/test_dashboard_smoke.py -v`
Expected: PASS (column-resolution + widget-ref checks still hold; the edit only widened a string literal).

- [ ] **Step 4: Run the full unit suite + ruff + CI-scope mypy**

Run: `uv run pytest tests/unit -q`
Expected: PASS (all prior tests + the new #9 tests).
Run: `uv run ruff check src/ tests/`
Expected: All checks passed.
Run: `uv run mypy src/common/`
Expected: same pre-existing errors as main, no NEW errors in changed files.

- [ ] **Step 5: Commit**

```bash
git add dashboards/migration_dashboard.lvdash.json
git commit -m "dashboard: surface skipped_dependency_not_migrated in hive skipped/failed panel (#9)

Co-authored-by: Isaac"
```

---

## Self-Review Notes

Spec requirements → tasks:
- "New shared helper `not_validated_object_names`" → Task 1.
- "New terminal status `skipped_dependency_not_migrated`" → Task 1 (registration) + used in Tasks 2/3.
- "Views worker: scan DDL textually, skip on not-migrated ref, transitive via topo order" → Task 2 (`view_dependency_skip` + `migrate_hive_view` gate; transitivity is automatic because a skipped view is itself in the not-migrated set on the same run — see note below).
- "Grants worker: skip per-object grant whose target isn't validated" → Task 3.
- "Terminal-status registration in get_pending_objects" → Task 1 (adds to `_TERMINAL_STATUSES`, which `get_pending_objects` consumes via `_TERMINAL_STATUSES_SQL`).
- "Dashboard recognizes the skip status" → Task 4.

**Transitivity (handled in Task 2 Step 3c):** views migrate in topological order; a view skipped this run is added back into `not_migrated_names` (`not_migrated_names.add(fqn)`), so a later view-on-a-skipped-view is caught in the same run. This makes the cascade fully transitive without a second pass. The topo sort guarantees dependencies are visited before dependents, so the set is always populated before a dependent is evaluated.

**Deferred (out of scope, matches spec):** functions-side cascade; a real SQL parser for view refs (textual heuristic retained).
