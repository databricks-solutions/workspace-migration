# Hive Dependency Skip Cascade (finding #9) — Design

## Problem

When a Hive object is **not migrated** — either config-skipped (a DBFS-root
table under `migrate_hive_dbfs_root=false` → `skipped_by_config`) or genuinely
failed — the tool still attempts to migrate objects that **depend on it**, and
those attempts hard-fail with raw errors:

- A view on the skipped table (`v_orders` → `SELECT … FROM dbfs_orders`) fails
  `TABLE_OR_VIEW_NOT_FOUND` on the target.
- The grants worker queues `OWNER_TABLE` / `OWNER_VIEW` (and other grants) on
  the skipped/absent objects → `TABLE_OR_VIEW_NOT_FOUND` / permission errors.

These are **expected consequences** of the skip, not real failures, but they
land in `migration_status` as `failed`. An operator reading the report cannot
distinguish "the tool broke" from "these were correctly skipped because DBFS-root
migration is off," which erodes trust and hides genuine failures in the noise.

Live evidence (2026-07-15): with `migrate_hive_dbfs_root=false`,
`hive_view v_orders` failed 42P01; `hive_grant OWNER_TABLE dbfs_orders` and
`OWNER_VIEW v_orders` failed 42P01.

## Goal

A dependent view or grant whose dependency was not migrated must be recorded
with a distinct, non-`failed` terminal status —
**`skipped_dependency_not_migrated`** — and its DDL / grant statement must not
be executed.

## Mechanism (query `migration_status`)

The skip information already lives in Delta. The Hive workflow runs
tables → views → grants in order, so by the time the views and grants workers
run, `migration_status` already holds the outcome of every table (e.g.
`dbfs_orders → skipped_by_config`). Workers consult that table rather than a
new task-value channel — consistent with the existing #12 anti-join pattern,
no size limit, and it reflects the **actual** outcome (catches config-skips
AND real failures, not just a predicted skip set).

"Migrated" means the object's **latest** status (by `migrated_at`) is
`validated`. Anything else — `skipped_by_config`, `failed`, `validation_failed`,
absent — counts as "not migrated" for cascade purposes.

### New shared helper

The views worker only receives its **view list** from the orchestrator (via
the `hive_view_list` task value) — it does NOT have the full set of table FQNs.
So instead of "scan for all known objects, then check each against a validated
set," we derive the **not-migrated set** directly and scan for exactly those.
That set is self-contained: the names in it ARE the things a dependent might
reference.

In `src/common/tracking.py` (method on `TrackingManager`):

```python
def not_validated_object_names(self, source_type: str | None = None) -> set[str]:
    """Object names whose LATEST migration_status is NOT 'validated'
    (skipped_by_config / failed / validation_failed / etc.).

    Used by the Hive views/grants workers to cascade-skip dependents of
    objects that were not migrated (finding #9). Reuses the latest-row
    window so a superseded in_progress row never counts. When ``source_type``
    is given, join discovery_inventory to scope to that source (e.g. 'hive').
    """
```

Implementation: `SELECT object_name FROM (<latest-row window>) WHERE status !=
'validated'` → set of `object_name`. The window is the same
`ROW_NUMBER() … PARTITION BY object_name, object_type ORDER BY migrated_at DESC`
used by `get_latest_migration_status`. Hive object names are namespace-unique
(`hive_metastore.db.t`) and won't collide with UC FQNs, so scoping to
`source_type='hive'` is a tidiness measure, not a correctness requirement.

### 1. Views worker (`hive_views_worker.py`)

- In `run()`, after fetching DDLs and before migrating, load
  `not_migrated = tracker.not_validated_object_names(source_type="hive")`.
- For each view (in the existing topological order), scan its DDL for any
  not-migrated object name using the **same textual technique**
  `_sort_views_by_deps` already uses (backticked or unquoted FQN substring
  match).
- If any not-migrated name appears in the DDL, record
  `skipped_dependency_not_migrated` (message names the missing dependency) and
  **skip execution** of the view DDL.
- Transitivity is automatic: a view on a skipped view runs later in topo order;
  the skipped view is itself in `not_migrated`, so the dependent view also skips.

A pure helper makes this unit-testable without Spark:

```python
def view_dependency_skip(ddl: str, not_migrated_names: set[str]) -> str | None:
    """Return the FQN of a not-migrated object the view DDL references
    (→ skip), or None if the DDL references no not-migrated object
    (→ migrate). Matches backticked (`hive_metastore`.`db`.`t`) and dotted
    (hive_metastore.db.t) forms, mirroring _sort_views_by_deps.
    """
```

### 2. Grants worker (`hive_grants_worker.py`)

- In `run()`, load `not_migrated = tracker.not_validated_object_names(source_type="hive")` once.
- Before emitting grants/ownership for a per-object securable, check whether its
  `object_name` is in `not_migrated`. If so, record a `hive_grant` row with
  status `skipped_dependency_not_migrated` (message names the object) and skip
  all grant/ownership statements for it. The grant's **target is the object
  itself** — no dependency parsing needed.
- Catalog- and schema-level grants are unaffected (the catalog/database always
  exists; only per-object grants are gated).

### 3. Terminal-status registration

- Add `skipped_dependency_not_migrated` to the terminal set in
  `TrackingManager.get_pending_objects` so re-runs don't reprocess these.
- Add it wherever skip statuses are recognized: the summary aggregation's
  skip handling and the dashboard `hive_skipped_failed` panel (its status
  filter already includes `skipped`/`skipped_by_config`; extend it).

## Testing (TDD, unit — no Spark in the unit env)

- **Helper `not_validated_object_names`**: asserts the emitted SQL uses the
  latest-row window and filters `status != 'validated'` (mock-spark call
  contract, mirroring `test_status_dedup.py`).
- **`view_dependency_skip` (pure)**:
  - DDL referencing a not-migrated table (backticked and dotted forms) →
    returns that FQN (skip);
  - DDL referencing no not-migrated object → returns None (migrate);
  - transitive: DDL referencing a not-migrated *view* → returns that FQN.
- **Views worker**: with `not_migrated = {dbfs_orders}`, a view whose DDL
  references it records `skipped_dependency_not_migrated` and does NOT call
  `execute_and_poll`; a view referencing nothing not-migrated migrates normally.
- **Grants worker**: target in not-migrated-set → grant row
  `skipped_dependency_not_migrated`, no `execute_and_poll`; target not in the
  set → grant emitted as today.
- **Terminal status**: `get_pending_objects` treats
  `skipped_dependency_not_migrated` as terminal.

## Out of scope / caveats

- Relies on the Hive workflow ordering (tables → views → grants), which already
  holds in `hive_*_workflow.yml`.
- View dependency detection is **textual** (the same heuristic the existing
  topological sort uses), not a real SQL parser — documented as such; it can
  over- or under-match in pathological DDL, consistent with current behavior.
- Functions: Hive function DDLs are replayed independently and don't reference
  tables in a way this cascade targets; no function-side change in this cycle.
