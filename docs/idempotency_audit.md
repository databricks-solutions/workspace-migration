# Per-Worker Idempotency Audit (X.2)

**Date:** 2026-04-23
**Task:** X.2 — prerequisite for X.1 (retry / resumability)
**Scope:** every worker under `src/migrate/` — 25 workers total.

## Purpose

The migration tool writes progress to
`main.migration_tracking.cp_migration.migration_status`, keyed by
`(object_name, object_type, source_type)`. The orchestrator uses
`TrackingManager.get_pending_objects` to filter out already-terminal rows
before handing a batch to the worker. Historically, each worker has its own
ad-hoc handling of "object already present on target" and "status row
already exists". This document pins current behaviour per worker and
identifies fixes required before retry/resumability (X.1) can safely
assume independent worker idempotency.

## Terminal / non-terminal status values

Terminal (filtered by `get_pending_objects`):

- `validated`
- `skipped_by_pipeline_migration` (DLT-owned MV; the pipelines workflow
  handles it)
- `skipped_target_exists` (X.4 — pre-existing target object under the
  `on_target_collision: skip` policy)
- `skipped_by_stateful_service_migration` (streaming tables, and the
  general escape hatch for objects moved to the future Stateful
  Services Phase — see `docs/stateful_services_phase.md`)

Non-terminal (re-picked-up on next run):

- `pending` — written by discovery only, not by workers.
- `in_progress` — written at the start of every worker's per-object call.
- `failed` — post-error.
- `validation_failed` — DDL/data succeeded but row-count mismatch.
- `skipped_by_config` — emitted by `hive_managed_dbfs_worker` when the
  opt-in flag is false. Worker re-emits on retry.
- `skipped_no_access` — emitted by discovery for securables the migration
  principal cannot read. Worker should never see these (discovery filters
  them from inventory); if one reaches a worker, behaviour is undefined
  (pin: re-runs the worker, which will fail a second time).

## How to read the tables

For each worker the matrix below lists **input status** (what the status
row contains when the orchestrator picks up the object) × **target
workspace state** (missing, exists, partial) and records the **current
behaviour** — as implemented today, pinned by a unit test in
`tests/unit/test_idempotency_audit.py`.

Three statuses (`validated`, `skipped`) are filtered upstream and never
reach the worker, so every row for those is `filtered upstream` and not
repeated per worker.

---

## UC Workers

### `managed_table_worker.clone_table`

DDL: `CREATE OR REPLACE TABLE {target} DEEP CLONE {consumer}` (Delta),
or DDL replay + INSERT (Iceberg opt-in).

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| `pending` | any | CREATE OR REPLACE; validate row count | yes — replaces existing |
| `in_progress` | any | same | yes (orphaned in_progress is retried, DEEP CLONE replaces) |
| `failed` | partial | same | yes — DEEP CLONE replaces any partial data |
| `validation_failed` | exists | same | yes |
| iceberg w/o opt-in | any | `status=skipped`, err=needs opt-in | yes |

Pin tests: `TestManagedTableIdempotency`.

### `external_table_worker.migrate_external_table`

DDL: `CREATE TABLE IF NOT EXISTS` (rewrite).

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any | missing | CREATE; validate | yes |
| any | exists | CREATE IF NOT EXISTS → no-op; validate | yes — IF NOT EXISTS guard |

Pin tests: `TestExternalTableIdempotency`.

### `views_worker.migrate_view`

DDL: `CREATE OR REPLACE VIEW`.

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any | any | CREATE OR REPLACE | yes |

Pin tests: `TestViewsIdempotency`.

### `functions_worker.migrate_function`

DDL: `CREATE OR REPLACE FUNCTION`.

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any | any | CREATE OR REPLACE | yes |

Pin tests: `TestFunctionsIdempotency`.

### `volume_worker.migrate_volume`

- EXTERNAL: `CREATE EXTERNAL VOLUME IF NOT EXISTS` at same storage location.
- MANAGED: ALTER SHARE ADD (source) → `CREATE VOLUME IF NOT EXISTS` on
  target → target-side notebook does `dbutils.fs.cp` per file → ALTER
  SHARE REMOVE.

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any (external) | any | CREATE IF NOT EXISTS | yes |
| any (managed) | missing | share + create + copy | yes — copy is new |
| any (managed) | exists (partial files) | share + CREATE IF NOT EXISTS + copy | mostly — copy re-runs, `dbutils.fs.cp` overwrites per file; no checksum verification |
| any | share "already" | tolerated by try/except + string match on "already" | yes |

Pin tests: `TestVolumeIdempotency`.
Caveat: partial volume copies on retry will re-copy every file (no
incremental logic). Not a bug — just slow on re-run.

### `grants_worker.replay_grants`

DDL: `GRANT {priv} ON {securable} TO {principal}`.

Worker does NOT read tracking; it re-reads `SHOW GRANTS` on every run.
UC `GRANT` is server-side idempotent (repeated grants are no-ops).
`OWN` action_type is skipped (ownership is set via `ALTER ... OWNER TO`,
not GRANT).

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| — | — | re-read + replay every GRANT every run | yes (server-side no-op) |
| action=OWN | — | skip | yes |

Pin tests: `TestGrantsIdempotency`.

### `comments_worker._emit_comment`

DDL: `COMMENT ON {securable} IS 'text'`. Overwrites — idempotent.

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any | any | overwrite | yes |

Worker re-reads from source `information_schema` on every run; no tracking
filter applied.

Pin tests: `TestCommentsIdempotency`.

### `mv_st_worker.migrate_mv_st`

DDL: `CREATE MATERIALIZED VIEW` (no OR REPLACE is supported by
Databricks) + `REFRESH`.

**Scope:** this worker covers **materialized views only**. Streaming
tables are hard-excluded from the core migration tool and short-
circuit to `skipped_by_stateful_service_migration` before any DLT
detection or DDL execution — the future Stateful Services Phase (a
separate job) migrates them with proper offset / checkpoint handling.
See `docs/stateful_services_phase.md`.

**Bug (fixed in PR #38):** on retry the CREATE fails with
`[TABLE_OR_VIEW_ALREADY_EXISTS]`, previously marking the object failed.
The worker now tolerates "already exists" errors from the CREATE step
and proceeds to `REFRESH`.

| Object type | Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|---|
| `mv` | any | missing | CREATE + REFRESH | yes |
| `mv` | any | exists (retry) | CREATE fails "already exists" → proceed to REFRESH | yes (after fix) |
| `mv` (DLT-defined) | any | any | `skipped_by_pipeline_migration` | yes |
| `mv` | any | any other CREATE error | `failed` | correct |
| `st` | any | any | `skipped_by_stateful_service_migration` (short-circuit) | yes — terminal skip |

Pin tests: `TestMvStIdempotency`.

### `tags_worker.apply_tag_group`

DDL: `ALTER {securable} SET TAGS ('k' = 'v', ...)`. Upsert by key — idempotent
server-side.

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any | any | ALTER SET TAGS (upsert) | yes |

Pin tests: `TestTagsIdempotency`.

### `row_filters_worker.apply_row_filter`

DDL: `ALTER TABLE {t} SET ROW FILTER {fn} ON (...)`. Replaces existing
row filter — idempotent server-side.

Pin tests: `TestRowFiltersIdempotency`.

### `column_masks_worker.apply_column_mask`

DDL: `ALTER TABLE ... ALTER COLUMN c SET MASK {fn} USING COLUMNS (...)`.
Replaces existing mask — idempotent server-side.

Pin tests: `TestColumnMasksIdempotency`.

### `policies_worker.apply_policy`

API: `POST /api/2.1/unity-catalog/policies`.

**Bug (fixed in this PR):** on retry the POST fails with "already exists"
and previously marked the object failed. Now tolerated and returned as
validated.

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any | missing | POST → validated | yes |
| any | exists (retry) | POST fails "already exists" → now validated (fix) | yes (after fix) |
| any | other error | failed | correct |

Pin tests: `TestPoliciesIdempotency`.

### `monitors_worker.apply_monitor`

API: `POST /api/2.1/unity-catalog/tables/{name}/monitor`.

**Bug (fixed in this PR):** same pattern — POST fails with "already
exists" on retry, now tolerated.

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any | missing | POST → validated | yes |
| any | exists (retry) | validated (fix) | yes |

Pin tests: `TestMonitorsIdempotency`.

### `connections_worker.apply_connection`

SDK: `connections.create(...)`. Secret fields (password, client_secret,
etc.) aren't returned by source's GET API — a successful create may still
produce `validation_failed` if there are credential gaps.

**Bug (fixed in this PR):** on retry the SDK call fails with "already
exists" and previously marked the object failed. Now tolerated — if the
connection already exists we still run the credential-gap check and
report `validation_failed` (same as first-run behaviour when creds are
missing).

| Input status | Target | Current behavior | Idempotent? |
|---|---|---|---|
| any | missing, no creds needed | create → validated | yes |
| any | missing, creds redacted | create → validation_failed (user must re-enter) | yes |
| any | exists (retry) | fall through to credential-gap check | yes (after fix) |
| any | other error | failed | correct |

Pin tests: `TestConnectionsIdempotency`.

### `foreign_catalogs_worker.apply_foreign_catalog`

SDK: `catalogs.create(connection_name=...)`. Already handles "already
exists" — emits `validated` with an informational message. No fix needed.

Pin tests: `TestForeignCatalogsIdempotency`.

### `online_tables_worker.apply_online_table`

API: `POST /api/2.0/online-tables`.

**Bug (fixed in this PR):** POST fails with "already exists" on retry,
previously marked the object failed. Now tolerated.

Pin tests: `TestOnlineTablesIdempotency`.

### `sharing_worker`

Three paths: `apply_share`, `apply_recipient`, `apply_provider`.

- `apply_share`: `shares.create(...)` then `ALTER SHARE {s} ADD
  {type} {fqn}` per object.

**Bug (fixed in this PR):** `ALTER SHARE ADD` errors with "already in
share" on retry. Previously each such object counted as a failure,
marking the entire share `validation_failed`. Now the worker counts
"already in share" errors separately (`already_present`) and treats a
clean retry (only already-present objects, no real failures) as
`validated` with no error_message.

- `apply_recipient`: already tolerates "already exists". No fix needed.
- `apply_provider`: already tolerates "already exists". No fix needed.

Pin tests: `TestSharingIdempotency`.

### `models_worker.apply_model`

Three sub-calls per model:

1. `registered_models.create` — tolerates "already exists" (pre-existing).
2. `model_versions.create` — tolerates "already exists" (pre-existing).
3. `registered_models.set_alias` — SDK overwrites the alias mapping,
   idempotent.

No new fixes needed.

Pin tests: `TestModelsIdempotency`.

---

## Hive Workers

### `hive_external_worker.migrate_hive_external_table`

DDL: `SHOW CREATE TABLE` → rewrite namespace `hive_metastore.` →
`{hive_target_catalog}.` → `CREATE TABLE IF NOT EXISTS`.

Idempotent via `IF NOT EXISTS`.

Pin tests: `TestHiveExternalIdempotency`.

### `hive_views_worker.migrate_hive_view`

DDL: extract from `DESCRIBE EXTENDED` View Text → rewrite namespace →
`CREATE OR REPLACE VIEW`. Idempotent.

Pin tests: `TestHiveViewsIdempotency`.

### `hive_functions_worker.migrate_hive_function`

DDL: extract from `DESCRIBE FUNCTION EXTENDED` → rewrite namespace →
`CREATE OR REPLACE FUNCTION`. Idempotent.

Pin tests: `TestHiveFunctionsIdempotency`.

### `hive_managed_dbfs_worker.migrate_hive_managed_dbfs`

1. Read source via Spark.
2. Write target path via `df.write.mode("overwrite").format("delta").save(path)`.
3. Register on target: `CREATE TABLE IF NOT EXISTS {target} USING DELTA
   LOCATION '{path}'`.

Idempotent: overwrite is clean, CREATE IF NOT EXISTS is idempotent.

`migrate_hive_dbfs_root=false` yields `skipped_by_config`, which is a
non-terminal status (retry re-emits). This is intentional — operator can
toggle the flag between runs.

Pin tests: `TestHiveManagedDbfsIdempotency`.

### `hive_managed_nondbfs_worker.migrate_hive_managed_nondbfs`

`SHOW CREATE TABLE` → rewrite namespace → ensure `LOCATION` clause →
`CREATE TABLE IF NOT EXISTS` → `MSCK REPAIR` (non-Delta).

Idempotent: `CREATE TABLE IF NOT EXISTS` + `MSCK REPAIR` can run
repeatedly without side effects.

Pin tests: `TestHiveManagedNondbfsIdempotency`.

### `hive_grants_worker._emit_grant`

Map Hive action → UC privilege → `GRANT`. Server-side idempotent.
`OWN` and unmapped privileges are skipped.

Pin tests: `TestHiveGrantsIdempotency`.

### `hive_common`

Shared helpers only — `rewrite_hive_namespace`, `rewrite_hive_fqn`,
`ensure_target_catalog_and_schema` (`CREATE CATALOG/SCHEMA IF NOT
EXISTS`), `HIVE_TO_UC_PRIVILEGES` mapping. No worker body.

---

## Summary

**Workers audited:** 25 (18 UC + 6 Hive workers + 1 shared helper).
**Cells pinned by unit tests:** 51 (in `tests/unit/test_idempotency_audit.py`).
**Bugs found + fixed in this PR:** 6.

1. `mv_st_worker` — CREATE MATERIALIZED VIEW / STREAMING TABLE does not
   support OR REPLACE. On retry of a succeeded-but-tracked-as-failed
   MV/ST the CREATE would error "already exists" and mark the object
   failed. Now tolerated: proceed to REFRESH and return validated.
2. `policies_worker` — POST /policies had no "already exists" handling.
   Now tolerated and returns validated.
3. `monitors_worker` — POST /monitor had no "already exists" handling.
   Now tolerated.
4. `connections_worker` — connections.create had no "already exists"
   handling. Now tolerated (credential-gap check still runs).
5. `online_tables_worker` — POST /online-tables had no "already exists"
   handling. Now tolerated.
6. `sharing_worker.apply_share` — `ALTER SHARE ADD` errors with
   "already in share" on retry. Previously marked the entire share
   `validation_failed`. Now counted as `already_present` and a clean
   retry is reported as validated.

## Recommendations for X.1 (retry / resumability)

1. **Terminal-state filter:** `TrackingManager.get_pending_objects`
   currently filters on `status NOT IN ('validated', 'skipped')`
   (explicit IN-list, per PR #26). Every other status is retried:
   `failed`, `in_progress`, `validation_failed`, `skipped_by_config`,
   `skipped_no_access`, `skipped_by_pipeline_migration`.
   - `skipped_by_pipeline_migration` is intentionally non-terminal so
     a DLT-owned MV/ST doesn't permanently block the row; on retry the
     MV/ST worker re-skips it (harmless).
   - `skipped_by_config` is intentionally non-terminal so operators
     can toggle the opt-in flag (e.g. `migrate_hive_dbfs_root=true`)
     and resume.
   - `skipped_no_access` is written by discovery only; it reaches a
     worker only if inventory is replayed — treat as retry-safe.
   - Before X.1 decide whether any of these should become terminal
     (for example, `skipped_no_access` from a final inventory pass).
2. **Discovery ↔ worker object_name mismatch for Phase 3 governance
   workers:** `tags`, `row_filter`, `column_mask`, `comment`, `monitor`,
   `policy`, `connection`, `foreign_catalog`, `share`, `recipient`,
   `provider`, `online_table` — discovery writes rows with one
   `object_name` (e.g. the table FQN or recipient name) while the
   worker writes the status row with a synthetic key (e.g.
   `TAGS_TABLE_{fqn}` / `RECIPIENT_{name}`). The join in
   `get_pending_objects` therefore never matches, and every run
   re-emits every governance row. This is safe (all governance ops are
   idempotent, as the audit shows) but means X.1's resume semantics
   will always re-run these workers in full. If X.1 wants true
   incremental resume for governance, align the two sides on a single
   key.
3. **`grants_worker` / `comments_worker` / `hive_grants_worker`** do
   not read the tracker at all — they re-read source state on every
   run and re-apply. X.1 can rely on these being fully idempotent.
4. **Partial state detection** — no worker today checks target state
   before acting (no GET-then-decide). Every fix above assumes the
   API / SQL layer surfaces an "already exists" error we can string-match
   on. If Databricks changes error wording, the idempotency fixes
   quietly break. X.1 should consider either a shared "already_exists"
   helper with a regex set, or per-worker `target_client.*.get(...)`
   lookups.
5. **Volumes on retry** — managed-volume copy re-copies every file.
   For X.1 this is slow but safe; if resumability needs to skip
   already-copied files the copy notebook needs a size/mtime check
   before `dbutils.fs.cp`.
6. **MV/ST REFRESH state** — REFRESH after the "already exists" path
   will start a new refresh that may conflict with the first run's
   still-running refresh. Behaviour is: `execute_and_poll` waits for
   the REFRESH to finish, so only one refresh is ever in flight from
   the tool's perspective.

## Running the audit tests

```bash
uv run pytest tests/unit/test_idempotency_audit.py -v
```

205 total unit tests pass on main + this PR (154 baseline + 51 new).

---

## Collision handling (X.4)

X.2 (this audit) pinned **per-worker resume idempotency**: if the migrator
ran yesterday and left a partial state, today's re-run must tolerate
"already exists" errors because the object WE created is still there.

X.4 adds the **other direction**: what if the target has an object we
*didn't* create and don't know about? The audit's state matrix answers
one dimension (input status × target state) but treats the target object
as always ours. X.4 layers collision scenarios on top:

- Operator ran a dev test migration yesterday against the same target
  and forgot to tear down.
- Customer created a catalog `retail` manually on target; source also
  has one, schema shape differs.
- Same-named recipient / share exists on target.
- Pre-existing UC ABAC policy / monitor / online-table attached by
  some other pipeline.

Before X.4: collisions silently got overwritten (DEEP CLONE `CREATE OR
REPLACE`), silently skipped (`CREATE TABLE IF NOT EXISTS`), or silently
succeeded against pre-existing objects (X.2's "already exists"
tolerance). The operator had no way to tell.

### Policy

Config field: `on_target_collision: fail | skip` (default: `fail`).

| Policy | Default? | Pre_check row | migrate behaviour |
|---|---|---|---|
| `fail` | yes | status=FAIL | Orchestrator gate refuses to run. Operator must rename / drop the colliding target object, then rerun pre_check. |
| `skip` | no | status=WARN | Pre_check seeds `skipped_target_exists` rows in `migration_status`. Workers short-circuit those objects on next run (now a terminal status in `get_pending_objects`). Target object is left untouched. |

No `overwrite` policy in v1 — destroying customer data by default is
almost never what anyone wants. Operators who truly want overwrite can
manually drop the target object and rerun.

### Detection

`src/pre_check/collision_detection.py` implements the probe. For each
row in `discovery_inventory`:

1. Skip if the row has a matching `(object_type, object_name)` entry in
   `migration_status`. That status row says we own the object — X.2's
   tolerance covers "already exists" in this case.
2. Probe the target metastore via SDK getter per object_type:
   - `catalog` → `catalogs.get(name=...)`
   - `schema` → `schemas.get(full_name=...)`
   - `managed_table` / `external_table` / `view` → `tables.get(full_name=...)`
   - `function` → `functions.get(name=...)`
   - `volume` → `volumes.read(name=...)`
   - `hive_table` / `hive_view` → rewrite to `<hive_target_catalog>.<db>.<t>` then `tables.get`.
3. SDK getter returning an object → collision. 404 / NOT_FOUND → no
   collision.

Phase 3 governance types (`share`, `recipient`, `provider`, `monitor`,
`registered_model`, `connection`, `foreign_catalog`, `online_table`,
`tag`, `row_filter`, `column_mask`, `comment`, `policy`) are
intentionally out of scope for v1. Those workers already tolerate
"already exists" (see the per-worker table above), so a pre-existing
target object there is safe to re-apply against. Future work can extend
`_PROBES` if we need strict fail-fast for governance too.

### Wiring

- `pre_check.pre_check` runs the collision check as `check_target_collisions`
  after the other prerequisites, emitting a pre_check_results row with
  PASS / WARN / FAIL per policy. Under `skip`, it also calls
  `tracker.append_migration_status` with `skipped_target_exists` rows.
- `migrate.orchestrator.check_collision_gate` (new helper) reads the
  latest `check_target_collisions` row per `check_name` and raises
  `RuntimeError` if the status is FAIL. The orchestrator notebook calls
  this gate right after config load, before publishing task values.
- `TrackingManager.get_pending_objects` adds `skipped_target_exists`
  to the terminal-status IN-list so workers never re-pick-up a skipped
  collision on later runs (unless the operator deliberately deletes the
  status row).

### Tests

- `tests/unit/test_collision_detection.py` (43 tests): per-securable
  probe matrix, Hive rewrite, X.2 compatibility (status row blocks
  re-probing), unsupported-type handling, `build_skip_status_rows`
  schema.
- `tests/unit/test_pre_check.py::TestPreCheckCollisionDetection` (7 tests):
  empty discovery, no collision, collision under fail, collision under
  skip + status row seeding, status-row-blocks-collision (X.2 compat),
  per-type summary message, missing discovery_inventory gracefully
  degrades.
- `tests/unit/test_orchestrator.py::TestOrchestratorCollisionGate`
  (8 tests): no rows passes, PASS / WARN no-op, FAIL raises, message
  surfaced in error, missing table is warn-and-return, query filters to
  target_collision rows + latest row per check_name, multi-row safety.
- `tests/unit/test_config.py::TestCollisionPolicyConfig` (6 tests):
  defaults, valid values, unknown value raises, case-insensitive,
  empty-string is fail.
- `tests/integration/test_collision_handling.py`: end-to-end fixture
  (run-ready, not live-tested): seeds a rogue target catalog, asserts
  pre_check_results + migration_status under each policy, asserts the
  rogue catalog is left untouched, asserts `check_collision_gate` raises
  under fail and not under skip.

### Interaction with X.2

Collision detection must NOT fire when the object is already tracked in
`migration_status` (even with a non-terminal status like `in_progress` or
`failed`). A tracked object is ours — X.2 handles the "we crashed mid-
migrate, re-run" case, and adding a collision row there would double-
gate the operator out of their own resume. The
`existing_status_keys` argument to `detect_collisions` enforces this;
the test `test_status_row_blocks_collision` pins it.

