# Retry / Resumability (X.1)

**Date:** 2026-04-23
**Task:** X.1 — retry / resumability
**Scope:** `migrate` workflow only (pre_check + discovery do not need resumability — they are read-only probes).

## Purpose

The operator kills the `migrate` workflow mid-flight (cluster death, manual stop, timeout). On restart the tool must:

1. Pick up where it left off — do not re-run already-finished objects.
2. Safely retry objects that were half-processed when the crash happened.
3. Clean up any partial target state that would block the retry (half-copied volume files, half-added share objects).
4. Surface cases that need operator intervention instead of silently retrying forever.

The design builds on work shipped in X.2 (per-worker idempotency audit, PR #38) and X.4 (target collision handling, PR #39):

- X.2 pins that every worker tolerates "already exists" on retry — so a retry is safe when the prior run succeeded on the target but crashed before writing the `validated` row.
- X.4 gives `get_pending_objects` the terminal-status IN-list pattern — new terminal statuses (e.g. `skipped_target_exists`) are opt-in.

## Reconciliation decision table

Reconciliation runs **once** at the start of every `migrate` workflow, in `migrate.orchestrator`, right after `check_collision_gate`. It scans the latest status row per `(object_name, object_type)` in `migration_status` and decides what to do:

| Prior status                                  | Action          | Why |
|-----------------------------------------------|-----------------|-----|
| `validated`                                   | no-op           | Terminal — filtered out by `get_pending_objects`. |
| `skipped_by_pipeline_migration`               | no-op           | Terminal — DLT-owned objects never re-migrate. |
| `skipped_target_exists` (X.4)                 | no-op           | Terminal — pre-existing target left alone. |
| `skipped_by_stateful_service_migration`      | no-op           | Terminal — object deferred to the future Stateful Services Phase (separate job). Today: streaming tables. See `docs/stateful_services_phase.md`. |
| `in_progress`, stale `job_run_id`             | **reset → pending** | Worker died mid-flight. Calls cleanup hook, appends new `pending` row. |
| `in_progress`, current `job_run_id`| no-op           | This run’s own worker; hands-off. |
| `failed`                           | no-op           | Workers already re-pickup via `get_pending_objects`. Reconciler does not rewrite. |
| `validation_failed`                | surface only    | Needs operator attention. Appears in summary. Row left alone. |
| `skipped_by_config`                | no-op           | Flag-gated skip (iceberg / rls_cm / dry_run). Operator flips the flag and reruns. |
| `pending`                          | no-op           | Worker will pick up on its own; reconciler does not double-write. |

The reset appends a new `migration_status` row with `status='pending'` and an `error_message` that records which prior `job_run_id` owned the orphan, so operators can trace back.

**Why "reset to pending" is safe.** X.2’s audit pins that every worker with "already exists" tolerance will no-op on retry if its target object survived the crash. If the target did not survive, the worker will recreate it. The reset row is a breadcrumb for the operator — the *actual* retry driver is `get_pending_objects`, which already treats non-terminal rows as candidates.

## Per-worker cleanup matrix

Some workers leave partial target state that would wedge the retry. Those define a `cleanup_<worker>_<role>` hook that reconciliation invokes before resetting the row. Others need no cleanup because their idempotency is built into the DDL itself.

| Worker                        | Cleanup needed? | Why |
|-------------------------------|-----------------|-----|
| `managed_table_worker`        | No              | `CREATE OR REPLACE TABLE ... DEEP CLONE` — next run overwrites cleanly. |
| `external_table_worker`       | No              | `CREATE TABLE IF NOT EXISTS` — idempotent. |
| `views_worker`                | No              | `CREATE OR REPLACE VIEW`. |
| `functions_worker`            | No              | `CREATE OR REPLACE FUNCTION`. |
| `volume_worker` (EXTERNAL)    | No              | `CREATE EXTERNAL VOLUME IF NOT EXISTS` at same storage_location. |
| `volume_worker` (MANAGED)     | **Yes** → `cleanup_partial_target` | `dbutils.fs.cp` may have copied only a subset of files. Drop the target volume so the retry starts clean. |
| `sharing_worker` (share)      | **Yes** → `cleanup_partial_share` | `ALTER SHARE ADD` may have added a subset of objects. Drop the share entirely; apply_share recreates from the live spec on retry. |
| `sharing_worker` (recipient)  | No              | `recipients.create` tolerates "already exists". |
| `sharing_worker` (provider)   | No              | `providers.create` tolerates "already exists". |
| `mv_st_worker` (MV)           | No              | X.2 added "already exists" tolerance + REFRESH always re-runs. |
| `mv_st_worker` (ST)           | No              | Hard-excluded — short-circuits to `skipped_by_stateful_service_migration` before any target state is touched. See `docs/stateful_services_phase.md`. |
| `tags` / `row_filters` / `column_masks` | No    | `ALTER … SET` is idempotent. |
| `policies` / `monitors` / `connections` / `online_tables` | No | X.2 added "already exists" tolerance. |
| `comments_worker`             | No              | `COMMENT ON ... IS` overwrites. |
| `models_worker`               | No              | Each sub-call is idempotent. |
| `grants_worker`               | No              | Re-reads source on every run; server-side GRANT is idempotent. |
| `foreign_catalogs_worker`     | No              | Already tolerates "already exists". |
| Hive workers (all)            | No              | DDL is `CREATE … IF NOT EXISTS` or `CREATE OR REPLACE`; classified in the X.2 audit. |

Hooks implement this interface:

```python
def cleanup_<role>(object_name: str, *, auth: AuthManager, spark, config) -> None: ...
```

They must be **best-effort**: NOT_FOUND errors are swallowed (the crash may have happened before the target object was created), but other errors propagate so the reconciler can log + continue (the reset row is appended regardless).

## Job-run identity

Each `migration_status` row carries a `job_run_id` column. Reconciliation resolves the current run’s ID from Databricks Jobs context (`dbutils.notebook.entry_point.getDbutils().notebook().getContext()`) and threads it as `current_job_run_id` to every worker that writes status rows. Orphaned `in_progress` rows are defined as:

- `status = 'in_progress'`, **and**
- `job_run_id != current_job_run_id` (or `job_run_id IS NULL`)

`NULL` job_run_id is treated as orphaned — the safe direction (retry vs. hang waiting for a worker that is not coming back).

> Implementation note: legacy status rows written before X.1 carry `job_run_id=NULL`. On the first X.1-enabled run those rows will be reconciled — which is desirable; they are almost certainly genuine orphans.

## Kill-injection (test only)

Config field `test_kill_after: int | None`. When set to `N`, workers raise `SystemExit` after processing N objects in the current batch. This is how the retry/resumability integration fixture simulates a mid-batch crash.

The field is **refused at runtime** unless the process is running under a test profile:

- `WSM_TEST_MODE=1` in the environment, OR
- `DATABRICKS_ENVIRONMENT` starts with `test` (ring/staging conventions).

If neither signal is present, `maybe_kill` raises `RuntimeError` the moment it sees a positive `test_kill_after`. That keeps an accidental `test_kill_after: 1` in a production `config.yaml` from dropping half the operator's catalog on the floor.

Currently wired into `managed_table_worker.clone_table` — the most-used worker in the integration fixture. Wiring more workers is additive; the helper is small:

```python
from migrate.reconciliation import maybe_kill
maybe_kill(config, _bump_kill_counter(), "my_worker")
```

## Interaction with the staging manifest (Path A)

Path A (`rls_cm_strategy='staging_copy'`) ships a separate crash-recovery mechanism for the staging tables that `setup_sharing` creates in `<tracking_catalog>.cp_migration_staging`: the `rls_cm_staging_manifest` table records each staging table the run materialised so the post-migrate `cleanup_staging` task can drop them — including stagings left behind by an earlier crashed run. That manifest is **not** consumed by reconciliation.

The manifest schema is `(original_fqn, staging_fqn, created_at, dropped_at, drop_failed_at, drop_error, run_id)`. `cleanup_staging.py` reads `tracker.get_active_stagings()` (every row where `dropped_at IS NULL`), issues `DROP TABLE IF EXISTS <staging_fqn>` per row, and stamps `dropped_at` on success or `drop_failed_at` + `drop_error` on failure. A single bad drop never blocks the rest of the batch — the task continues, then raises at the end if anything failed so the operator sees the workflow task fail. Re-running the task is idempotent: `DROP TABLE IF EXISTS` tolerates already-dropped tables, and the `WHERE dropped_at IS NULL` filter naturally excludes finished work.

Both systems coexist by design:

- `rls_cm_staging_manifest` tracks **target-side staging tables** created during `setup_sharing` (rare, opt-in to the `staging_copy` strategy, narrow blast radius — only RLS/CM-bearing tables).
- `migration_status` tracks **target-side migration progress** for every object across every run.

Merging them would couple two unrelated lifecycles and hurt the clarity of both. The reconciler skips any row whose `status` is staging-specific (there is no such status today — `setup_sharing` writes only its own scoped fields into the manifest, not `migration_status`).

## Deferred for follow-up

- **"Already exists" helper** (X.2 recommendation). Reconciliation still string-matches `"already"` + `"exist"` / `"found"` in cleanup error paths. A shared helper with a regex set or a mapping of SDK exception classes would be more robust.
- **Reconciliation for governance workers** (`tags`, `row_filters`, `column_masks`, `policies`, `monitors`, `comments`, `connections`, `foreign_catalogs`, `online_tables`, `registered_model`, `provider`, `recipient`). These workers write synthetic status keys (e.g. `RECIPIENT_<name>`) that do not join on discovery_inventory, so `get_pending_objects` always re-emits them. That is safe (idempotency audit pins every one) but means the reconciler never sees them as "orphaned" — because there are no "ours" in the `migration_status` table beyond the ones we wrote right before a potential crash. If incremental resume is needed for governance later, align the key between discovery and worker.
- **Kill-injection in more workers.** `managed_table_worker` is enough to exercise the end-to-end reconciliation flow; adding to each worker costs ~3 lines but is not needed until we want per-worker crash tests.
- **Stale in_progress by timestamp.** The reconciler uses `job_run_id != current_job_run_id` as the orphan signal. A second-tier heuristic — "`migrated_at` older than N hours" — would catch the rare case where an in_progress row somehow carries the current run ID (e.g. a retry within the same Jobs run). Not needed today because Jobs for_each tasks always get a fresh run_id.
- **Recovery from destroyed workspaces.** Out of scope by design; a missing target metastore is a different category of disaster and needs its own playbook.

## Running the tests

```bash
uv run pytest tests/unit/test_reconciliation.py -v
```

42 unit tests cover the decision table, the kill-injection safety guard, and the per-worker cleanup hooks. Total suite after X.1: **737 passing** (695 baseline + 42).

Integration fixture: `tests/integration/test_retry_resumability.py`. Designed to run after the UC end-to-end fixture has seeded data; the fixture run is deferred (no live integration runs this PR) but the code is ready to be invoked from the `uc_integration_test` workflow.
