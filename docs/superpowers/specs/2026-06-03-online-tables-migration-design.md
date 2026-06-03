# Online Tables Migration (`migrate_online_tables`) — design

**Date:** 2026-06-03
**Status:** approved (brainstorm complete) — implementation plan pending
**Scope:** Migrate Unity Catalog **Online Tables** from source to target as a new
standalone `migrate_online_tables` job (one job per stateful service). Replaces
the current Phase-4 hard-exclude. Closely mirrors the live-validated
`migrate_vector_search` job (PR #54), minus the endpoint concept and the
Direct-Access negative case. Consumes the `source_type='stateful'`,
`object_type='online_table'` rows already produced by discovery (PR #53).

## Background

An Online Table is a read-only, low-latency copy of a UC Delta table served from
an online store, kept in sync from a source Delta table (which must have a
primary key). The online store's materialized copy + sync cursor are runtime
state that a CREATE cannot transfer — so "migration" = recreate the spec on the
target pointing at the (already-migrated) source table, which re-syncs from
scratch (sync history lost; same accepted trade-off as Vector Search re-embed).

Today `online_tables_worker.py` hard-excludes every online table
(`skipped_by_stateful_service_migration`) and runs **inside the `migrate_uc`
job**. This spec turns it into a real migration in its own job.

## Decisions (brainstorm 2026-06-03)

| # | Decision |
|---|---|
| Placement | New standalone `migrate_online_tables` job. REMOVE the `migrate_online_tables` task from `migrate_uc_workflow.yml`, drop it from `summary_uc`'s `depends_on`, and remove `online_table` from `summary_uc`'s `object_types` (it's `source_type='stateful'`, not UC data). |
| Create API | Typed SDK `target_client.online_tables.create(OnlineTable(name, spec))`, reconstructing `OnlineTableSpec` from the discovered definition and dropping the response-only `pipeline_id` (same pattern as the VS worker). |
| Statuses | Reuse `created_resync_pending` (on accept) + `skipped_target_exists` (already exists). NO new statuses — no endpoint, no Direct-Access. |
| Pre-check | Fail the job up-front if any online table's `source_table_full_name` is absent on target (mirrors VS pre-check). |
| Test modes | Integration seeds a **Triggered** online table (positive case). Worker is mode-agnostic (recreates whatever spec was discovered), so one mode validates the path for Snapshot/Continuous/timeseries too. |
| Live run | Build full real test, then probe whether an online table is creatable on the pair; run live if yes, else ship for later (no false validation). |
| `object_name` fix | Worker records `object_name = <online_table_fqn>` (matching discovery), NOT the legacy `ONLINE_TABLE_<fqn>` (which never matched discovery → row reprocessed forever). |

## Architecture & components

New job `migrate_online_tables`, task chain:
```
pre_check_online_tables → orchestrator → migrate_online_tables → summary_online_tables
```
The `orchestrator` task reuses the shared `src/migrate/orchestrator.py`, which
already publishes `online_table_list` (online_table is in `LIST_TYPES`). The
worker consumes that task value, exactly as it does today and as the VS job
consumes `vector_search_index_list`.

New/changed files:
- `src/migrate/online_tables_worker.py` — **rewrite**: hard-exclude → real migration.
- `src/pre_check/pre_check_online_tables.py` — **new**: source-table gate.
- `resources/production/migrate_online_tables_workflow.yml` — **new** job.
- `resources/production/migrate_uc_workflow.yml` — **modify**: remove the `migrate_online_tables` task + drop it from `summary_uc` deps + remove `online_table` from `summary_uc` `object_types`.
- `resources/integration_tests/online_tables_integration_test_workflow.yml` — **new** test job.
- `tests/integration/{seed,teardown}_online_tables_test_data.py` + `tests/integration/test_online_tables.py` — **new**.
- `tests/unit/test_online_tables_worker.py` — **rewrite** for real migration; `tests/unit/test_pre_check_online_tables.py` — **new**.
- `docs/user_guide.md` + `docs/stateful_services_phase.md` — **update**.

## Worker logic (`online_tables_worker.py` rewrite)

Reads `online_table_list` from the orchestrator task value. Per row, parse
`metadata_json.definition` (the discovered GET response: `name`,
`spec.source_table_full_name`, sync mode flags, `primary_key_columns`,
`timeseries_key`, response-only `pipeline_id`). Then:

1. Reconstruct the create spec: `_build_online_table_spec(definition)` →
   `OnlineTableSpec.from_dict(spec_dict)` after `spec_dict.pop("pipeline_id", None)`.
2. `target_client.online_tables.create(OnlineTable(name=<fqn>, spec=<spec>))`.
3. `AlreadyExists` → `skipped_target_exists`; success → `created_resync_pending`;
   any other exception (incl. malformed metadata, missing definition) → `failed`.
4. Full per-row exception isolation — one bad row never aborts the batch (the VS
   lesson). `object_name` = the online table FQN; `object_type='online_table'`.
5. `tracker.append_migration_status(results)`. Module-level `logger` (sibling style).

No endpoint handling. No Direct-Access branch. The source table FQN is unchanged
across workspaces (UC name preserved), and must exist on target (pre-check
enforces).

## Pre-check (`pre_check_online_tables.py`)

For each `online_table` row from `get_pending_objects("online_table")`, read
`definition.spec.source_table_full_name` and probe `target_client.tables.get`.
Collect missing; write a `pre_check_results` row
(`check_name="online_table_source_tables"`, status PASS/FAIL) then **raise** on
any missing so the task fails and halts the job. Mirrors
`pre_check_vector_search.py` exactly (including the broad-except → warn-then-treat-as-absent
logging).

## Removing OT from `migrate_uc`

- Delete the `migrate_online_tables` task block from `migrate_uc_workflow.yml`.
- Remove `- task_key: migrate_online_tables` from `summary_uc`'s `depends_on`.
- Remove `online_table` from `summary_uc`'s `base_parameters.object_types`
  (leaving `managed_table,external_table,view,function,volume,mv,st,registered_model,grant`).

The shared orchestrator still lists `online_table` (harmless — only the new job's
worker consumes the list).

## Testing

- **Unit** — rewrite `tests/unit/test_online_tables_worker.py` from the
  hard-exclude assertions to real-migration assertions: spec round-trip drops
  `pipeline_id`; create called with reconstructed spec → `created_resync_pending`;
  `AlreadyExists` → `skipped_target_exists`; create failure → `failed`; malformed
  metadata → `failed` (no abort); `object_name` is the FQN. New
  `tests/unit/test_pre_check_online_tables.py` (missing source → FAIL/raise;
  present → PASS). **Flip any existing assertion** (unit or integration) that
  expected `online_table → skipped_by_stateful_service_migration`.
- **Integration (real, like VS, positive-only)** — `online_tables_integration_test`
  workflow: setup_test_config → seed (Triggered online table on source + same
  source Delta table with PK on target) → discovery → migrate_online_tables
  (run_job_task) → test → teardown(ALL_DONE). Seed + assertion emit
  `dbutils.notebook.exit(json)` so outcomes are retrievable via the Jobs API.
  Assertion: `migration_status == created_resync_pending` AND
  `target_client.online_tables.get(<fqn>)` succeeds. Infra-graceful: if the seed
  can't create an online table (preview unavailable), `has_online_table=false`
  and the assertion records `skipped_no_seed` (visible in the exit value).

## Live run

1. Build + unit-test + commit on a feature branch.
2. **Probe**: create a throwaway Triggered online table on the source; confirm it
   provisions. Delete it.
3. If creatable → deploy bundle, run `online_tables_integration_test` live, read
   the seed + assertion `notebook.exit` values for hard proof, confirm teardown.
4. If NOT creatable on the pair → report honestly; the test ships and runs later
   on an online-tables-enabled workspace. No false validation claim.

Deploy note: use `DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform
DATABRICKS_TF_VERSION=1.15.5` (CLI's own terraform download fails on an expired
HashiCorp PGP key).

## Docs

- `user_guide.md`: new `migrate_online_tables` section — recreate + re-sync
  (sync history lost), opt-in = running the job, precondition (run `migrate_uc`
  first; pre-check fails if source absent), statuses, run command.
- `stateful_services_phase.md`: update the Online Tables row's current-tool
  behaviour from the hard-exclude note to "Migrated by `migrate_online_tables` —
  recreate + re-sync from the target source table; sync state lost."

## Out of scope (YAGNI)

- Lakebase **synced tables** (`synced_table`, `capability='lakebase'`) — separate
  service / future job. This job is the legacy Online Table only.
- Waiting for the target online table to finish syncing / become ACTIVE.
- Continuous / Snapshot / timeseries-key live coverage (worker is mode-agnostic;
  add later if desired).
- Any change to `mv_st_worker` (MV/ST stay hard-excluded).

## File-touch summary (for the plan)

- `src/migrate/online_tables_worker.py` — rewrite (+ unit test rewrite).
- `src/pre_check/pre_check_online_tables.py` — new (+ unit test).
- `resources/production/migrate_online_tables_workflow.yml` — new.
- `resources/production/migrate_uc_workflow.yml` — remove OT task + summary refs.
- `resources/integration_tests/online_tables_integration_test_workflow.yml` — new.
- `tests/integration/seed_online_tables_test_data.py`, `test_online_tables.py`, `teardown_online_tables.py` — new.
- `docs/user_guide.md`, `docs/stateful_services_phase.md` — update.
