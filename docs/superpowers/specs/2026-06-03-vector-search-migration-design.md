# Vector Search Migration (`migrate_vector_search`) — design

**Date:** 2026-06-03
**Status:** approved (brainstorm complete) — implementation plan pending
**Scope:** Migrate **Delta Sync** Vector Search indexes from source to target as a
new standalone `migrate_vector_search` job. Consumes the `source_type='stateful'`,
`object_type='vector_search_index'` rows produced by the discovery extension
(PR #53). This is the first migration capability of the Stateful Services Phase.

## Background

The discovery extension (PR #53) enumerates Vector Search indexes into
`discovery_inventory` with their full spec under `metadata_json.definition`, but
nothing migrates them yet. This spec builds that migration.

Decision from brainstorm: the Stateful Services Phase is built as **one job per
stateful service** (not a single shared job). This spec delivers the **Vector
Search** job; Online Tables, Lakebase, Model Serving, Apps, and LFC each get
their own later job/spec.

## Decisions (brainstorm 2026-06-03)

| # | Decision |
|---|---|
| Q1 | One job per stateful service. This spec = `migrate_vector_search` only. |
| Q2 | Vector Search developed first; Online Tables is a separate later spec. |
| Q3 | **Pre-check gate**: fail the job up-front if any Delta Sync index's source Delta table is absent on target. |
| Q4 | **Distinct terminal status `created_resync_pending`** on successful create — no long wait for ONLINE. |
| Q5 | **Endpoint create-if-missing** (idempotent): worker ensures the target VS endpoint exists before creating the index. |
| Q6 | No config flag — deliberately running the (separate) job IS the opt-in. |
| Q7 | **Delta Sync indexes only.** Direct Vector Access indexes get a terminal skip; the limitation is documented in the user guide. |

## Architecture & components

New standalone job `migrate_vector_search`, task chain:

```
pre_check_vector_search → orchestrator → migrate_vector_search → summary_vector_search
```

The `orchestrator` task is retained (even for a single object type) for
uniformity with the existing four jobs: it runs `reconcile_stale_runs`
(resumability — resets orphaned `in_progress` rows from a crashed run) and
publishes the pending `vector_search_index` list as a task value the worker
consumes. This matches `migrate_governance`/`migrate_uc` exactly.

New files:
- `src/pre_check/pre_check_vector_search.py` — source-table existence gate
- `src/migrate/vector_search_worker.py` — index migration logic
- `resources/production/migrate_vector_search_workflow.yml` — the job
- `resources/integration_tests/vector_search_integration_test_workflow.yml` — test job
- `tests/unit/test_vector_search_worker.py`, `tests/unit/test_pre_check_vector_search.py`
- user-guide section + `stateful_services_phase.md` update

## Worker logic (`vector_search_worker.py`)

Notebook worker following the established shape (bootstrap cell → functions →
`run(dbutils, spark)` → notebook guard). Reads the pending list from the
orchestrator task value, parses each row's `metadata_json.definition` (the full
`get_index` spec: `index_type`, `endpoint_name`, `primary_key`,
`delta_sync_index_spec`). Per index:

1. **Direct Access index** (`index_type` is not `DELTA_SYNC`) → record terminal
   `skipped_direct_access_unsupported` with a message explaining vectors are
   external app-written state the tool cannot recreate. Continue.
2. **Delta Sync index:**
   a. **Ensure endpoint** — `target_client.vector_search_endpoints.get(name)`;
      if absent, `create_endpoint(name, endpoint_type)` mirroring the source
      endpoint, then a bounded wait for it to become usable. If the endpoint is
      still not ready when the wait elapses, the index is not created this run —
      record the non-terminal `skipped_endpoint_not_ready` so a later re-run
      retries it (see status table). Do NOT record `created_resync_pending`
      here, since no index was created.
   b. **Create index** — `target_client.vector_search_indexes.create_index(...)`
      with the `delta_sync_index_spec` pointing at the **same-named** source
      Delta table on target (UC preserves `catalog.schema.table` names across
      workspaces in this migration model), carrying over `primary_key`,
      embedding config, and pipeline type from the source spec.
   c. `AlreadyExists` → terminal `skipped_target_exists` (idempotent re-run).
   d. On accept → terminal `created_resync_pending` (optionally a short bounded
      poll to confirm the index did not immediately enter a FAILED state).
3. Accumulate results; `tracker.append_migration_status(results)`.

Per-index isolation: one index failing (record `failed` + error) never aborts
the others.

## New statuses

Added to `_TERMINAL_STATUSES` in `src/common/tracking.py`:
- `created_resync_pending` — index created on target; re-embedding/sync still in
  progress. The "done" signal (Q4); terminal so re-runs don't recreate.
- `skipped_direct_access_unsupported` — Direct Access index; vectors can't be
  recreated (Q7).

Non-terminal (re-pickable) status:
- `skipped_endpoint_not_ready` — target endpoint still provisioning when the
  worker gave up waiting; a later re-run retries the index. NOT added to
  `_TERMINAL_STATUSES`.

## Pre-check (`pre_check_vector_search.py`) — Q3 gate

For each **Delta Sync** `vector_search_index` row in `discovery_inventory`,
probe the target for its source Delta table via
`target_client.tables.get(source_fqn)`. Collect any missing. If any are missing,
write a `pre_check_results` row with result FAIL; the `orchestrator` reads the
latest pre-check result and raises, failing the job up-front (mirrors the
existing `check_target_collisions` gate consumed by `orchestrator.py`). Direct
Access rows are excluded from this check (they have no source table).

## Error handling

- Per-index isolation in the worker (one bad index → `failed`, continue).
- Endpoint-create failure for a given index → `failed` for that index with the
  endpoint error.
- VS not enabled / not permitted on target (surface-level SDK failure) → loud
  failure, consistent with the tool's fail-loud posture (not a silent skip).

## Testing

- **Unit** — `tests/unit/test_vector_search_worker.py`: delta-sync create path
  (asserts `create_index` called with the target source table + carried spec →
  `created_resync_pending`); endpoint create-if-missing (endpoint absent →
  `create_endpoint` called); endpoint-not-ready → `skipped_endpoint_not_ready`
  (re-pickable); Direct Access index → `skipped_direct_access_unsupported`;
  `AlreadyExists` → `skipped_target_exists`; create failure → `failed`.
  `tests/unit/test_pre_check_vector_search.py`: missing source → FAIL; present →
  PASS; Direct Access excluded from the source-table check. Mock
  `target_client.vector_search_endpoints` / `vector_search_indexes` / `tables`.
- **Integration** — a `vector_search_integration_test_workflow.yml`
  (seed endpoint + delta-sync index on source → discovery → pre_check →
  migrate → assert `created_resync_pending`). VS requires a preview/enabled
  workspace + real embedding, so live seeding is expensive: make the assertion
  **tolerant / preview-gated** (zero VS rows = skip/pass), same posture as the
  PR #53 stateful inventory assertion. Unit tests are the CI gate.

## Docs (ship with the code)

- New `migrate_vector_search` section in `user_guide.md` covering: what it does,
  the run-the-job-is-opt-in model, the `created_resync_pending` semantics, and
  the **Known limitations** below.
- Update `stateful_services_phase.md`: Vector Search now has a migration job.

## Known limitations / deferred (revisit later)

Captured explicitly so they can be picked up in a future iteration:

1. **Direct Vector Access indexes are NOT migrated.** They are recorded
   `skipped_direct_access_unsupported`. Their vectors are written directly by the
   customer's application (external state), so recreating the index would yield
   an empty index. A future iteration could migrate the index *definition* and
   provide an operator runbook for re-population, if there is demand.
2. **Custom embedding-model serving endpoints are NOT checked or migrated.**
   Delta Sync indexes using managed embeddings backed by a *custom* model serving
   endpoint depend on that endpoint existing on target. This spec does NOT verify
   it in the pre-check and does NOT migrate it — it's treated as a documented
   operator precondition. Databricks-hosted embedding models (e.g.
   `databricks-gte-large-en`) are available on target by default and are
   unaffected. A future iteration could add an embedding-endpoint existence
   check to the pre-check once Model Serving migration exists.

## Out of scope (YAGNI)

- Online Tables and all other stateful services (separate jobs/specs).
- Direct Access migration; embedding-endpoint migration or pre-check.
- Waiting for the index to reach ONLINE/READY.
- Dependency-graph / topo-sort work.

## File-touch summary (for the implementation plan)

- `src/migrate/vector_search_worker.py` — **new**
- `src/pre_check/pre_check_vector_search.py` — **new**
- `src/common/tracking.py` — add `created_resync_pending` +
  `skipped_direct_access_unsupported` to `_TERMINAL_STATUSES`
- `resources/production/migrate_vector_search_workflow.yml` — **new**
- `resources/integration_tests/vector_search_integration_test_workflow.yml` — **new**
- `tests/unit/test_vector_search_worker.py`, `tests/unit/test_pre_check_vector_search.py` — **new**
- `docs/user_guide.md` — add `migrate_vector_search` section + Known limitations
- `docs/stateful_services_phase.md` — note VS now has a migration job
