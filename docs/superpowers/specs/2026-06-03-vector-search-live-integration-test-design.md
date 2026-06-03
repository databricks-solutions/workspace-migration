# Vector Search live integration test — design

**Date:** 2026-06-03
**Status:** approved (brainstorm complete) — implementation plan pending
**Branch:** `feat/migrate-vector-search` (extends PR #54, still open)
**Scope:** Turn the `vector_search_integration_test` workflow into a REAL end-to-end
test on the live source/target workspace pair: seed a Delta Sync index (positive
case) and a Direct Access index (negative case) on the source, run discovery +
`migrate_vector_search`, and assert against the **target** that the Delta Sync
index was created and the Direct Access index was not. Unit tests remain
mock-based (unchanged).

## Why

PR #54's `migrate_vector_search` was validated only by mock-based unit tests + a
tolerant integration stub (zero rows = pass). That does not prove the code works
against real Vector Search — the SDK round-trip (`get_index().as_dict()` →
`DeltaSyncVectorIndexSpecRequest.from_dict` → `create_index`), real endpoint
provisioning, and the actual skip of Direct Access indexes were unverified. This
spec closes that gap with a real run.

## Confirmed live infra (probed 2026-06-03)

- Source `adb-7405609086312103.3` and target `adb-7405615515664170.10` both
  reachable (profiles `source-migration` / `target-migration`).
- Vector Search enabled on both (endpoint list returns `[]`, no permission error).
- `databricks-gte-large-en` embedding endpoint READY on both → managed-embeddings
  Delta Sync index works with no custom endpoint.
- No VS endpoints exist on either workspace yet → genuine cold-start.

## Decisions (brainstorm 2026-06-03)

| # | Decision |
|---|---|
| Cases | Positive = Delta Sync index (must be migrated); Negative = Direct Access index (must be skipped). Both seeded on source. |
| Target assertion | Resource-exists check via `target_client.vector_search_indexes.get_index`, not a wait-for-ONLINE. Positive: exists + `created_resync_pending`. Negative: NotFound + `skipped_direct_access_unsupported`. |
| Cold-start | Nothing pre-created on target — the worker creates the target endpoint + index from scratch. |
| Endpoint wait | NO config knob. Bump `_ensure_endpoint`'s default budget to a realistic cold-provision bound (~30 min) — correct for production too. |
| Infra-graceful | If the seed cannot create VS objects (VS unavailable), it sets `has_*` flags false and the assertions for that case are skipped (mirrors the UC `has_<type>` pattern). On this VS-enabled pair both run for real. |
| Teardown | Best-effort, `run_if ALL_DONE`: delete both indexes + the VS endpoint on source AND target (endpoints cost money), drop the source test catalog, delete tracking rows. |
| Execution | Build + run live now against the confirmed-present pair; report the `test_vector_search` task outcome; teardown auto-runs. |

## Workflow

`resources/integration_tests/vector_search_integration_test_workflow.yml` task graph:
```
setup_test_config → seed_vector_search → discovery → migrate_vector_search (run_job_task) → test_vector_search → teardown_vector_search (run_if ALL_DONE)
```
- `setup_test_config` — reuse the existing `tests/integration/setup_test_config.py` to point config at the live pair / tracking catalog (same as the UC integration workflow).
- `migrate_vector_search` invokes the production job (`${resources.jobs.migrate_vector_search.id}`), which internally runs its own `pre_check_vector_search → orchestrator → vector_search_worker → summary`.
- Mirrors `uc_integration_test_workflow.yml`'s structure exactly (run_as SPN, run_job_task, `teardown` ALL_DONE).

## Components

### Worker change (the only production-code change)
`src/migrate/vector_search_worker.py` — `_ensure_endpoint` default budget raised
from `max_attempts=30, sleep_seconds=10` (5 min) to a realistic cold-provision
bound (`max_attempts=120, sleep_seconds=15` ≈ 30 min). The create-if-missing
logic is unchanged; only the default poll budget changes. The existing
`skipped_endpoint_not_ready` re-pickable path remains as the safety valve if an
endpoint exceeds even the larger bound. One unit test updated to pin the new
default budget.

### Seed — `tests/integration/seed_vector_search_test_data.py`
Ambient `WorkspaceClient()` (like `seed_uc_test_data.py`). On **source** only:
1. Create test catalog/schema; create a Delta table with
   `TBLPROPERTIES (delta.enableChangeDataFeed = true)`, a primary-key column, and
   a text column; insert a few rows. (CDF is required for Delta Sync indexes.)
2. Create a source VS endpoint (`cp_migration_vs_it`); wait for ONLINE.
3. Create a **Delta Sync** index (managed embeddings, `databricks-gte-large-en`)
   on that endpoint over the source table. Set `has_delta_index=true`.
4. Create a **Direct Access** index on the same endpoint (with a minimal
   `schema_json` / embedding dimension). Set `has_direct_index=true`.
5. On any failure, set the corresponding flag false and print the reason
   (infra-graceful). Publish the index FQNs as task values for the assertion.

Nothing is created on the target.

### Assertion — `tests/integration/test_vector_search.py` (replaces the tolerant stub)
Notebook using `AuthManager(config, dbutils).target_client`. Reads the seed flags
+ index FQNs from `seed_vector_search` task values. Accumulates into
`error_messages` and raises at the end (the established notebook pattern).
- **Positive (if `has_delta_index`):** assert `migration_status` latest row for the
  Delta Sync index has `status='created_resync_pending'`; assert
  `target_client.vector_search_indexes.get_index(<delta_fqn>)` succeeds.
- **Negative (if `has_direct_index`):** assert `migration_status` row has
  `status='skipped_direct_access_unsupported'`; assert
  `get_index(<direct_fqn>)` raises NotFound (does not exist on target).
- If a flag is false, print a skip line for that case (no failure).

### Teardown — `tests/integration/teardown_vector_search.py`
`run_if ALL_DONE`, every step `try/except` (best-effort), mirroring
`teardown_uc.py`:
- Delete the Delta Sync + Direct Access indexes on **source** and **target**.
- Delete the VS endpoint on **source** and **target** (must clean — paid resource).
- Drop the source test catalog (CASCADE).
- Delete `migration_status` / `discovery_inventory` rows for the two index FQNs.

## Live run

1. Confirm both workspaces reachable (done: both return `current-user me`).
2. `databricks bundle validate -t dev --profile source-migration`, then
   `databricks bundle deploy -t dev --profile source-migration` with the
   migration SPN var (per the session-recovery deploy command).
3. Trigger the `vector_search_integration_test` job; poll to completion (~15–25 min:
   source endpoint provision + index create during seed, then target cold-start
   during migrate).
4. Report the `test_vector_search` task result. Teardown runs automatically.

## Out of scope (YAGNI)

- Waiting for the migrated index to reach ONLINE / running a similarity query.
- Custom embedding-model serving endpoints.
- Any change to unit tests beyond the one `_ensure_endpoint` default-budget pin.
- Config knobs.

## File-touch summary (for the plan)

- `src/migrate/vector_search_worker.py` — raise `_ensure_endpoint` default budget; update the one unit test that pins it.
- `tests/integration/seed_vector_search_test_data.py` — **new**.
- `tests/integration/test_vector_search.py` — **replace** tolerant stub with real positive+negative assertions.
- `tests/integration/teardown_vector_search.py` — **new**.
- `resources/integration_tests/vector_search_integration_test_workflow.yml` — add `setup_test_config`, `seed_vector_search`, `teardown_vector_search` (ALL_DONE) tasks around the existing discovery → migrate → test chain.
