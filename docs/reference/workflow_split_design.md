# Workflow Split Design

**Status**: Approved design, pending implementation
**Author**: hari.selvarajan@databricks.com
**Date**: 2026-05-06
**Prerequisite**: Path A staging_copy rewrite (must land first)

## Goals

Split the monolithic `migrate_workflow.yml` into independent, standalone-runnable jobs so operators can:

- Run UC migration on its own cadence (e.g., one-shot bulk migration)
- Run Hive migration on a different cadence (e.g., decommissioning legacy DBs)
- Run governance migration standalone — *after* data migration — without re-running the data plane
- Re-run governance migration alone when policies change in the source workspace

## Non-goals

- One-button "migrate everything" — operators chain via runbook / shell script if they want it. Not our layer.
- Cross-workflow state aggregation — each workflow reports its own slice of `migration_status`.
- Partial / incremental discovery — discovery is one upstream scan, no per-workflow filtered scans.

## Final topology

```
discovery (job)                              [shared upstream]
   ├─ scan source workspace
   ├─ write discovery_inventory
   └─ summary (counts by object_type)

migrate_uc (job)                             [depends_on: discovery]
   ├─ pre_check
   ├─ setup_sharing                          ← Path A: creates staging copies
   ├─ migrate_managed_tables
   ├─ migrate_external_tables
   ├─ migrate_views
   ├─ migrate_volumes
   ├─ migrate_models
   ├─ migrate_grants                         ← UC ACL replay (D1=b)
   ├─ migrate_pipelines (skip)
   ├─ migrate_streaming_tables (skip)
   ├─ migrate_mv_st (skip — Phase 4)
   ├─ migrate_online_tables (skip — Phase 4)
   ├─ cleanup_staging                        ← Path A: drops staging tables
   └─ summary_uc

migrate_hive (job)                           [depends_on: discovery]
   ├─ pre_check_hive
   ├─ migrate_hive_databases
   ├─ migrate_hive_tables
   ├─ migrate_hive_views
   ├─ migrate_hive_functions
   ├─ migrate_hive_grants                    ← Hive ACL replay (D1=b)
   └─ summary_hive

migrate_governance (job)                     [depends_on: discovery]
   ├─ pre_check_governance                   ← documents trust-the-operator contract
   ├─ migrate_tags
   ├─ migrate_comments
   ├─ migrate_row_filters
   ├─ migrate_column_masks
   ├─ migrate_customer_shares                ← D2=a
   └─ summary_governance
```

Each `migrate_*` job depends on `discovery` via DAB cross-job dependency. No `migrate_all` umbrella.

## Decisions

### Q1 — Standalone `migrate_governance` contract

When `migrate_governance` runs and target tables don't exist, **trust the operator**. No pre-check, no soft-skip. Operator is responsible for ordering. Documented in:
- `README.md` — "Pre-conditions for standalone runs" section
- `migrate_governance_workflow.yml` — task and job descriptions
- `pre_check_governance` notebook — informational header only

### Q2 — Discovery scope

Single shared `discovery` job populates `discovery_inventory`. UC / Hive / Governance workflows each declare DAB cross-job dependency on it. Trade-off: stale-snapshot risk if workflows run on different cadences against the same inventory — documented.

### Q3 — `restore_rls_cm` placement

**N/A.** Path A staging_copy rewrite eliminates source mutation entirely. Source RLS/CM is never stripped, so no restore is needed. `restore_rls_cm.py` is renamed to `cleanup_staging.py` (drops staging tables only). This is a Path A deliverable, not a workflow-split deliverable.

### Q4 — Summary task placement

Per-workflow inline summary inside each job: `summary_uc`, `summary_hive`, `summary_governance`. No global summary. Operators query `migration_status` directly for cross-workflow aggregation.

### Q5 — Config reshape

`scope.include_uc` and `scope.include_hive` flags are **removed entirely**. Workflow choice is the only scope gate. ~30 LOC of internal short-circuit branches in workers are deleted. `X.3.3` negative-paths test is repurposed: was "both flags false → no-op"; becomes "empty discovery → no-op" (the underlying behaviour assertion still applies, just driven by zero discovered objects rather than scope flags).

### Q6 — Workflow naming

`migrate_uc` / `migrate_hive` / `migrate_governance`. Concise data names; "governance" matches existing Phase 3 worker terminology.

### Q7 — Test workflows

Three test workflows mirror production:
- `uc_integration_test_workflow.yml` (existing, scoped down to UC only)
- `hive_integration_test_workflow.yml` (existing)
- `governance_integration_test_workflow.yml` (new)

Plus unified `negative_paths_integration_test_workflow.yml`. Phase 3 governance assertions (3.15, 3.17, 3.19, 3.21, 3.22, 3.24) move from `uc_integration_test` into the new governance test.

### Q8 — Rollout

Hard cutover, two PRs:
1. **Path A staging_copy** PR (prerequisite)
2. **Workflow split** PR — drops `migrate_workflow.yml`, adds 4 production workflows + 3 test workflows + scope-flag removal + worker simplifications, in one shot

Internal tool with small audience; deprecation period not warranted.

### BS-3 — Top-level orchestration

Hard split: 4 independent jobs (`discovery` + 3 `migrate_*`). No `migrate_all` umbrella. Operators chain via runbook / shell script for one-button.

### BS-4 sub-decisions

- **D1 = b** — `migrate_grants` (UC) stays in `migrate_uc`; `migrate_hive_grants` stays in `migrate_hive`. `migrate_governance` is purely fine-grained governance.
- **D2 = a** — Customer-defined shares live in `migrate_governance` (metadata, not data).
- **D3 = b** — Governance integration test pre-seeds target via direct SQL setup task (no UC migration dependency).
- **D4 = a** — Discovery summary reports discovery counts only, not `migration_status` state.
- **D5 = b** — `resources/production/` (4 files) + `resources/integration_tests/` (4 files).

## File-by-file change inventory

### Added

- `resources/production/discovery_workflow.yml` — discovery job
- `resources/production/migrate_uc_workflow.yml` — UC job
- `resources/production/migrate_hive_workflow.yml` — Hive job
- `resources/production/migrate_governance_workflow.yml` — governance job
- `resources/integration_tests/governance_integration_test_workflow.yml` — new test workflow
- `src/migrate/pre_check_governance.py` — informational, no enforcement
- `tests/integration/seed_governance_target_state.py` — pre-seed target tables for governance standalone test (D3)

### Renamed / moved

- `resources/migrate_workflow.yml` → **deleted**
- `resources/uc_integration_test_workflow.yml` → `resources/integration_tests/uc_integration_test_workflow.yml`
- `resources/hive_integration_test_workflow.yml` → `resources/integration_tests/hive_integration_test_workflow.yml`
- `resources/negative_paths_integration_test_workflow.yml` → `resources/integration_tests/negative_paths_integration_test_workflow.yml`

### Modified

- `src/common/config.py` — remove `scope.include_uc` / `scope.include_hive` fields
- `config.yaml` and `config.example.yaml` — remove `scope` block
- `src/migrate/orchestrator.py` — remove scope-gate short-circuits; per-workflow summary registration
- `src/migrate/grants_worker.py` — remove `if not config.include_uc` branch
- `src/migrate/hive_grants_worker.py` — remove `if not config.include_hive` branch
- `src/migrate/hive_orchestrator.py` — remove scope-gate short-circuit
- `src/migrate/setup_sharing.py` — remove scope checks
- `src/discovery/discovery.py` — remove scope-based filtering (full scan always)
- `tests/unit/test_hive_orchestrator.py` — drop scope-gate tests
- `tests/integration/test_uc_end_to_end.py` — split: UC slice stays, governance slice moves
- `tests/integration/test_governance_end_to_end.py` — new file with relocated assertions
- `README.md` — document 4-job operator flow + standalone-runnable contract + trust-the-operator pre-conditions

### Deleted

- `resources/migrate_workflow.yml`
- All `if not config.include_uc:` / `if not config.include_hive:` short-circuit branches across workers (~30 LOC)

### Repurposed

- X.3.3 negative-paths case: was "both scope flags false → no-op"; now "empty discovery → no-op" (asserts the same end state via a different driver)

## Test strategy

- **Unit tests**: drop scope-gate tests, add per-workflow scope assertions
- **UC integration test**: data plane + UC grants only; governance assertions removed
- **Hive integration test**: data plane + Hive grants only (unchanged in spirit)
- **Governance integration test**: pre-seeds target via direct SQL (catalog/schema/tables/columns/views), then runs `migrate_governance` workflow only; asserts tags, comments, RLS, CM, customer shares
- **Negative paths**: stays unified, exercises error paths across all three; X.3.3 repurposed (empty-discovery no-op rather than both-flags-false no-op)

## Migration path

1. **Path A PR** (prerequisite) — staging_copy rewrite per existing backlog design (pre-validated 2026-04-24)
2. **Workflow split PR** — this design

After PR2 merges, operators redeploy with `databricks bundle deploy`. Old `migrate` job disappears; new 4 jobs appear.

## Documentation deltas

`README.md` gets:
- Quickstart updated for 4-job operator flow
- "Standalone-runnable workflows" section explaining what each workflow does
- "Pre-conditions for `migrate_governance`" subsection with the trust-the-operator contract
- Note that `migrate_*` jobs depend on `discovery` completing first
- Removed all references to `scope.include_uc` / `scope.include_hive`

## Resolved review findings

The following review findings from `2026-04-27` are resolved by Path A or this split:

| ID | Resolved by | Note |
|---|---|---|
| C2 | Path A | No restore loop, no `"already"` substring matching |
| C3 | Path A | No CTAS branch — DEEP CLONE always works (source intact) |
| C4 | Path A | Schema preservation via DEEP CLONE |
| H1 | Path A | No restore concurrency race |
| H2 | Path A | No `record_rls_cm_strip` SQL injection |
| H3 | Path A | No `restore_rls_cm` recovery problem (task gone) |
| H4 | Path A | No `ALTER SHARE … REMOVE TABLE` consumer-visibility window |
| H9 | Path A | No CTAS path → no `validated_via_ctas` signal needed |

Remaining review items (C1, C5, C6, H5, H6, H7, H8, H10, H11) are independent of the split and are tracked as separate fixes.

## Open follow-ups (out of scope for this PR)

- **Phase 4** — hard-exclude MV + Online Tables (mirror PR #41 ST pattern). 8-file delta, ~150-200 LOC, 1 hour, single PR.
- **C1 NameError fix in `tracking.py:445,449`** — independent CRITICAL fix
- **C5 model rollback parity with volumes** — independent CRITICAL fix
- **C6 governance worker key alignment** — independent CRITICAL fix
- **X.6 Load testing** — 50+ tables/schema smoke
- **Azure SQL test infra teardown finish** — needs whitelisted-network access
- **Path B** — operator-runbook one-button shell script if demand arises
