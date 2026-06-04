# Integration-Test Coverage Expansion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or executing-plans. Steps use checkbox (`- [ ]`).

**Goal:** Make the integration suite exercise **every in-scope object type** — seed → migrate → assert, with **no silent skips** — so the class of bug found in the 2026-06-04 review (green runs hiding real defects) cannot recur.

**Why:** See [[reference_wsm_test_false_confidence]]. Root causes: (1) unwired suites, (2) conditional assertions that silently skip when a fixture isn't seeded, (3) a broken validation gate (fixed in PR #2), (4) test env masking fail-open paths. This plan closes (1) and (2) at the assertion/seed level. Follows the data-safety fixes merged in PR #2 + #3.

**Tech Stack:** Notebook-based integration tests (`tests/integration/`), DAB workflows (`resources/integration_tests/`), live Azure lab workspace pair (source `adb-7405609086312103.3`, target `adb-7405615515664170.10`), `uv` for the notebook-shape lint.

---

## Guiding policy (locked)

1. **A not-seeded MANDATORY in-scope fixture FAILS the run**, never silently skips. Replace `else: print("… skipping assertion")` with `error_messages.append("MANDATORY fixture X not seeded — in-scope type must be covered")`.
2. **Env-blocked types keep a soft-skip, but it must be a NAMED, TRACKED skip** surfaced in the run summary (not a bare `print`). Only these are allowed to soft-skip: ABAC `policy` (runtime may reject `SET ABAC POLICY`), and `connection`/`foreign_catalog` IF the SQL Server can't be reached.
3. **Every new fixture is unconditionally seeded** where the lab supports it (external locations, Lakebase, SQL Server all exist or can be stood up).

## Coverage gaps to close (from the 2026-06-04 matrix)

**P0 — no seed, no assertion:** monitor, connection, foreign_catalog, provider.
**P1 — seed exists, assertion absent/disabled:** share + recipient (F.1 intermittent), grants (only schema-grant, gated).
**P1 — asserted but silently skippable:** row_filter/column_mask reapply (T29/T30), registered_model, vector_search, online_table, policy(ABAC).
**P1 — worker's only coverage is a gated leg:** hive_external, hive_managed_nondbfs, hive_grant.

---

## Phase A — De-conditionalize silent skips (authorable now; validate live)

### Task A.1: Hive mandatory fixtures fail-if-not-seeded
**Files:** `tests/integration/test_hive_end_to_end.py`, `tests/integration/seed_hive_test_data.py`
- [ ] Make `hive_external`, `hive_managed_nondbfs`, `hive_grant` seeds unconditional (they need only a valid external location, which the lab has). Flip each assertion's `else: print(skipping)` → `error_messages.append(...)`.
- [ ] Notebook-shape lint: `uv run pytest tests/lint/test_notebook_shape.py -q`. Commit.

### Task A.2: UC mandatory fixtures fail-if-not-seeded
**Files:** `tests/integration/test_uc_end_to_end.py`, `seed_uc_test_data.py`
- [ ] Schema grant, external_table partitioned, volume nested, column/volume comments → unconditional seed + fail-if-missing assertion.
- [ ] Lint. Commit.

### Task A.3: Named tracked skips for env-blocked types
**Files:** the assertion notebooks + a small `_tracked_skip(name, reason)` helper
- [ ] Replace bare-print skips for ABAC/connection/foreign_catalog with a helper that appends a `{skipped: X, reason: …}` entry to a `skips` list echoed in `dbutils.notebook.exit(...)`, so the run output names what was skipped. Lint. Commit.

## Phase B — New seeds + assertions for untested types

### Task B.1: monitor
- [ ] Seed a Lakehouse monitor on a managed table in `seed_uc_test_data.py`; assert `monitors_worker` migrates it (named-skip if monitor preview unavailable). Commit.

### Task B.2: provider
- [ ] Seed/assert an inbound provider if feasible; else named-skip with rationale. Commit.

### Task B.3: share + recipient (stabilize F.1)
- [ ] Make the customer-share assertion unconditional + add a dedicated recipient assertion; apply the PR #33 SPN-owner fix consistently. Commit.

### Task B.4: row_filter + column_mask reapply (T29/T30)
- [ ] Add a leg running `migrate_uc` → `migrate_governance` back-to-back; assert the filter/mask is present + enforced on the target. Commit.

## Phase C — SQL Server for connection / foreign_catalog (LIVE infra, DBU)

### Task C.1: Re-stand-up the Azure SQL Server
- [ ] `cd ~/uksouth_migration/infra/azure-sql-test && terraform apply` (per [[project_uc_migration_backlog]] item 12 — server `sqlsrv-wsm-test-ne` + PE + NCC). Re-attach NCC to source workspace if needed.
- [ ] Seed a UC `connection` to it + a `foreign_catalog`; assert both migrate (named-skip if unreachable). Commit.

## Phase D — Retry/collision DAB workflows (#11 follow-up)

### Task D.1: collision_handling workflow
- [ ] Author `resources/integration_tests/collision_handling_integration_test_workflow.yml`: seed rogue target → pre_check (expect FAIL/skip) → assert via `test_collision_handling.py`. Wire into `integration.yml`. Commit.

### Task D.2: retry_resumability workflow
- [ ] Author the two-pass chain: seed → migrate (`test_kill_after: 2`) → migrate (resume) → assert via `test_retry_resumability.py`. Wire into `integration.yml`. Commit.

## Phase E — Live validation (DBU — confirm with user first)

- [ ] Deploy to lab pair (`DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform DATABRICKS_TF_VERSION=1.15.5 BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae databricks bundle deploy -t dev --profile source-migration`).
- [ ] Run every expanded suite live; confirm no silent skips, all mandatory assertions exercised. This also serves as the **live retest of the PR #2/#3 fixes**.
- [ ] Capture results; tear down SQL Server.

## Out of scope (separate)
- Full-`src/` mypy (108 pre-existing errors) — #11 follow-up.
- Git-history re-scrub for config values — #12 release-gate.
