# Code-Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve all 13 findings from the 2026-06-04 external code review of `workspace-migration`, AND close the integration-test coverage gaps that let these bugs ship green — so every in-scope object type is seeded, migrated, and asserted unconditionally.

**Architecture:** Three sequenced phases on branch `fix/code-review-2026-06-04`. Phase 1 fixes data-safety/correctness bugs **and repairs the broken validation gate first** (so later phases are validated by tests that actually work). Phase 2 adds missing migration coverage (ownership, collision) and expands the integration suite to exercise every object type with no silent skips. Phase 3 fixes CI wiring, static/coverage gating, and config/security posture.

**Tech Stack:** Python 3 + Databricks SDK + PySpark, DABs (`databricks.yml` + `resources/`), `uv` for tests (`uv run pytest tests/unit/`), pytest unit + notebook-based integration tests on the live lab workspace pair.

---

## Status

- **Phase 1 — ✅ COMPLETE** (8/13: #1,#2,#3,#4,#7,#8,#9,#10). PR #2. 878 tests.
- **Phase 2 code — ✅ COMPLETE** (#5 ownership, #6 collision probes + self-enforcing guard test). 891 tests.
- **Phase 3 — ✅ COMPLETE** (#11 CI wiring + coverage→src/, #12 config untrack + gitignore, #13 admin-group var). 891 tests.
- **All 13 findings fixed at code/config level.** Remaining work is the **integration-test expansion** (Tasks 2.3/2.4) — needs live workspaces + SQL Server standup + DBU. Plus two tracked follow-ups: retry/collision DAB workflows (#11), full-`src/` mypy (108 pre-existing errors, #11), git-history re-scrub (#12).
- **Not yet live-integration-tested.**

---

## Design Decisions (locked — flagged for user veto before execution)

| # | Decision | Rationale |
|---|----------|-----------|
| **2** | Add existence-gate to `managed_table_worker`: **skip** target if it exists AND latest status is `validated`; add opt-in `overwrite_existing: bool = False` config flag. Closes the orphan-replay window (reset-to-pending row whose target already exists & validated → skip, don't re-REPLACE). | Safe default: never clobber a good target on resume. Flag preserves the deliberate-overwrite path. |
| **3** | Iceberg branch: rewrite CREATE → `CREATE TABLE IF NOT EXISTS`; change `INSERT INTO` → `INSERT OVERWRITE`. | Makes both steps idempotent — retry can't double rows. |
| **4** | DBFS-root Hive: read source partition columns + `TBLPROPERTIES`, apply `.partitionBy(*cols)` + properties on write; **re-read target** for real `target_row_count` instead of hard-coding `= source_row_count`. | Stops silent partition/property loss AND stops the validator from rubber-stamping it. |
| **5** | Handle ownership **inside the existing grants workers** (`grants_worker.py` / `hive_grants_worker.py`) — replace the `OWN` skip with `ALTER <securable> OWNER TO <original_owner>`, applied **after** all non-OWN grants for that securable (so the SPN keeps MANAGE while granting). Config flag `transfer_ownership: bool = True`. Per-row **fail-loud** (`status='failed'`, clear message) if the original principal doesn't exist on target — never crash the batch, never silently skip. No new worker/orchestrator task. | Ownership is a permission concern and `OWN` already flows through the grants workers; co-locating keeps ACL logic in one place. Applying OWN last avoids the SPN losing grant ability mid-batch. |
| **6** | (a) Add collision probes for global-namespace types feasible via SDK: `connection`, `share`, `recipient`, `registered_model`. (b) For any in-scope type still lacking a probe, **surface an explicit "NOT collision-checked: [...]" line** in `pre_check_results`. | Removes the silent blind spot; operator sees exactly what wasn't checked. |
| **10** | Collision probes: map only `NotFound`/`RESOURCE_DOES_NOT_EXIST`/404 → `False`. Any other exception (esp. `PermissionDenied`) → re-raise as a distinct **check failure** (collision check FAILS, migration does not proceed). Fix `_fqn_to_parts` dotted-name parse. | Converts fail-open → fail-closed; a scoped SPN can no longer mask a pre-existing target. |
| **13** | Replace `databricks.yml` `group_name: users` `CAN_MANAGE` with a bundle variable `migration_admin_group` (default **`admins`** — built-in workspace admins, always exists, zero escalation since admins already manage all jobs). README documents tightening to a dedicated group (e.g. `migration_admins`). | Least-privilege default that can't break deploy; mirrors "old-workspace administrators" management model; operators override to a dedicated group if desired. Data-object permissions are preserved separately via grants migration + ownership transfer (#5). |
| **12** | `git rm --cached config.yaml`; add `config.yaml` to `.gitignore`; overwrite the working copy's real values with placeholders; ship `config.example.yaml`; README says copy example→config at deploy. **Flag:** real SPN client-ID + workspace URLs are already in git history → note that a history re-scrub is required before any public release (tracked, not done in this PR). | Stops future secret leakage; history scrub is a separate release-gate task. |
| **Test policy** | A not-seeded **in-scope mandatory** fixture must **FAIL** the run, not skip. Conditional skips remain ONLY for genuinely environment-blocked types, and those must emit a tracked `pre_check_results`/summary row naming the skipped type (no silent pass). | Directly fixes the false-confidence root cause. |
| **Connection/Foreign-catalog seed** | **Re-stand-up the Azure SQL Server** (as before — the `~/uksouth_migration/infra/azure-sql-test` terraform that was torn down per backlog item 12/13) as the external federation source for the `connection` + `foreign_catalog` seed. | Tests federation exactly as it runs in production (matches the original 3.21/3.22 design), rather than substituting a different engine. |

---

## Phase 1 — Correctness fixes + validation-gate repair

> Highest leverage. Fix the gate (#8) first so the bug fixes are provable. Each fix is unit-tested; data-path fixes also get an integration assertion in Phase 2.

### Task 1.1: Repair `validate_schema_match` (#8)
**Files:** Modify `src/common/validation.py`; Test `tests/unit/test_validation.py`
- [ ] Write failing tests: (a) identical schema with differing `comment` column → must PASS (no mismatch); (b) partitioned table whose DESCRIBE output includes `# Partition Information`/blank/`# col_name` rows → must PASS when columns+types match; (c) genuinely divergent `data_type` → must FAIL.
- [ ] Run: `uv run pytest tests/unit/test_validation.py -v` → expect FAIL.
- [ ] Implement: build the column map from DESCRIBE rows filtered to **real** rows — drop rows where `col_name` is blank/None, starts with `#`, or is a known metadata key (`# Partition Information`, `# col_name`, `# Detailed Table Information`, etc.); compare only the `(col_name, data_type)` tuple, never the full dict (excludes `comment`). Stop at the first metadata separator row.
- [ ] Run tests → expect PASS. Commit.

### Task 1.2: Hive non-DBFS `KeyError` (#1)
**Files:** Modify `src/migrate/hive_managed_nondbfs_worker.py` (`record["fqn"]`→`record["object_name"]` at ~:96; `rec_info["fqn"]`→`rec_info["object_name"]` at ~:264); Test `tests/unit/test_hive_managed_nondbfs_worker.py`
- [ ] Write failing test: drive the per-record path with an orchestrator-shaped record (`object_name` key, no `fqn`) → assert it does NOT raise `KeyError` and produces a status row keyed by `object_name`. Add a second test that the failure-handler path records `object_name` (not KeyError) on a forced DDL error.
- [ ] Run → FAIL (KeyError). Implement key rename. Run → PASS. Commit.

### Task 1.3: Managed Delta overwrite-on-resume gate (#2)
**Files:** Modify `src/migrate/managed_table_worker.py`; `src/common/config.py` (+`overwrite_existing: bool = False`); Test `tests/unit/test_managed_table_worker.py`
- [ ] Write failing tests: (a) target exists + latest status `validated` + `overwrite_existing=False` → worker SKIPS (status `skipped_target_exists`/`already_validated`), no CREATE OR REPLACE issued; (b) `overwrite_existing=True` → CREATE OR REPLACE issued; (c) target absent → normal clone.
- [ ] Run → FAIL. Implement: before the CREATE OR REPLACE (both the staging-consumer and direct branches), check target existence + terminal-validated status; gate on `config.overwrite_existing`. Run → PASS. Commit.

### Task 1.4: Iceberg idempotency (#3)
**Files:** Modify `src/migrate/managed_table_worker.py` (iceberg branch ~:179/:194/:205); Test `tests/unit/test_managed_table_worker.py`
- [ ] Write failing test: simulate CREATE-succeeded-then-retry on iceberg branch → assert no second full append (uses `INSERT OVERWRITE`) and CREATE uses `IF NOT EXISTS`.
- [ ] Run → FAIL. Implement: rewrite create DDL to `CREATE TABLE IF NOT EXISTS` (reuse the `rewrite_ddl` helper the hive worker uses); change `INSERT INTO` → `INSERT OVERWRITE`. Run → PASS. Commit.

### Task 1.5: DBFS-root partitioning + properties + real validation (#4)
**Files:** Modify `src/migrate/hive_managed_dbfs_worker.py`; Test `tests/unit/test_hive_managed_dbfs_worker.py`
- [ ] Write failing tests: (a) partitioned source → write call includes `partitionBy(<cols>)`; (b) `target_row_count` is derived by re-reading the target, not copied from source.
- [ ] Run → FAIL. Implement: read partition columns (`DESCRIBE DETAIL`/`SHOW PARTITIONS`/catalog metadata) + `TBLPROPERTIES`, pass `.partitionBy(*cols)` and `.options(**props)` on the Delta write; after registration, re-read target for `target_row_count`. Run → PASS. Commit.

### Task 1.6: Thread real `job_run_id` onto status rows (#7)
**Files:** Modify `src/migrate/orchestrator.py` + `src/migrate/hive_orchestrator.py` (pass `job_run_id` into `migrate_*` worker fns), all `src/migrate/*_worker.py` in_progress writes (`"job_run_id": None`→threaded value), `src/common/tracking.py` write helper signature if needed; Test `tests/unit/test_reconciliation.py` + affected worker tests
- [ ] Write failing test: a worker in_progress row written during the current run carries the real `current_job_run_id`; reconciliation's "only reset prior runs" guard now correctly SKIPS a current-run in_progress row.
- [ ] Run → FAIL. Implement: thread `job_run_id` (orchestrator already resolves `_job_run_id`) through each worker into `append_migration_status`. Update all worker callsites + their unit tests. Run full unit suite → PASS. Commit.

### Task 1.7: Gate reconciliation cleanup hooks on `dry_run` (#9)
**Files:** Modify `src/migrate/reconciliation.py` (`_run_cleanup` / call site ~:269); Test `tests/unit/test_reconciliation.py`
- [ ] Write failing test: `config.dry_run=True` reconcile with an orphaned in_progress volume/model row → cleanup hook (delete/drop) is NOT called; a log line says "[DRY RUN] would clean up".
- [ ] Run → FAIL. Implement: early-return/skip the destructive hook when `config.dry_run`. Run → PASS. Commit.

### Task 1.8: Collision probes fail-closed + dotted-FQN parse (#10)
**Files:** Modify `src/pre_check/collision_detection.py` (all `_*_exists` handlers + `_fqn_to_parts`); Test `tests/unit/test_collision_detection.py`
- [ ] Write failing tests: (a) probe raises `PermissionDenied` → does NOT return False; propagates as a distinct check-failure signal; (b) probe raises NotFound/`RESOURCE_DOES_NOT_EXIST` → returns False; (c) `_fqn_to_parts` on a backticked identifier containing a literal dot → parsed correctly.
- [ ] Run → FAIL. Implement: catch only the not-found error shapes → False; re-raise/flag others; `detect_collisions` records a `check_failed` outcome (not "safe") on unexpected errors. Fix `_fqn_to_parts`. Run → PASS. Commit.

### Task 1.9: Phase 1 unit-suite gate
- [ ] Run `uv run pytest tests/unit/ -q` → all green (≥ 858 + new tests). Commit any stragglers.

---

## Phase 2 — Missing coverage + integration suite expansion

### Task 2.1: Ownership transfer inside the grants workers (#5)
**Files:** Modify `src/migrate/grants_worker.py` (replace the `OWN` skip ~:68), `src/migrate/hive_grants_worker.py` (replace the `OWN` skip ~:85), `src/common/config.py` (+`transfer_ownership: bool = True`), `src/discovery/discovery.py` (ensure original `owner`/`OWN` principal is captured in discovery); Test `tests/unit/test_grants_worker.py` + `tests/unit/test_hive_grants_worker.py`
- [ ] Write failing tests (both workers): (a) an `OWN` action for a securable with a known original owner → emits `ALTER <type> <fqn> OWNER TO `<owner>`` **after** the securable's non-OWN grants; (b) owner principal missing on target → status `failed`, message names the missing principal, batch continues; (c) `transfer_ownership=False` → `OWN` is skipped exactly as today.
- [ ] Run → FAIL. Implement: in each worker, stop `continue`-ing on `OWN`; instead collect OWN actions and apply them as `ALTER … OWNER TO` after the non-OWN grants for that securable, gated on `config.transfer_ownership`, with fail-loud per-row handling. Run → PASS. Commit. Update `docs/idempotency_audit.md` + `hive_common.py:52` comment (remove the false "ownership is set via ALTER…OWNER TO, worker skips" claim → describe the new behaviour).

### Task 2.2: Collision probes for global-namespace types + surfacing + coverage guard test (#6)
**Files:** Modify `src/pre_check/collision_detection.py` (+probes `_connection_exists`, `_share_exists`, `_recipient_exists`, `_registered_model_exists` using #10's fail-closed pattern; add to `_PROBES`; add an explicit `_NOT_PROBED_TYPES` allowlist with per-type reasons), `src/pre_check/pre_check.py` (emit "NOT collision-checked: [...]" for in-scope types lacking a probe); Test `tests/unit/test_collision_detection.py` + `tests/unit/test_pre_check.py`
- [ ] Write failing tests: (a) each new probe returns True/False correctly (mock SDK get) and FAIL-CLOSES on PermissionDenied; (b) `check_target_collisions` result includes an explicit list of in-scope object types not collision-checked.
- [ ] Run → FAIL. Implement probes (fail-closed) + surfacing. Run → PASS. Commit.
- [ ] **Coverage guard test (self-enforcing rule):** Write `test_every_migrated_type_is_probed_or_exempt` — derive the full set of migrated `object_type`s from the orchestrators (`orchestrator.LIST_TYPES`/`BATCHED_TYPES` + `hive_orchestrator` categories) and assert each is in `_PROBES` ∪ `_HIVE_PROBES` ∪ `_NOT_PROBED_TYPES`. If a migrated type is none of those → FAIL with a message telling the dev to add a probe or an exemption-with-reason. This makes "new migrated object ⇒ probe" fail CI rather than rely on memory.
- [ ] Run → expect PASS against current types (after exemptions are filled in). Commit.

### Task 2.3: De-conditionalize mandatory integration assertions (test policy)
**Files:** Modify `tests/integration/test_uc_end_to_end.py`, `test_hive_end_to_end.py`, `test_governance_end_to_end.py`, `test_vector_search.py`, `test_online_tables.py`; the seed files to always seed mandatory fixtures
**Principle:** For mandatory in-scope types, the seed must always succeed; if `has_X` is false at assert time the test **fails** ("fixture X was not seeded — mandatory in-scope type"). Keep soft-skip ONLY for env-blocked types, and have those emit a named tracked skip row.
- [ ] hive_external + hive_managed_nondbfs + hive_grant: make seeds unconditional (they only need a valid external location, which the lab has); flip assertions from skip→fail-if-missing.
- [ ] UC schema grant: make seed + assertion unconditional.
- [ ] external_table partitioned + volume nested + column/volume comments: make unconditional.
- [ ] vector_search + online_table: convert silent soft-skip into a tracked, named skip that the summary reports (and that CI treats as a real result, not green-by-default).
- [ ] Run each affected integration test's unit-shape lint (`tests/lint/test_notebook_shape.py`) → PASS. Commit.

### Task 2.4: New seeds + assertions for untested in-scope types (P0/P1)
**Files:** Modify `tests/integration/seed_uc_test_data.py`, `seed_*`, `test_governance_end_to_end.py`; Create Lakebase-backed connection seed helper
- [ ] **monitor**: seed a Lakehouse monitor on a managed table; assert `monitors_worker` migrates it (or records env-skip if monitor preview unavailable — tracked, not silent).
- [ ] **connection + foreign_catalog**: re-apply the `~/uksouth_migration/infra/azure-sql-test` terraform to stand up the Azure SQL Server (+ private endpoint / NCC as before); seed a UC `connection` to it + a `foreign_catalog`; assert both migrate. Re-attach the NCC to the source workspace if needed. (If the SQL Server can't be reached at run time → tracked named skip, not silent.)
- [ ] **provider**: seed/assert inbound provider if feasible; else tracked named skip with rationale.
- [ ] **share + recipient**: stabilize the F.1 customer-share assertion (the SPN-owner fix from PR #33) and make it unconditional + add a dedicated recipient assertion.
- [ ] **row_filter + column_mask reapply**: add an end-to-end leg that runs `migrate_uc` → `migrate_governance` back-to-back and asserts the filter/mask is actually present + enforced on the target (T29/T30).
- [ ] Run notebook-shape lint → PASS. Commit per object type.

### Task 2.5: Phase 2 unit-suite gate
- [ ] `uv run pytest tests/unit/ -q` → all green. Commit.

---

## Phase 3 — CI wiring, static/coverage gating, config/security

### Task 3.1: Wire all integration suites into CI + the kill/resume chain (#11)
**Files:** Modify `.github/workflows/integration.yml` (+governance, negative_paths, online_tables, vector_search, retry_resumability, collision_handling jobs); Create the DAB job chain that runs `migrate` twice with `test_kill_after: 2` (the resumability test's documented prerequisite) in `resources/integration_tests/`; Modify `.github/workflows/ci.yml` (mypy `src/common/`→`src/`; coverage `--cov=src/common`→`--cov=src`)
- [ ] Add the two-pass kill/resume job chain wiring `test_retry_resumability.py`.
- [ ] Add the missing `bundle run` invocations for the 4 unwired suites.
- [ ] Widen mypy + coverage to `src/`; set a realistic `--cov-fail-under` (measure first, then set a floor that doesn't drop coverage).
- [ ] Run `uv run mypy src/` locally; fix new type errors surfaced (track any that are large as follow-ups). Commit.

### Task 3.2: Bundle least-privilege permissions (#13)
**Files:** Modify `databricks.yml` (replace `group_name: users` with `group_name: ${var.migration_admin_group}`; add the variable with default `migration_admins`)
- [ ] Implement + update README permissions note. `databricks bundle validate` (no deploy) → PASS. Commit.

### Task 3.3: Untrack config.yaml + scrub working copy (#12)
**Files:** `.gitignore` (+`config.yaml`), `git rm --cached config.yaml`, overwrite `config.yaml` working copy with placeholders, README "Deploy + configure flow" (copy example→config)
- [ ] `git rm --cached config.yaml`; add to `.gitignore`; replace real values in the working file with placeholders; update README step 2. Commit.
- [ ] **Tracked follow-up (NOT this PR):** git-history re-scrub of the already-committed real SPN client-ID + workspace URLs before any public release — add to backlog.

### Task 3.4: Final gate
- [ ] `uv run pytest tests/unit/ -q` + `uv run mypy src/` + `databricks bundle validate` → all green. Update `docs/user_guide.md` / `README.md` for new config flags (`overwrite_existing`, `transfer_ownership`, `migration_admin_group`). Commit.

---

## Live integration retest (gated — DBU cost on lab workspaces)

After all phases land on the branch, deploy to the lab pair (source `adb-7405609086312103.3`, target `adb-7405615515664170.10`) and run the expanded suites. **Confirm with user before running — this incurs DBU.** Deploy env: `DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform DATABRICKS_TF_VERSION=1.15.5 BUNDLE_VAR_migration_spn_id=d0354350-71fa-4bb4-aa55-8adb5dd9f1ae`.

## Out of scope / external constraints
- Git-history re-scrub for #12 (release-gate, separate).
- `connection`/`foreign_catalog` live coverage depends on standing up Lakebase in-region; falls back to a tracked named skip if unavailable.
- ABAC `policy` coverage remains environment-dependent (workspace runtime may reject `SET ABAC POLICY`).

## Self-review notes
- Every finding 1-13 maps to a task: #1→1.2, #2→1.3, #3→1.4, #4→1.5, #5→2.1, #6→2.2, #7→1.6, #8→1.1, #9→1.7, #10→1.8, #11→3.1, #12→3.3, #13→3.2.
- Test-coverage expansion (the user's second objective) → Tasks 2.3 + 2.4 + 3.1, governed by the locked Test policy.
