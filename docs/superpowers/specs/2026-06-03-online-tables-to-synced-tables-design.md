# Online Tables → Lakebase Synced Tables migration — design

**Date:** 2026-06-03
**Status:** approved (brainstorm complete) — implementation plan pending
**Branch:** `feat/migrate-online-tables` (evolves the branch; PR #55 description to be updated to this design)
**Supersedes:** `docs/superpowers/specs/2026-06-03-online-tables-migration-design.md` (the original "recreate the online table" approach — invalid because legacy online-table creation is now deprecated/blocked platform-wide).

## Background / why this changed

Legacy Unity Catalog **online tables are deprecated**: as of Jan 15 2026 they are inaccessible, and `online_tables.create` is **blocked platform-wide** (verified live: `BadRequest: "Online Table is being deprecated. Creating new online table is not allowed. Databricks recommends switching to Synced Tables."`). So the original `migrate_online_tables` design (recreate the online table) cannot work.

Databricks' documented replacement is the **Lakebase synced table** — a Unity Catalog read-only Postgres table that auto-syncs from a UC source table into a Lakebase database instance ([migration doc](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/migrate-from-online-tables)). This spec redefines `migrate_online_tables` to convert each discovered online table into a **Lakebase synced table** on the target.

The doc offers two targets — **online feature store** (for model/feature serving) and **synced table** (for OLTP). We scope this build to **synced tables only**: it works for *every* online table (all have a primary key — required to have created the online table), uses the existing `databricks-sdk`, and is self-contained. The feature-store path is **out of scope** (needs the `databricks-feature-engineering` dependency, only works on registered feature tables, and its real migration is a customer-owned serving-endpoint cutover) — deferred to a possible future spec.

## Decisions (brainstorm 2026-06-03)

| # | Decision |
|---|---|
| Target | **Lakebase synced table only.** All discovered online tables → synced tables. No feature_store target, no `databricks-feature-engineering` dependency, no per-table choice map. |
| Pattern | Mirror the live-validated `migrate_vector_search` job: ensure infra (create-if-missing) → create object → `created_resync_pending`. Consumer-side change (apps repointing to the new Postgres endpoint) is **out of scope, documented** (VS didn't rewire consumers either). |
| Lakebase instance | **Create-if-missing** (VS-endpoint-style): one shared target Lakebase database instance (name/logical-db/capacity from config, with defaults), created + waited-for-ready if absent, then synced tables created in it. The tool provisions paid Lakebase compute — flagged prominently in docs. |
| Synced table name | The synced table takes the **online table's FQN** on the target (free there — online tables can't exist on target). |
| Statuses | Reuse `created_resync_pending` (on accept) + `skipped_target_exists`. Add non-terminal `skipped_instance_not_ready` (Lakebase instance still provisioning; re-run finishes it). |
| Pre-check | Source Delta table exists on target **and has a primary key** (synced tables require it). Fail loud up-front. |
| Testing | Unit tests cover all worker logic (CI gate). Live integration validates the **real synced-table mechanics** via a synthetic injected online-table discovery row (legacy online tables can no longer be created to seed). Honest boundary documented. |

## Architecture & components

Job `migrate_online_tables` (unchanged shape from the prior OT work, already removed from `migrate_uc`):
```
pre_check_online_tables → orchestrator → migrate_online_tables → summary_online_tables
```
reusing the shared orchestrator's `online_table_list` task value.

- **Rewrite** `src/migrate/online_tables_worker.py` — online table → synced table conversion.
- **Rewrite** `src/pre_check/pre_check_online_tables.py` — add the primary-key check (alongside the existing source-table-exists gate).
- `src/common/tracking.py` — add `skipped_instance_not_ready` (non-terminal).
- Config (`config.yaml`): `lakebase_instance_name`, `lakebase_logical_database`, `lakebase_capacity` (defaults provided).
- Integration: rewrite `seed_online_tables_test_data.py` / `test_online_tables.py` / `teardown_online_tables.py` for the synced-table mechanics; keep the workflow YAML.
- Docs: update the `migrate_online_tables` user-guide section + the stateful_services_phase OT row.

## Worker logic (`online_tables_worker.py` rewrite)

Reads `online_table_list`. Per row (definition = discovered online-table spec: `name`, `spec.source_table_full_name`, `spec.primary_key_columns`, sync-mode flags):
1. `_build_synced_table_spec(definition)` → `SyncedTableSpec(source_table_full_name, primary_key_columns, scheduling_policy=<mapped from the online table's run_triggered/run_continuously/perform_full_copy>, timeseries_key if present)`.
2. `_ensure_lakebase_instance(target_client, name, capacity, wait budget)` — get; if absent, `create_database_instance(DatabaseInstance(name=..., capacity=...))`; poll until ready (VS-endpoint-style bounded wait). Returns ready?/instance.
3. If instance not ready → `skipped_instance_not_ready` (re-pickable).
4. `target_client.database.create_synced_database_table(SyncedDatabaseTable(name=<online_table_fqn>, database_instance_name=<instance>, logical_database_name=<config>, spec=<spec>))` → `created_resync_pending`.
5. `AlreadyExists` → `skipped_target_exists`; any other error / malformed metadata / missing spec → `failed`. Per-row exception isolation; `object_name`=online_table FQN. Reuse the VS worker structure exactly (`_result` closure, two-phase try/except, list-comprehension `run`, module logger).

No feature-store branch. No consumer rewiring.

## Pre-check (`pre_check_online_tables.py`)

For each online_table row: (a) source Delta table exists on target (`tables.get`), and (b) it has a **primary key** (inspect the table's columns/constraints for a PK; synced tables require it). Any missing → `pre_check_results` FAIL + raise (halts the job) with guidance ("ensure the source table is migrated with a primary key, or this online table can't be converted to a synced table"). Mirrors the VS pre-check's structure + broad-except-warn pattern.

## Config

```yaml
online_tables:
  lakebase_instance_name: cp_migration_lakebase      # created if missing
  lakebase_logical_database: databricks_postgres
  lakebase_capacity: CU_1
```
Defaults applied if absent. One shared instance for all migrated synced tables.

## Testing (two layers + honest boundary)

**Unit (CI gate, no real infra):** `_build_synced_table_spec` (scheduling-policy mapping, PK/timeseries carry-over), `_ensure_lakebase_instance` (existing-ready / absent→create→ready / never-ready), `migrate_online_table` (created_resync_pending / already-exists / instance-not-ready / failed / malformed / object_name=FQN), pre-check (source+PK present → pass; missing → raise). Rewrite `tests/unit/test_online_tables_worker.py` + `test_pre_check_online_tables.py`. Flip/retire any assertion from the prior OT design.

**Live integration (validates the real synced-table operation):** Legacy online tables can no longer be created to seed, so the front-half (real online table → discovered spec) is **not live-testable**. The live test validates the *conversion + creation* against real Lakebase:
1. Seed a **PK'd Delta table on the target**; **inject a synthetic `online_table` discovery row** into `discovery_inventory` (object_type=`online_table`, `metadata_json.definition` shaped exactly as discovery produces, `spec.source_table_full_name` → the seeded table).
2. Run `migrate_online_tables`: pre-check passes (source+PK on target) → orchestrator publishes the synthetic row → worker **really creates a Lakebase instance + synced table** on target.
3. Assert (retrievable via `dbutils.notebook.exit`): `database.get_synced_database_table(<fqn>)` succeeds + `migration_status == created_resync_pending`.
4. Teardown (best-effort, both sides): delete the synced table, **delete the Lakebase instance** (paid), drop the test catalog, clear tracking.
5. **Probe via the run:** if `create_database_instance`/`create_synced_database_table` is blocked/unavailable on the pair, the seed/worker reports it (retrievable) → report honestly, ship for later, no false-green.

**Honest boundary (documented in spec + PR):**
- ✅ Live-proven: Lakebase instance create-if-missing; `create_synced_database_table` accepts the reconstructed `SyncedTableSpec`; synced table lands on target.
- ⚠️ Not live-provable (platform no longer allows creating a legacy online table): that discovery captures a *real* online table's spec correctly — covered by unit tests + the already-shipped discovery (PR #53). The injected synthetic row matches discovery's exact output shape to mitigate.

## Docs

- `user_guide.md` `migrate_online_tables`: now converts online tables → **Lakebase synced tables** (legacy online tables deprecated). Note: **the tool provisions a paid Lakebase database instance**; consumer apps must repoint to the new Postgres endpoint (out of scope — operator action); precondition (source migrated with a PK); statuses.
- `stateful_services_phase.md` Online Tables row → "Migrated by `migrate_online_tables` as a Lakebase **synced table** (legacy online tables deprecated; create blocked). Source re-syncs into a Lakebase instance; consumer repoint is operator-owned."

## Out of scope (YAGNI)

- **feature_store target** + `databricks-feature-engineering` dependency + feature-table publish + serving-endpoint cutover.
- Per-table target choice config (single target now).
- Consumer repoint (app connection strings) / waiting for sync to complete.
- Auto-adding primary keys or registering feature tables on source (invasive, customer-owned — pre-check reports instead).

## File-touch summary (for the plan)

- `src/migrate/online_tables_worker.py` — rewrite (online table → synced table) + unit-test rewrite.
- `src/pre_check/pre_check_online_tables.py` — add PK check + unit-test update.
- `src/common/tracking.py` — add `skipped_instance_not_ready` (+ test).
- `config.yaml` / `config.example.yaml` — add `online_tables` Lakebase settings (+ defaults in `MigrationConfig`).
- `tests/integration/{seed,test,teardown}_online_tables*.py` — rewrite for synced-table mechanics + synthetic-row injection.
- `resources/integration_tests/online_tables_integration_test_workflow.yml` — keep (seed/migrate/test/teardown), adjust if needed.
- `docs/user_guide.md`, `docs/stateful_services_phase.md` — update.
- PR #55 description — update to this design.
