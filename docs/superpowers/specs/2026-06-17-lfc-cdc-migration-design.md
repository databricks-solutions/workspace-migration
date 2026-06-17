# LFC CDC (Tier-2, SQL Server) Migration — Design Spec

**Status:** Design agreed 2026-06-17. Next: implementation plan → subagent-driven build.
**Branch:** `feat/lfc-migration-cdc` (off `main`, which now has the query-based + SaaS stages).

## Goal
Extend `migrate_lfc` to migrate **CDC / gateway-based** Lakeflow Connect pipelines (SQL Server first). These are **Tier-2**: there is no settable boundary, so the migration **recreates** the ingestion gateway(s) + ingestion pipeline(s) on the target, mirroring the source topology, and leaves them **created + validated but not started**. The customer starts the continuous gateway / re-hydrate at cutover.

## Background
A CDC source is **two linked pipeline objects**:
- an **ingestion gateway** (`gateway_definition`: a SQL Server connection + a UC **managed staging volume** at `gateway_storage_{catalog,schema,name}` where it lands raw snapshot/change data), and
- an **ingestion pipeline** (`ingestion_definition` with `ingestion_gateway_id` → the gateway) that reads the staging and writes the destination tables.

The gateway must run as a **continuous** pipeline; it never reaches `COMPLETED`. SQL Server change capture can be **Change Tracking (CT, preferred, PK tables)** or **CDC (`sys.sp_cdc_*`, no-PK / full history)** — the migration logic is **identical** either way (it never touches the capture mechanism).

## Decisions (agreed)

| # | Decision |
|---|---|
| D1 | **Tier-2: recreate, full re-hydrate.** No history clone, no `row_filter`, no unified view. Pre-cutover SCD2 history is **not preserved** (documented limitation). |
| D2 | **Create-only + dry-validate; do NOT start.** Recreate the objects, run `start_update(validate_only=True)` to confirm the config resolves, then leave them **created-and-validated, not started**. Starting the continuous gateway / re-hydrate is the customer's cutover action. |
| D3 | **Mirror source topology exactly.** Reproduce the same gateways and the same gateway↔ingestion-pipeline mapping on the target (1→1, N→N). Each unique source gateway is recreated **once**; ingestion pipelines reference their mapped new gateway. |
| D4 | **Exclude the gateway staging volume** from volume migration. It is a UC managed volume often co-located with destination data; it is the gateway's internal working storage and the recreated gateway creates its own. Discovery tags/excludes it in place (see Discovery). |
| D5 | **Reuse `lfc_target_connection_name`** for the gateway's target SQL Server connection (same knob as query-based). |
| D6 | **Leave the source untouched.** |

## Components & data flow

```
Source:   [gateway pipeline] --(ingestion_gateway_id)--> [ingestion pipeline] --> dest tables
                 |                                              |
          gateway_storage volume (staging)              destination tables (the real data; migrated
                 |                                       separately by table migration)
                 X  excluded from volume migration

Target (this stage creates, does NOT start):
          recreate gateway (new id, target connection, mirrored storage location)
            └─ recreate ingestion pipeline(s) → new gateway id, full-reload config
            └─ validate_only=True on each → confirm resolves
          (customer starts continuous run at cutover)
```

## Discovery (one job; `src/discovery/discovery.py`)
Discovery is a single job: `_discover_uc` (tables + **volumes**) → `_discover_stateful` (pipelines), accumulated into one in-memory `inventory`, written once to `discovery_inventory`. The migration workers (volume worker, `migrate_lfc`) are separate jobs that each read their slice from that one inventory.

Changes:
1. **Capture gateways.** `StatefulExplorer.list_lfc_pipelines` currently filters to `ingestion_definition` and misses gateway pipelines (they carry `gateway_definition`). Extend discovery to:
   - enumerate gateway pipelines (distinct entities), and
   - for each ingestion pipeline with an `ingestion_gateway_id`, capture the link to its gateway.
   - Represent the topology so the worker can recreate each gateway once and map pipelines to it (e.g. gateway rows `object_type=lfc_gateway` + ingestion rows referencing the source gateway id; the worker recreates gateways first and records source-gw-id → new-target-gw-id).
2. **Exclude the gateway staging volume (reconcile-and-tag).** After stateful discovery knows each gateway's `gateway_storage_{catalog,schema,name}`, match it against the already-collected volume rows and mark that row excluded (distinct `object_type=gateway_staging_volume` or an excluded status) **before** the single inventory write. The volume worker's work-list is `object_type=volume`, so it never sees it — no volume-worker change, single source of truth. (Per the project guard: this is the documented exemption — "gateway staging volume, owned by the recreated gateway".)

## Worker (`src/migrate/lfc_worker.py`)
Classification already returns `("cdc","tier2")` when `ingestion_gateway_id` is present. Add the CDC handling:

1. **Gateway-first pass.** For each unique source gateway: recreate on target via `pipelines.create` with the `gateway_definition` shape — `connection_name = lfc_target_connection_name`, `gateway_storage_{catalog,schema,name}` mirrored from source (create the catalog/schema if absent; the gateway creates the volume). Record source-gateway-id → **new** target-gateway-id (in `migration_status`/metadata so the ingestion step can look it up). Status `lfc_gateway_created`.
2. **Ingestion pipelines.** For each ingestion pipeline: recreate on target with the `ingestion_definition`, **remapping `ingestion_gateway_id`** to the new target gateway id, destination catalog/schema/tables mirrored, **no `row_filter`** (full reload). Status `lfc_pipeline_created_fullreload`.
3. **Dry-validate.** Run `start_update(validate_only=True)` on the recreated objects to confirm they resolve. Record the validation result; fail the object only on a genuine **config-resolution** error. **Do not start a real run.**
   - **LIVE-VALIDATION nuance (confirm during build):** validate behavior may differ for a gateway vs an ingestion pipeline, and an ingestion-pipeline validate may depend on the gateway having staging. Treat validation as record-and-assert; only hard-fail on clear config errors (e.g. unresolved connection / bad gateway reference). This mirrors how the query-based `pipelines.create` shape was confirmed live.
4. **Idempotency:** if the target gateway + ingestion pipeline already exist by name → `skipped_target_pipeline_exists`.
5. **No clone, no view, no trigger, no count check** (we don't run the pipelines).

## Statuses (`src/common/tracking.py`)
Add terminal statuses: `lfc_gateway_created`, `lfc_pipeline_created_fullreload`, plus `lfc_pipeline_validated` (or fold the validate result into the created status' error_message). Gateway-staging-volume exclusion status from Discovery.

## Integration test (real SQL Server + Change Tracking)
Extend the existing `azure-sql-test` lab (same server): enable **Change Tracking** on a DB + table(s), seed a **gateway + ingestion pipeline** on source, run them so the source side is real, then run discovery + `migrate_lfc`. Assert:
- gateway recreated on target (correct connection + mirrored storage location), `lfc_gateway_created`;
- ingestion pipeline recreated on target, `ingestion_gateway_id` remapped to the new gateway, full-reload config (no `row_filter`), `lfc_pipeline_created_fullreload`;
- **dry-validate passed** on the recreated objects;
- the gateway staging volume was **excluded** from volume migration (not present as a migrated volume / tagged excluded in inventory);
- topology mirrored (gateway↔pipeline mapping matches source).
- **NOT asserted:** destination-table data landing (we don't start the pipelines — by design D2).
- Coverage guard: zero CDC rows ⇒ RED.
Teardown: delete recreated gateway + ingestion pipeline + the seeded source pair; drop catalog/schema; leave the SQL server + connection.

## Out of scope (this stage)
- Starting the continuous gateway / actual re-hydrate (customer's cutover action).
- MySQL / PostgreSQL CDC (SQL Server first; same code path expected later).
- Non-row_filter SaaS Tier-2 (Workday etc.) — separate follow-up (also Tier-2 full re-load, no gateway).
