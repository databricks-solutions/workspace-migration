# Lakeflow Connect Migration (`migrate_lfc`) — design

**Date:** 2026-06-15
**Status:** brainstorm complete — pending spec review, then implementation plan
**Scope:** Migrate Lakeflow Connect (LFC) ingestion pipelines and their landed
data from a source workspace to a target workspace as a new standalone
`migrate_lfc` job. Consumes the `source_type='stateful'`,
`object_type='lfc_pipeline'` rows produced by stateful discovery (PR #53), plus
new discovery for CDC ingestion gateways. Part of the Stateful Services Phase
(one job per stateful service — after Vector Search and Online Tables).

## Background

LFC ingestion is **not migratable as state** — it is a **cut-over, not a
migration**. The three hard facts (validated, Aha DB-I-18972):

1. `ALTER … SET PIPELINE_ID` is **blocked** for LFC-managed streaming tables, so
   a landed destination table cannot be re-pointed at a new pipeline.
2. Pipeline cursor / checkpoint state is **not portable** across workspaces.
3. For CDC connectors, the gateway's resume position is a **source-side cursor**
   (Postgres replication slot / SQL Server CDC offset) plus the pipeline's own
   checkpoint — neither transplantable; and resume is bounded by the source
   log's retention window (default 3 days).

So for every LFC pipeline the target gets a **new** pipeline. The design problem
is entirely: *how do we avoid re-pulling the history from the source?* The
answer depends on whether the connector exposes a **settable boundary handle**.

### Guiding constraint

Build the target from **already-landed data** wherever possible; do **not**
reconnect to the source to re-pull history. This is fully achievable for
connectors with a settable boundary (Tier 1) and **not** achievable for the rest
(Tier 2) — a product limitation we accept and document.

## Decisions (brainstorm 2026-06-15)

| # | Decision |
|---|---|
| D1 | One job per stateful service. This spec = `migrate_lfc` only. |
| D2 | The real split is **"settable boundary handle or not,"** not SaaS-vs-DB. Two tiers. |
| D3 | **Tier 1** (clean resume): clone landed history + recreate pipeline with `row_filter ≥ T` + unified view. Honors no-re-pull. |
| D4 | **Tier 2** (no boundary handle): recreate the pipeline and let it **fully re-hydrate** from source. No clone, no view. Re-pull is accepted. |
| D5 | **Option B table layout** (Tier 1): cloned history → `<t>_history`, recreated pipeline → `<t>_incr`, unified view at the **canonical** name `<t>`. |
| D6 | Unified-view shape is chosen by `scd_type`: SCD1 → PK-dedup merge; SCD2/append → `UNION ALL` (+ SCD2 boundary stitch). |
| D7 | `row_filter` is **per-table**. In one pipeline, incremental tables get a filter; batch/formula/non-incremental tables get none (they full-load, which is their normal behaviour). |
| D8 | Reuse Path A staging + `clone_table` for the data clone (Tier 1), with two small refactors: explicit target FQN + parametrized `object_type`. |
| D9 | **CDC SCD2 history-loss is accepted and documented** (D4): a re-pull rebuilds history only from cutover forward; pre-cutover `__START_AT/__END_AT` versions are lost. Operators who must keep them archive the old table separately. |
| D10 | The gateway↔ingestion **relationship** is captured at discovery and used to pair + order + re-wire recreation; the source id **values** are discarded and remapped to new target ids. |
| D11 | **Real-resource integration tests for all 4 scenarios**, on two source systems: one Azure SQL (S3, CDC+CT) for C+D, one Salesforce dev org for A+B (B routed through Tier 2 with no filter). No preview-gated stubs. Coverage guard fails RED on any unexercised scenario. |

## The two tiers

**Tier 1 — clean resume (honors no-re-pull).** Connector exposes a `row_filter`
on its cursor column (SaaS `row_filter` connectors + all query-based DB). Clone
the landed history, recreate the pipeline to pull only rows `> T`, stitch with a
unified view.

**Tier 2 — re-pull unavoidable.** No settable boundary (CDC/gateway DB +
non-`row_filter` SaaS). Recreate the pipeline (and gateway, for CDC) and let it
fully re-hydrate from source. No clone, no view.

### Connector → tier mapping

| Tier | Connectors |
|---|---|
| **1** | Salesforce, Google Analytics, ServiceNow; **all query-based DB** (Oracle, Teradata, SQL Server-qb, MySQL-qb, MariaDB-qb, PostgreSQL-qb, federation sources) |
| **2** | **CDC/gateway DB** (SQL Server, MySQL, PostgreSQL in CDC/CT mode); non-`row_filter` SaaS (Workday, HubSpot, Jira, SharePoint, Google Drive, RabbitMQ, …) |

Classification inputs (from the discovered spec): connector/source type,
presence of a `gateway_definition` (⇒ CDC), `row_filter` capability, and presence
of an explicit `cursor_column` (query-based).

## The three components (per pipeline)

| Component | Handling |
|---|---|
| **UC connection** | Already migrated by core `connections_worker`. Re-wired by id on recreation. |
| **Destination table(s)** (the data) | **Tier 1**: clone landed history (Option B). **Tier 2**: none — the recreated pipeline reloads it. |
| **Ingestion pipeline** (+ **gateway** for CDC) | Always recreated fresh from the discovered spec, ids remapped. Tier 1 adds per-table `row_filter ≥ T`; Tier 2 full-reloads. |

## Architecture & components

New standalone job `migrate_lfc`, task chain (mirrors `migrate_vector_search`):

```
pre_check_lfc → orchestrator → migrate_lfc → summary_lfc
```

The `orchestrator` retains `reconcile_stale_runs` (resumability) and publishes
the pending `lfc_pipeline` list (with paired gateway rows) as a task value the
worker consumes — identical to the other jobs.

New files:
- `src/pre_check/pre_check_lfc.py` — connection/catalog/schema existence + tier classification gate
- `src/migrate/lfc_worker.py` — per-pipeline migration logic (both tiers)
- `resources/production/migrate_lfc_workflow.yml`
- `resources/integration_tests/lfc_integration_test_workflow.yml`
- `tests/unit/test_lfc_worker.py`, `tests/unit/test_pre_check_lfc.py`

### Discovery extensions (in `src/common/stateful_utils.py`)
- **Discover CDC gateways**: today `list_lfc_pipelines` filters on
  `ingestion_definition` only and **misses** gateway pipelines
  (`pipeline_type=INGESTION_GATEWAY`, `gateway_definition`). Add gateway
  enumeration and the `ingestion_gateway_id → gateway_pipeline_id` edge so a CDC
  pipeline is discovered as a **(gateway, ingestion)** pair.
- Capture per-table `scd_type`, `primary_keys`, `cursor_column`, and
  `row_filter`-capability into the row's `metadata_json.definition` (these come
  straight from `ingestion_definition.objects[].table.table_configuration`).

## Worker logic (`lfc_worker.py`)

Notebook worker, established shape (bootstrap → functions → `run(dbutils, spark)`
→ guard). Reads the pending pipeline list from the orchestrator task value.
Per pipeline, classify the tier, then:

### Tier 1 (clean resume)
The strategy is **per-table** (D7), because a single pipeline can mix
incremental and batch/formula tables.

**Incremental tables** (have a usable cursor):
1. `T = MAX(cursor_column)` from the **landed** table (cursor column is present
   because it is ingested).
2. **Clone history** → `<schema>.<table>_history` via the reused Path A staging
   + `clone_table` path (CTAS the ST into staging — required because an ST is not
   directly deep-clonable — share it, deep-clone on target). Record `validated`
   under `object_type='lfc_table'`.
3. The recreated pipeline writes this table to `<schema>.<table>_incr` with
   `row_filter = "<cursor_col> >= '<T>'"`.
4. **Create the unified view** at the canonical name `<schema>.<table>`:
   - SCD1 → PK-dedup merge: `ROW_NUMBER() OVER (PARTITION BY <primary_keys> ORDER BY <cursor> DESC)`, keep `rn=1`;
   - SCD2/append → `UNION ALL` (+ stitch v1's open `__END_AT` at the boundary for SCD2).
   Record `lfc_view_created`.

**Batch / formula / non-incremental tables** (no cursor `row_filter` possible):
no clone, no view — the recreated pipeline writes them to the **canonical** name
and full-loads them (their normal every-run behaviour; v2 alone is complete).

**Recreate the pipeline** once for the whole table set: same connection (remapped
id), per-table `scd_type` / `primary_keys`, `channel=PREVIEW`, filtered objects
for incremental tables and unfiltered objects for batch tables. Record
`lfc_pipeline_created_incremental`.

### Tier 2 (re-pull, accepted)
1. **(CDC only)** provision target **staging volume**; recreate the **gateway**
   from `gateway_definition` (remapped `connection_id`); capture the new
   `gateway_pipeline_id`. Record `lfc_gateway_created`.
2. **Recreate the ingestion pipeline** at the **canonical** table name (CDC:
   `ingestion_gateway_id` = new gateway id), let it perform a full initial load.
   Record `lfc_pipeline_created_fullreload`.
3. No clone, no view. SCD2 pre-cutover history is not preserved (documented).

Per-pipeline isolation: one pipeline failing (record `failed` + error) never
aborts the others. Recreation order always follows the dependency chain
**connection ← gateway ← ingestion pipeline**, remapping ids at each edge (D10).

## New statuses (`src/common/tracking.py`)

Terminal:
- `lfc_pipeline_created_incremental` — Tier 1 pipeline recreated with `row_filter`.
- `lfc_pipeline_created_fullreload` — Tier 2 pipeline recreated (full re-hydrate).
- `lfc_gateway_created` — CDC gateway recreated on target.
- `lfc_view_created` — unified view created (Tier 1).

(`lfc_table` history clones reuse the existing `validated` / `validation_failed`
statuses via the parametrized `clone_table`.)

Non-terminal (re-pickable):
- `skipped_target_pipeline_exists` — idempotent re-run guard if the target
  pipeline/view already exists.

## Reuse seams (`clone_table`, Path A)

`clone_table` (`managed_table_worker.py:105`) is the consumer half of the
Delta-Sharing flow and already CTAS-stages RLS/CM tables (Path A) — exactly the
shape an ST needs. Two small refactors:
1. **Explicit target FQN** — `clone_table` currently assumes `target == source`;
   Tier 1 needs to land at `<table>_history`.
2. **Parametrized `object_type`** — currently hardcoded `managed_table`; LFC
   history clones record under `lfc_table` for the coverage guard + summary.

## Pre-check (`pre_check_lfc.py`)

For each discovered LFC pipeline: verify the UC connection exists on target and
the destination catalog/schema exist (Tier 1 view + Tier 2 reload both need
them). Classify the tier and record it. Tier-2/CDC additionally records the
infra preconditions (network egress to source DB, classic-compute gateway, target
staging volume) as **operator preconditions** in the pre-check result. Fail the
job up-front (via the existing orchestrator gate) only on hard blockers
(connection/catalog/schema absent).

## Error handling

- Per-pipeline isolation (one bad pipeline → `failed`, continue).
- Recreation failure at any edge of the dependency chain → `failed` for that
  pipeline with the edge error; do not leave a half-wired pair (best-effort
  cleanup of a created gateway if the ingestion-pipeline create fails).
- LFC / pipelines API not permitted on target → loud failure (fail-loud posture).

## Testing

**Goal: real-resource integration coverage of all four scenarios** (not
preview-gated stubs), mirroring the confidence the governance/hive suites built.
The four scenarios collapse onto **two source systems** because the tool's code
paths are connector-agnostic within a tier.

### Source systems (real)

| Source | Scenarios covered | Provisioning |
|---|---|---|
| **One Azure SQL Server** (S3 SKU, **CDC *and* Change Tracking** enabled on the seeded tables) | **C** query-based + **D** CDC/gateway | Extend `infra/azure-sql-test` (existing PE + NCC + cursor-friendly seed). S3 SKU so true CDC — not just CT — is exercised. |
| **One Salesforce dev org** (`lfc-test`, already seeded) | **A** Tier-1 `row_filter` + **B** Tier-2 no-filter | Creds-gated UC connection (same posture as the governance connection test). |

- **C (query-based):** existing `dbo.customers`/`dbo.orders` already carry
  timestamp cursor columns (`created_at`/`placed_at`) + PKs. Test creates the
  connection + query-based pipeline; serverless reaches the DB via the existing
  NCC PE.
- **D (CDC/gateway):** enable CDC + CT on the seeded tables; test provisions a
  **staging volume**, recreates **gateway + ingestion pipeline**; the classic
  gateway reaches the DB via the existing workspace-VNet PE. This is the riskiest
  code (gateway recreate, id-remap, full reload) → real coverage matters most.
- **A (SaaS `row_filter`):** Salesforce pipeline **with** a cursor `row_filter` →
  Tier-1 clone + filtered recreate + unified view.
- **B (SaaS non-`row_filter`):** **same Salesforce org**, a pipeline **without** a
  `row_filter`, deliberately routed through **Tier 2** → full reload, no
  clone/view. Salesforce-no-filter faithfully reproduces B's runtime (full initial
  load); the Tier-2 worker is connector-agnostic so no real non-`row_filter`
  connector hits a path this misses. The **classifier's** "Workday/HubSpot/Jira →
  Tier 2" decision (the only connector-specific part) is covered by **unit test**
  against the real connector-type strings.

### Test workflows
- `lfc_integration_test_workflow.yml` — seed → discovery → pre_check → migrate →
  assert, run live against the two source systems. Per-scenario assertions:
  C/A → history clone + `_incr` pipeline with `row_filter` + unified view;
  D/B → recreated pipeline (+ gateway for D) + full-reloaded table.
- **Coverage guard** (same red-if-untested pattern as the UC/governance/hive
  suites): fail RED if any of the 4 scenarios isn't exercised (validated or
  documented-exempt with reason).

### Unit
- `test_lfc_worker.py`: Tier-1 clone+filtered-recreate+SCD1-merge-view; Tier-1
  SCD2 union view; per-table mixed filter (incremental vs batch); Tier-2
  full-reload recreate; CDC gateway recreate + id remap + ordered wiring;
  idempotent `skipped_target_pipeline_exists`; per-pipeline isolation on failure.
- `test_pre_check_lfc.py`: **tier classification across all connector types**
  (incl. Workday/HubSpot/Jira → Tier 2); missing connection/catalog → FAIL; CDC
  preconditions recorded. Mock `pipelines`, `connections`, `tables`, warehouse exec.

### Infra build (staged — see implementation plan)
Real testing requires a live lab (sandbox; likely expired — rebuild first). Build
in layers, pausing for verification between each: ① workspaces → ② Azure SQL
(S3) + PE + NCC → ③ enable CDC/CT + cursor seed → ④ Salesforce connection +
seeded objects → ⑤ LFC pipelines/seeds + test workflows.

## Docs (ship with the code)

- New `migrate_lfc` section in `user_guide.md`: the two-tier model, Option B
  layout, the run-the-job-is-opt-in model, and the **Known limitations** below.
- Update `stateful_services_phase.md`: LFC now has a migration job.

### Documented caveats / known limitations
1. **CDC + SCD2 history loss** (D9): a Tier-2 re-pull rebuilds SCD2 history only
   from cutover forward; pre-cutover `__START_AT/__END_AT` versions are lost.
   Operators who must keep them archive the old table before cutover.
2. **Tier-2 re-pull**: CDC and non-`row_filter` SaaS re-hydrate from source — the
   "don't re-pull" guarantee applies only to Tier 1.
3. **Tier-1 deletes after cutover** are not propagated (LFC `row_filter`
   limitation); SCD1 unified view shows ghost rows for post-cutover source
   deletes until a reconcile. Query-based supports `deletion_condition`
   (soft-delete) / hard-delete (Beta) — carried over in the recreated spec.
4. **Batch / formula tables** (e.g. Salesforce formula-field tables) can't use a
   cursor `row_filter` and full-load on the recreated pipeline even in Tier 1.
5. **CDC cutover timing**: start the new gateway at/before cutover so its capture
   window overlaps the snapshot → no data gap.

## Out of scope (YAGNI)

- Automated Tier-2 archive of SCD2 history (operator-owned per D9).
- Migrating the staging volume contents or gateway checkpoint (proven impossible).
- CDC dual-run gap automation (operator-owned timing).
- Dependency-graph / topo-sort across stateful services.
- Other stateful services (Apps, Model Serving — separate jobs/specs).

## Suggested phasing (for the implementation plan)

1. Discovery extensions (gateway + pairing + per-table config) + pre-check + tier classifier.
2. Tier 1 (query-based first — cleanest): clone reuse + filtered recreate + view.
3. Tier 1 SaaS `row_filter` connectors (same path, connector-specific cursor columns).
4. Tier 2 non-`row_filter` SaaS (recreate full-reload — no gateway).
5. Tier 2 CDC (gateway + staging volume + ordered wiring) — heaviest, infra-gated.

## File-touch summary

- `src/migrate/lfc_worker.py` — **new**
- `src/pre_check/pre_check_lfc.py` — **new**
- `src/common/stateful_utils.py` — gateway discovery + pairing edge + per-table config capture
- `src/migrate/managed_table_worker.py` — `clone_table`: explicit target FQN + parametrized `object_type`
- `src/common/tracking.py` — add the 4 terminal LFC statuses
- `resources/production/migrate_lfc_workflow.yml` — **new**
- `resources/integration_tests/lfc_integration_test_workflow.yml` — **new**
- `tests/unit/test_lfc_worker.py`, `tests/unit/test_pre_check_lfc.py` — **new**
- `docs/user_guide.md` — `migrate_lfc` section + Known limitations
- `docs/stateful_services_phase.md` — LFC now has a migration job
- `infra/azure-sql-test/` (local infra repo, not this bundle) — bump SKU to S3;
  enable CDC + Change Tracking on seeded tables; cursor columns already present.
  Salesforce connection/seeded objects reuse the `lfc-test` dev org.
