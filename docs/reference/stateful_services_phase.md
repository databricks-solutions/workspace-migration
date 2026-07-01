# Stateful Services Phase — separate future job

**Status:** planned (not yet implemented).
**Scope boundary:** this document enumerates the Databricks object
types that the **core migration tool** (this repo) explicitly does
*not* fully migrate because correct migration requires stateful /
runtime orchestration that belongs in a separate phase and a separate
job. The Stateful Services Phase will be built as a follow-on tool
that consumes the same `discovery_inventory` this tool writes, so
operators can plan a full migration as: core tool → Stateful Services
Phase.

## Why a separate phase

The core tool is a **stateless DDL/API replayer** — it re-creates
catalog objects on the target by re-issuing DDL or SDK calls and
then validates the result. The object types below fail that model
because they carry *runtime* state that cannot be transferred by
replaying a CREATE statement:

- stream offsets (Kafka, Kinesis, Pulsar),
- structured-streaming checkpoints (Auto Loader, Delta CDF cursors),
- ingestion cursors (Lakeflow Connect source watermarks / SCD position),
- online-store deltas (online-tables sync lag, Lakebase WAL),
- vector index materialisation,
- running compute (Apps, Model Serving endpoints).

Forcing the core tool to cover these would double its blast radius
and couple it to product APIs that are evolving rapidly. The
Stateful Services Phase will own the runtime orchestration (cut-over
coordination, offset snapshot + replay, endpoint health gates).

## Object types in scope for the Stateful Services Phase

| Object type | Why stateful | Planned future handling | Current-tool behaviour |
|---|---|---|---|
| **Streaming Tables** (`st`) | Source stream state — Kafka/Kinesis offsets, Auto Loader checkpoints, Delta CDF cursors — does not transfer by DDL replay. Target would restart from the source's current position, duplicating or missing records. | Phase job coordinates a quiesced cut-over: drain source stream → snapshot offsets → re-create ST on target with explicit offsets → resume. | Hard-excluded. `mv_st_worker` short-circuits with `skipped_by_stateful_service_migration`. |
| **DLT pipelines (MV / ST)** (`mv`, `st` where `spec.libraries` non-empty) | A DLT pipeline is the entire graph — dependencies, dataset defs, checkpoints, expectations. Re-issuing a CREATE on target orphans the pipeline from its source definition. | Phase job (or the existing separate pipelines-migration tool) re-publishes the pipeline definition from its source files and coordinates cut-over. | `mv_st_worker` skips DLT-owned MVs/STs with `skipped_by_pipeline_migration`. (STs further short-circuit to `skipped_by_stateful_service_migration` before the DLT check runs.) |
| **Lakeflow Connect — Tier 1: query-based DB + SaaS row_filter** (SQL Server/PostgreSQL/MySQL/Oracle/Teradata/MariaDB query-based; Salesforce/GA4/ServiceNow) | A managed-connector ingestion pipeline carries a source-side cursor / watermark (and SCD position) that does not transfer by replaying a CREATE. **Known hard gap:** LFC pipelines can't be re-pointed at a target even on a shared metastore — `ALTER ... SET PIPELINE_ID` is blocked (Aha DB-I-18972). So it's a *cut-over*. | **Available now** via `migrate_lfc`: clone each landed table to `<t>_history` (Delta Sharing + `DEEP CLONE`), recreate the pipeline writing `<t>_incr` with a per-table `row_filter = "<cursor> >= '<T>'"` boundary, trigger it, then build a unified view at `<t>` (SCD1 → PK-dedup; SCD2/append → `UNION ALL`). Query-based reads the cursor from the spec; **SaaS requires the operator to supply the cursor** via `lfc_saas_cursor_columns` (discovery surfaces candidates) — no cursor → full-load. Operator pre-creates the UC connection + sets `lfc_target_connection_name`. See user guide Step 9. | **Available now.** Emits `lfc_table` (`validated`), `lfc_pipeline` (`lfc_pipeline_created_incremental`), `lfc_view` (`lfc_view_created` / `lfc_view_skipped_no_cursor`); re-run → `skipped_target_pipeline_exists`. Salesforce live-validated; GA4/ServiceNow share the path, pending a live test. |
| **Lakeflow Connect — Tier 2: CDC / gateway DB** (SQL Server / MySQL / PostgreSQL in CDC mode) | Same gap, plus a CDC source carries change-stream state and runs via a continuous **ingestion gateway** + ingestion pipeline. No settable boundary, so a full re-hydrate is unavoidable. | **Available now** via `migrate_lfc`: recreate the gateway + ingestion pipeline mirroring source topology (gateway reuses `lfc_target_connection_name`, then is **stopped**), remap `ingestion_gateway_id`, dry-validate — **create-only, not started**; the customer starts the continuous re-hydrate at cutover. The gateway's staging volume is excluded from volume migration. Pre-cutover SCD2 history is not preserved. | **Available now** (SQL Server live-validated; MySQL/Postgres share the path, pending a live test). Emits `lfc_gateway` (`lfc_gateway_created`) + `lfc_pipeline` (`lfc_pipeline_created_fullreload`). |
| **Lakeflow Connect — non-row_filter SaaS** (Workday, NetSuite, SharePoint, Dynamics 365, etc.) | No settable boundary and no gateway — a full re-load on recreate. | Future: recreate the pipeline and full re-hydrate. | Not yet in scope — not migrated by any current job. |
| **Online Tables** | The online store carries a continuous delta sync against the source UC table. Migrating only the spec leaves the target spec running but empty until it catches up (and state from the source sync is lost). | Phase job snapshots the delta lag, pauses source sync, seeds target, resumes. | Migrated by the `migrate_online_tables` job as a Lakebase **synced table** (legacy online tables deprecated; create blocked). Re-syncs from the source Delta table into a Lakebase instance the job creates; consumer repoint is operator-owned. |
| **Lakebase (`database_instance` + `synced_table`)** | A Lakebase instance is a live Postgres-compatible service with WAL and client connections; synced tables ride a replication pipeline. Nothing about this is a stateless DDL replay. | Phase job provisions a target instance, coordinates client cut-over, re-establishes sync from a consistent snapshot. | Not in scope for the core tool at all — no worker discovers or migrates `database_instance` / `synced_table`. |
| **Vector Search indexes** | An index carries materialised embeddings keyed off a source table — re-creating the spec means re-embedding everything (cost + latency). | Phase job either replays the source-side embedding job against the target, or snapshots the vector store if the backend supports it. | Migrated by the `migrate_vector_search` job — Delta Sync indexes recreated and re-sync triggered from the target source table. Direct Access indexes skipped (`skipped_direct_access_unsupported`); see user guide. |
| **Model Serving endpoints** | Endpoints carry scaling state, warm caches, attached route configs, and dependent client traffic. Migrating them as "POST the spec" destroys the endpoint identity clients rely on. | Phase job provisions target endpoints, re-attaches routes, coordinates traffic cut-over. | Not in scope for the core tool. |
| **Apps** | Databricks Apps are running compute with its own runtime state, routes, permissions, and secrets. | Phase job deploys the app to the target workspace and coordinates URL cut-over. | Not in scope for the core tool. |

## Status taxonomy (current tool)

The core tool emits one of three terminal skip statuses for stateful
services:

- `skipped_by_pipeline_migration` — DLT-owned MV (and formerly ST; STs
  now hit the new status first). Handled by the separate DLT/pipelines
  migration tool.
- `skipped_by_stateful_service_migration` — object deferred to the
  Stateful Services Phase. Currently used for streaming tables; the
  taxonomy is general so future stateful objects can join the filter
  without another schema change.
- `skipped_target_exists` — object already exists on target under the
  `on_target_collision: skip` policy (X.4). Not stateful-services-
  related; listed here for completeness of the terminal set.

All three are terminal in `TrackingManager.get_pending_objects` — a
re-run of the core tool will not re-emit them.

## Hand-off contract

The core tool writes `discovery_inventory` rows for **every** object
type it discovers (including STs, online tables, etc.) even when it
does not migrate them. The Stateful Services Phase will read the same
`discovery_inventory` to plan its own workload, so operators do not
have to run discovery twice.

The core tool's `migration_status` table is **not** mutated by the
Stateful Services Phase — each phase keeps its own ledger, joined on
`(object_type, object_name)` when a cross-phase view is needed.

## Cross-references

- `docs/idempotency_audit.md` — per-worker idempotency audit and
  terminal-status taxonomy.
- `docs/retry_resumability.md` — reconciliation decision table (includes
  `skipped_by_stateful_service_migration` as a no-op row).
- `src/migrate/mv_st_worker.py` — today's short-circuit for streaming
  tables.
- `src/common/tracking.py :: TrackingManager.get_pending_objects` —
  terminal-status IN list.
