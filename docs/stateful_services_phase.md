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
| **Lakeflow Connect ingestion pipelines** (GA4, Salesforce, SQL Server, ServiceNow, Workday, etc.) | A managed-connector ingestion pipeline carries a source-side ingestion cursor / watermark (and SCD position) that does not transfer by replaying a CREATE. **Known hard gap:** LFC pipelines cannot be re-pointed at a target workspace even on a shared metastore — `ALTER ... SET PIPELINE_ID` is blocked for LFC (Aha DB-I-18972). So this is a *cut-over*, not a migration. | Phase job stands up a **new** LFC pipeline on the target against the same connection/ingestion gateway, then bridges historical vs new data via either a `START_DATE` boundary or a server-side `row_filter` push-down on the ingested objects, exposed through a unified view. Coordinates source quiesce → target backfill → reader cut-over. | Not in scope for the core tool — no worker discovers or migrates LFC pipelines / ingestion gateways / connections-as-ingestion-source. |
| **Online Tables** | The online store carries a continuous delta sync against the source UC table. Migrating only the spec leaves the target spec running but empty until it catches up (and state from the source sync is lost). | Phase job snapshots the delta lag, pauses source sync, seeds target, resumes. | `online_tables_worker` POSTs the spec with an explicit state-loss warning in `error_message`. Operators currently accept the "restart from source" semantics at their own risk. |
| **Lakebase (`database_instance` + `synced_table`)** | A Lakebase instance is a live Postgres-compatible service with WAL and client connections; synced tables ride a replication pipeline. Nothing about this is a stateless DDL replay. | Phase job provisions a target instance, coordinates client cut-over, re-establishes sync from a consistent snapshot. | Not in scope for the core tool at all — no worker discovers or migrates `database_instance` / `synced_table`. |
| **Vector Search indexes** | An index carries materialised embeddings keyed off a source table — re-creating the spec means re-embedding everything (cost + latency). | Phase job either replays the source-side embedding job against the target, or snapshots the vector store if the backend supports it. | Not in scope for the core tool. |
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
