# Stateful-Service Discovery Extension — design

**Date:** 2026-06-03
**Status:** approved (brainstorm complete) — implementation plan pending
**Scope:** discovery only. Enumerate the stateful-service object types the core
tool does not yet discover, persist their raw specs into the existing
`discovery_inventory`, and tag them so the future Stateful Services Phase can
query its slice. **No dependency-graph building, no topo-sort, no migration /
cut-over logic** — those are explicitly later specs.

## Background

The core migration tool is a stateless DDL/API replayer. The Stateful Services
Phase (`docs/stateful_services_phase.md`) is a planned follow-on that will
migrate object types carrying runtime state. Its hand-off contract says the
core tool writes `discovery_inventory` rows for *every* object type it
discovers, and the phase reads those rows to plan its workload.

Today that contract is unmet: the existing `discovery` job discovers
`online_table` but **not** Vector Search indexes, Apps, Lakebase
(`database_instance` + `synced_table`), Model Serving endpoints, or Lakeflow
Connect (LFC) ingestion pipelines. This spec closes that gap.

This also resolves an internal contradiction in `stateful_services_phase.md`
(hand-off contract says "core discovers everything"; the object table says
"core discovers none of these") — **toward "core discovers"**. The phase doc
should be reconciled when the matching code ships.

## Decisions (from brainstorm, 2026-06-02/03)

| # | Decision |
|---|---|
| Q1 | **Extend the core `discovery` job** — same job, same `discovery_inventory`, reuse existing tracking/schema plumbing. No new job, no new table. |
| Q2 | **Discover objects only; defer dependency analysis.** Each row's `metadata_json` carries the full raw spec so a later step parses edges without re-fetching. |
| Q3 | **Add all 5 missing surfaces now** (VS, Apps, Lakebase, serving, LFC). A complete inventory is required because dependency detection needs every node type present. |
| Q4 | **Tag rows `source_type='stateful'` + a `capability` subtype** in `metadata_json`. |
| Q5 | **New `StatefulExplorer` class** in `src/common/stateful_utils.py` — keep the already-1067-line `CatalogExplorer` focused; these surfaces use `WorkspaceClient`/REST, not `spark.sql` catalog traversal. |

## Architecture & components

- **New module** `src/common/stateful_utils.py` with class `StatefulExplorer`,
  constructed `StatefulExplorer(auth_manager)` (mirrors `CatalogExplorer`).
  It reaches the SDK via `auth_manager.source_client` and raw REST via
  `auth_manager.source_client.api_client` — the same access pattern as the
  existing `list_connections` / `list_online_tables`.
- **`discovery.py`** gains `_discover_stateful(config, stateful_explorer, now)`,
  called from `run()` after `_discover_uc` / `_discover_hive`. Returns rows
  appended to the same `inventory` list and written by the existing
  `tracker.write_discovery_inventory(df)` path.
- No change to `discovery_schema()` — the flat row + free-form `metadata_json`
  already accommodates every new surface. (`object_name`, `object_type`,
  `source_type`, `metadata_json`, `discovered_at` are the only required fields;
  catalog/schema are populated where meaningful, else `None`.)

## Surfaces & capture

Each `list_*` returns `list[dict]`; the **full raw spec** is stashed under a
`definition` key so the deferred dependency step can re-parse without
re-fetching (mirrors today's `list_online_tables`, which already keeps
`definition` + `source_table_fqn`).

| Surface | `object_type` | Enumeration (source_client) | `object_name` |
|---|---|---|---|
| Vector Search index | `vector_search_index` | `vector_search_endpoints.list_endpoints()` → `vector_search_indexes.list_indexes(endpoint_name)` | index full name |
| App | `app` | `apps.list()` | app name |
| Lakebase instance | `database_instance` | `database.list_database_instances()` | instance name |
| Synced table | `synced_table` | `database.list_synced_database_tables(instance)` (per instance) | synced table full name |
| Model Serving endpoint | `model_serving_endpoint` | `serving_endpoints.list()` | endpoint name |
| LFC ingestion pipeline | `lfc_pipeline` | `pipelines.list_pipelines()` filtered to specs with an `ingestion_definition` | pipeline name |

> SDK method names above are the intended targets; the implementation will pin
> them against the installed `databricks-sdk` version during the first TDD task
> and fall back to `api_client.do("GET", ...)` REST calls where an SDK helper is
> absent or behind a preview (same approach as `list_online_tables`).

**`online_table` reclassification:** the existing `online_table` row
(`discovery.py:325-336`) is changed from `source_type="uc"` to
`source_type="stateful"` and gains its `capability`. This is the only edit to
existing discovery output.

## Row tagging & capability taxonomy

All new rows: `source_type='stateful'`. A `capability` key inside
`metadata_json` marks the runtime-state class:

| `capability` | Surfaces | Runtime state |
|---|---|---|
| `vector` | `vector_search_index` | materialized embeddings |
| `lakebase` | `database_instance`, `synced_table` | Postgres WAL + replication sync |
| `online_store` | `online_table` | classic online-store sync delta |
| `compute` | `app`, `model_serving_endpoint` | running compute / routes / caches |
| `ingestion` | `lfc_pipeline` | source ingestion cursor / watermark |

`stream` is reserved for Streaming Tables, which stay `source_type='uc'` with
their existing `skipped_by_stateful_service_migration` status and are **out of
scope** here.

The existing discovery summary groups by `(source_type, object_type)`, so
`stateful` rows surface in the run summary automatically with no change.

## Error handling

Each `list_*` is wrapped in `try/except` for **preview-not-enabled /
permission-denied**: it returns `[]` but **logs a visible warning** naming the
surface and the reason, e.g.:

```
[stateful][warn] vector search not enabled or not permitted — skipping (PermissionDenied)
```

This deliberately diverges from the existing silent `return []` in
`list_connections` (flagged as a risk in the 2026-04-27 review): a skipped
surface must be visible in the run log so discovery never *looks* complete when
a surface was silently empty. One surface erroring never aborts the other four,
nor the UC/Hive scans. A workspace with a preview disabled yielding zero
`stateful` rows is a normal no-op (same contract as an empty Hive metastore).

## Testing

- **Unit** — `tests/unit/test_stateful_utils.py`: mock `source_client` /
  `api_client`. Per surface assert: returned shape, `definition` carries the
  raw spec, `capability` is correct, `object_type` / `object_name` correct, and
  preview-not-enabled → `[]` **and a logged warning** (not a raise). Add a
  pin that `online_table` now carries `source_type='stateful'`.
- **Discovery integration** — extend the discovery assertions so `stateful`
  rows land in `discovery_inventory` when fixtures exist; tolerate zero rows
  where a preview is off (normal no-op). Reuse the existing
  `setup_test_config` / seed harness; add new seeders only where a surface can
  be cheaply created in the test workspace.
- No migration workers in this spec.

## Out of scope (YAGNI)

- Dependency-graph building, edge extraction, topological sort.
- Any migration, cut-over, or `skipped_by_*` status emission for the new types.
- Migration of Vector Search + Online Tables — that is the **next** spec, and
  the first to consume these rows.
- Reconciling `stateful_services_phase.md` prose — done when the code ships,
  per the "docs ship with functionality" rule.

## File-touch summary (for the implementation plan)

- `src/common/stateful_utils.py` — **new**: `StatefulExplorer` + 6 `list_*`.
- `src/discovery/discovery.py` — add `_discover_stateful()`, call it in
  `run()`, reclassify the `online_table` row.
- `tests/unit/test_stateful_utils.py` — **new**.
- `tests/integration/test_*` — extend discovery assertions (+ seeders where cheap).
