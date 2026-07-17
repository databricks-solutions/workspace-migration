# Hive like-for-like migration (HMS → HMS) — design

**Date:** 2026-07-16
**Status:** Approved (brainstorming) — pending implementation plan
**Related findings:** #9, #10, #12 (fixed), #13, #14, #15

## Problem

The workspace-migration tool's Hive path is hardcoded to **upgrade** legacy
`hive_metastore` (HMS) content **into a new Unity Catalog catalog**
(`hive_target_catalog`, default `hive_upgraded`): it creates the catalog and
rewrites every `hive_metastore.` reference to `<catalog>.` in the replayed DDL
(`hive_orchestrator.py:92`, `hive_common.py:67-80`).

This is out of scope for a **workspace migration** tool. HMS content —
external tables, `/mnt`-backed tables, DBFS-root managed tables — should
migrate **like-for-like into the target workspace's own `hive_metastore`**,
not be converted to UC. The UC-upgrade behavior also produced a cluster of
ownership defects (#13 re-run lockout, #14 catalog left SPN-owned) that are
artifacts of creating and governing a new UC catalog.

## Scope

**In scope:** migrate Hive objects like-for-like into the target
`hive_metastore` — same database/table names, same storage. This is the
**only** Hive mode; the UC-upgrade path is retired.

**Out of scope:** UC conversion of HMS content; auto-recreating `/mnt` mounts;
reading or moving mount credentials/secrets.

## Decisions (from brainstorming)

1. **Only mode is like-for-like** (`hive_metastore` → `hive_metastore`). Remove
   the UC-upgrade code (catalog creation, namespace rewrite, DBFS→cloud rehome).
2. **DBFS-root managed tables** move via a **two-hop shared external staging**
   path (no UC involvement anywhere): source writes the table data to a shared
   `abfss://` staging location; the target reads staging and writes into its own
   DBFS root as a managed table. (Delta Sharing was rejected because it operates
   only on UC tables — HMS tables can't be added to a share without a transient
   UC hop on the source.)
3. **`/mnt`-backed tables** are reported as **prerequisites** (recreate the
   mount on the target first); the tool does not touch mount credentials. A
   **pre_check guard verifies each required mount exists on the target** before
   migrating any `/mnt`-backed table.
4. **Grants + ownership** are replayed into the target `hive_metastore`, with
   idempotency mechanisms **grant-before-transfer** + **skip-transfer-if-already-
   owned** so re-runs neither lock out (#13) nor fail on re-transfer.

## Per-table-type data flow

| Type | Target | Mechanism |
|---|---|---|
| External (`abfss://` / `s3://`) | `hive_metastore.<db>.<t>`, same `LOCATION` | replay `SHOW CREATE TABLE` DDL as-is, **no data copy** |
| Managed non-DBFS (cloud path) | same `LOCATION` | replay DDL as-is, **no data copy** |
| `/mnt`-backed | same `dbfs:/mnt/...` `LOCATION` | replay DDL as-is; **pre_check verifies mount exists on target** |
| DBFS-root managed | target's own DBFS root (managed, no `LOCATION`) | **two-hop**: source → shared `abfss://` staging → target writes into its DBFS root |
| View / Function | `hive_metastore.<db>` | replay DDL as-is |

## Component changes

### `common/config.py`
- **Remove** `hive_target_catalog` and the namespace-rewrite target concept.
- **Repurpose** `hive_dbfs_target_path` (cloud rehome path) → `hive_dbfs_staging_path`:
  a shared `abfss://` container used only as the transient two-hop staging area
  (not the table's final home).
- Keep `migrate_hive_dbfs_root` as the gate for the DBFS-root copy.

### `migrate/hive_common.py`
- `rewrite_hive_namespace` / `rewrite_hive_fqn` become **identity** (target
  namespace = source namespace = `hive_metastore`); the rewrite is dropped so
  DDL is replayed as-is. Target FQN == source FQN.
- `ensure_target_catalog_and_schema` → `ensure_target_database`
  (`CREATE DATABASE IF NOT EXISTS hive_metastore.<db>`).

### `migrate/hive_orchestrator.py`
- Drop `CREATE CATALOG`; ensure target **databases** exist in `hive_metastore`.
- Keep the #12 anti-join (already fixed; matches on `object_name` only).

### Workers
- `hive_external_worker`, `hive_managed_nondbfs_worker`, `hive_views_worker`,
  `hive_functions_worker`: replay DDL as-is into `hive_metastore` (no rewrite),
  same `LOCATION`, no data copy.
- `hive_managed_dbfs_worker`: reworked to the **two-hop staging copy** — source
  writes table data to `hive_dbfs_staging_path`; target reads staging and writes
  a **managed** table into its own DBFS root (no `LOCATION`). Gated on
  `migrate_hive_dbfs_root` + staging path set + target-DBFS-root-enabled check.
- `hive_grants_worker`: replay grants + ownership into `hive_metastore`; add
  **grant-before-transfer** (`GRANT USAGE, CREATE ON SCHEMA … TO <spn>` before
  `ALTER SCHEMA … OWNER TO <original>`) and **skip-transfer-if-already-owned**;
  drop catalog-ownership handling (#14 moot — no catalog).

## Prerequisite guards (`pre_check/pre_check.py`) — new

- **Target DBFS root enabled** when DBFS-root tables are in scope.
- **Required `/mnt` mounts exist** on the target for every `/mnt`-backed table —
  fail/skip with a clear message naming the missing mount.
- **Shared staging path reachable** from both workspaces when DBFS-root is in
  scope.

## Discovery + dashboard

- Discovery classifies each table's storage type
  (external-cloud / mnt / dbfs-root / managed-nondbfs) and emits
  **mount-prerequisite markers**.
- Dashboard panel: required mounts (recreate-first prereqs), DBFS-root tables
  (copied via staging), and anything skipped/failed with reasons.

## Idempotency (#12 / #13)

- Anti-join on `object_name` (done) — re-runs subtract already-`validated`
  objects, so tables are not re-processed (no DBFS-root re-copy, no
  `LOCATION_OVERLAP`).
- Grant-before-transfer + skip-if-owned — re-runs that add a newly-in-scope
  object still have SPN `CREATE`, and the ownership step skips (already owned)
  instead of failing.

### First run / second run (worked example)

First run: `CREATE DATABASE IF NOT EXISTS hive_metastore.legacy_sales` (SPN
owns) → workers create tables (external replayed same-location; DBFS-root
staged in then written to target DBFS root) → grants worker replays grants,
then `GRANT USAGE, CREATE … TO <spn>` (grant-before-transfer), then
`ALTER … OWNER TO <original>`. End state: objects owned by originals; SPN
retains explicit `USAGE, CREATE` on databases.

Second run: #12 anti-join subtracts already-validated tables (no re-copy); a
newly-in-scope table creates fine (SPN still has `CREATE`); grants are
idempotent no-ops; ownership step **skips** (target already owns).

## SPN permissions on `hive_metastore` (user-guide update)

**Source workspace (read):**
- Legacy Hive `SELECT` + `READ_METADATA` on migrated `hive_metastore`
  databases/tables (for `SHOW CREATE TABLE`, `SHOW GRANTS`, and reading rows for
  the DBFS-root staging copy).
- Source DBFS-root read access (runs on a classic cluster; workspace-level).
- ADLS storage account key (secret) for ADLS-backed HMS external/nondbfs tables
  (legacy `fs.azure.account.key`; UC vending doesn't cover HMS `LOCATION`s).
- **Write** access to the shared staging container.

**Target workspace (write):**
- Legacy Hive `CREATE` on `hive_metastore` (create databases) and on each target
  database (create tables).
- Target **DBFS root enabled** + write access.
- **Read** on the shared staging container.
- Storage access to the same cloud paths for external tables (so replayed
  external tables resolve).
- Required `/mnt` mounts pre-existing.

**What changed vs the UC-upgrade path — no longer needs:**
- `CREATE CATALOG` on the metastore.
- UC `CREATE SCHEMA` / `CREATE TABLE` / `USE CATALOG` in a UC catalog.
- Delta Sharing privileges (`CREATE SHARE`, `CREATE RECIPIENT`) for the Hive path.
- UC external-location grants (`CREATE EXTERNAL TABLE` / `READ FILES`) for the
  Hive path.

**Now needs:** legacy Hive `CREATE` on target `hive_metastore` + databases,
target DBFS root enabled + write access, and shared-staging container access.

## Testing

**Unit:**
- Identity-rewrite guard: no `hive_metastore` → catalog leak; replayed DDL keeps
  `hive_metastore` namespace.
- Grant-before-transfer ordering (GRANT emitted before `ALTER … OWNER`).
- Skip-transfer-if-already-owned.
- Mount-existence pre_check (missing mount → fail/skip with named mount).
- Two-hop staging copy (source-stage then target-write path).

**Integration:**
- migrate_hive **re-run leg** (moves `migrate_hive` out of `RERUN_EXEMPT` in
  `coverage_manifest.py`) asserting idempotency.
- Prerequisite-guard cases (DBFS-root disabled, missing mount, staging
  unreachable).

## Findings resolved

| Finding | Outcome |
|---|---|
| #15 no HMS→HMS mode | resolved (like-for-like is the only mode) |
| #12 hive re-run idempotency | fixed (anti-join on `object_name`) |
| #13 schema re-run lockout | fixed (grant-before-transfer + skip-if-owned) |
| #14 catalog ownership left with SPN | moot (no catalog created) |
| #9 DBFS-root skip cascade | largely dissolves (DBFS-root now migrates) |
| #10 transfer-to-user as non-admin SPN | documented caveat (unchanged) |
