# Workspace Migration

A Databricks Asset Bundle (DAB) that migrates Unity Catalog and legacy Hive
Metastore objects between Databricks workspaces — useful for control-plane
migrations, account consolidations, or moving between Azure regions.

## Coverage

**Unity Catalog**
- Catalogs, schemas, grants
- Managed tables (Delta, Iceberg), external tables
- Views, SQL and Python functions
- Volumes (managed with file-level copy, external via metadata replay)
- Materialized views and streaming tables (SQL-created; DLT-owned
  variants are out of scope — migrate those via pipeline migration)
- Tags, row filters, column masks, ABAC policies
- Lakehouse monitors, registered models (metadata + aliases)
- Connections, foreign catalogs, online tables
- Delta Sharing (shares, recipients, providers; objects include tables,
  views, volumes, schemas, catalogs)

**Legacy Hive Metastore**
- Databases, managed + external tables, views, functions, grants
- Migrated by the standalone `migrate_hive` job (run only when needed)

## Layout

```
.
├── databricks.yml              # Bundle root
├── config.yaml                 # Customer-editable runtime config
├── config.example.yaml         # Reference
├── resources/                  # Workflow + dashboard definitions
├── src/
│   ├── common/                 # auth, catalog_utils, tracking, sql_utils, validation
│   ├── pre_check/              # pre-migration validation
│   ├── discovery/              # inventory source workspace
│   └── migrate/                # per-object-type workers + orchestrator
├── tests/{unit,connect,integration,lint}
├── scripts/                    # CI helpers
└── dashboards/
```

## Usage

Only `config.example.yaml` ships in the repo. `config.yaml` is **git-ignored**
so the source tree never carries environment-specific identifiers (workspace
URLs, SPN client id). Operators **copy the example and fill in real values**:

```bash
cp config.example.yaml config.yaml   # then edit config.yaml with real values
```

`databricks bundle deploy` syncs your local `config.yaml` to the workspace at
`${workspace.file_path}/config.yaml` (DAB syncs working-tree files, so the
git-ignored config still deploys).

> **Don't edit the workspace copy directly.** The workspace copy is a
> mirror of your local copy — any workspace-side edit will be
> overwritten by the next `bundle deploy`. Edit locally and redeploy.

### Required deploy-time variables

`databricks.yml` declares two variables with no baked-in defaults —
operators must supply them for every deploy:

| Variable | Purpose | How to set |
|---|---|---|
| `migration_spn_id` | SPN application ID that jobs run as | `--var migration_spn_id=<app-id>` or env `BUNDLE_VAR_migration_spn_id` |
| `dashboard_warehouse_name` | Name of the SQL warehouse the dashboard reads from (resolved to an ID via lookup). Defaults to `cp-migration` — override if your warehouse has a different name | `--var dashboard_warehouse_name=<name>` or env `BUNDLE_VAR_dashboard_warehouse_name` |

The SPN needs: workspace admin on source + target, metastore-level
`CREATE_*` privileges, `USE_PROVIDER` on target, and
`READ_FILES`/`WRITE_FILES`/`CREATE_EXTERNAL_TABLE` on any external
location used for Hive DBFS-root migration.

#### Terraform / CLI compatibility note

The Databricks CLI ships a bundled Terraform binary whose OpenPGP signing
key has expired, causing `bundle deploy` to fail with
`error downloading Terraform: unable to verify checksums signature: openpgp: key expired`.
Work around by pointing the CLI at a locally-installed Terraform:

```
export DATABRICKS_TF_VERSION=1.5.7          # or your installed version
export DATABRICKS_TF_EXEC_PATH=/path/to/terraform
databricks bundle deploy -t dev
```

### Config reference (`config.yaml`)

| Field | Required | Default | Purpose |
|---|---|---|---|
| `source_workspace_url` | yes | — | Source workspace HTTPS URL |
| `target_workspace_url` | yes | — | Target workspace HTTPS URL |
| `spn_client_id` | yes | — | OAuth SPN application ID (jobs run as this) |
| `spn_secret_scope` | yes | — | Databricks secret scope holding the SPN secret |
| `spn_secret_key` | yes | — | Key within the secret scope |
| `catalog_filter` | no | `[]` (all) | Restrict discovery + migration to named catalogs. List or comma-separated string. |
| `schema_filter` | no | `[]` (all) | Restrict to named schemas within each catalog |
| `tracking_catalog` | no | `migration_tracking` | Catalog holding `discovery_inventory` / `migration_status` / `pre_check_results` |
| `tracking_schema` | no | `cp_migration` | Schema under `tracking_catalog` for the tracking tables |
| `dry_run` | no | `false` | Emit `skipped`/`dry_run` status rows; run no DDL against target |
| `batch_size` | no | `50` | Max objects per batched for-each task (keeps payload under the 3000-byte Databricks Jobs limit) |
| `iceberg_strategy` | no | `""` | `""` skips Iceberg managed tables (marking `skipped_by_config`). `"ddl_replay"` opts into the Option A path — rebuild schema + re-ingest via `cp_migration_share`. Loses snapshot history / time travel / branches + tags. |
| `rls_cm_strategy` | no | `""` | Managed tables carrying legacy row filter / column mask. `""` skips them (marking `skipped_by_rls_cm_policy`). `"staging_copy"` CTAS-copies each affected table into `<tracking_catalog>.cp_migration_staging`, shares the staging copy, DEEP CLONEs on target, then drops the staging copy. Source RLS/CM is never mutated. Requires the migration SPN to be a workspace admin and every active filter/mask to contain an admin-bypass call (`pre_check` enforces both). |
| `on_target_collision` | no | `"fail"` | What to do when a discovered source object has the same FQN as an object already on target AND no `migration_status` row says the tool created it. `"fail"` (default) — pre_check emits a FAIL `check_target_collisions` row and the migrate workflow refuses to start; operator must rename / drop the colliding object and rerun pre_check. `"skip"` — pre_check emits a WARN row and seeds `skipped_target_exists` migration_status rows; workers skip those objects on the next migrate run (target copy left untouched). See [docs/idempotency_audit.md](docs/idempotency_audit.md#collision-handling-x4). |
| `migrate_hive_dbfs_root` | no | `false` | Enables `hive_managed_dbfs_worker` — copies DBFS-root bytes to `hive_dbfs_target_path` and registers the target table as EXTERNAL |
| `hive_dbfs_target_path` | conditional | `""` | ADLS/S3/GCS path where DBFS-root bytes land on target. Required when `migrate_hive_dbfs_root=true`. The SPN needs `READ_FILES`/`WRITE_FILES`/`CREATE_EXTERNAL_TABLE` on the external location that owns this path. |
| `hive_target_catalog` | no | `hive_upgraded` | Target catalog name for Hive-to-UC migration. Created during migrate if missing. |

### Deploy + configure flow

1. Clone this repo
2. `cp config.example.yaml config.yaml` and fill in real values (the file is
   git-ignored, so your real values never get committed):
   - `source_workspace_url` / `target_workspace_url`
   - `spn_client_id` + `spn_secret_scope`/`spn_secret_key` (OAuth service
     principal with access to both workspaces)
   - optional: `catalog_filter`, `schema_filter`, `iceberg_strategy`,
     `migrate_hive_dbfs_root`, `hive_dbfs_target_path`,
     `overwrite_existing` (default false), `transfer_ownership` (default true)
3. `databricks bundle deploy -t dev --var migration_spn_id=<your-app-id>`
   (optionally `--var migration_admin_group=<your-admin-group>` to scope job
   management to a dedicated group instead of the default `admins`). This
   syncs your local `config.yaml` to the workspace along with the workflow
   definitions.
4. Run the `pre_check` workflow to validate connectivity and grants
5. Run `discovery` to inventory source objects
6. Re-run `pre_check` — the second run detects **target collisions**:
   source objects whose FQN already exists on target and isn't tracked
   by the tool. By default (`on_target_collision: fail`) this blocks
   step 7. Either rename / drop the colliding target objects, or flip
   `on_target_collision: skip` to proceed and leave them untouched.
7. Run the `migrate_*` jobs to replay on target — see the
   [Operator flow](#operator-flow) section below for the 4-job model
   and ordering.

To change values later, edit your local `config.yaml` and redeploy —
**don't edit the workspace copy** (it's a mirror and gets overwritten).

### Operator flow

The migration tool ships four production jobs. Run them in this order:

1. **`discovery`** — scans the source workspace and writes
   `discovery_inventory`. The `migrate_*` jobs depend on it
   operationally; run discovery first.
2. **`migrate_uc`** — UC data plane migration: managed/external tables,
   views, volumes, models, online tables, plus UC grants. Runs
   `setup_sharing` → `orchestrator` → workers → `cleanup_staging` →
   `summary_uc`.
3. **`migrate_hive`** — Hive (legacy) data plane migration:
   external/managed tables, functions, views, plus Hive ACLs replayed
   as UC grants on the target catalog.
4. **`migrate_governance`** — fine-grained governance: tags, comments,
   row filters, column masks, customer-defined shares, policies,
   monitors, foreign catalogs, connections.

Each `migrate_*` job is independent and standalone-runnable. They
assume `discovery_inventory` has been populated by an earlier
`discovery` run.

#### Standalone-runnable contract

`migrate_governance` runs standalone (per design Q1 in
[docs/workflow_split_design.md](docs/workflow_split_design.md)). It
assumes target catalog/schema/table/view/volume objects already exist
on target. **Do NOT** run `migrate_governance` against an empty
target — it will write governance state for objects that don't exist.

The `migrate_governance` job's first task `pre_check_governance`
validates only that `discovery_inventory` has governance rows. It
does **not** validate target objects.

#### Pre-conditions

For Path A `staging_copy` strategy (recommended for RLS/CM tables),
see the [Row filter / column mask on managed tables](#row-filter--column-mask-on-managed-tables)
section below.

### Running the integration tests

The integration workflows override the workspace `config.yaml` per-run
(backup before, restore in teardown), so you only need to populate the
environment-specific fields **once** after deploy.

1. `databricks bundle deploy -t dev --var migration_spn_id=<your-app-id>`
2. Edit `${workspace.file_path}/config.yaml` with the environment-specific
   fields once:
   - Real workspace URLs, SPN app ID, secret scope/key
   - `hive_dbfs_target_path: abfss://<container>@<account>.dfs.core.windows.net/<path>`
     (the SPN needs `READ_FILES` + `WRITE_FILES` +
     `CREATE_EXTERNAL_TABLE` on the corresponding external location)
3. Trigger `uc_integration_test` — the first task (`setup_test_config`)
   rewrites the workspace config.yaml with UC-appropriate behavioural
   settings (`iceberg_strategy=ddl_replay`), runs
   seed → pre_check → discovery → `migrate_uc` → test, and
   `teardown_uc` restores the original config.yaml from the backup.
4. Trigger `hive_integration_test` — same pattern, with Hive-appropriate
   settings (`migrate_hive_dbfs_root=true`, `iceberg_strategy=""`),
   running seed → pre_check → discovery → `migrate_hive` → test. Your
   operator-set `hive_dbfs_target_path` from step 2 is preserved —
   workflows don't overwrite env-specific paths, only the behavioural
   settings.
5. Trigger `governance_integration_test` — exercises the standalone
   `migrate_governance` job against fixtures pre-seeded on target.

The per-workflow settings live in each workflow's YAML task parameters;
edit them there if you need to change test behavior.

See [docs/external_hive_metastore.md](docs/external_hive_metastore.md) for
the Hive-specific cluster/init-script reconfiguration checklist.

## Delta Sharing prerequisites

Unity Catalog's Delta Sharing APIs (`shares.list()`, `recipients.list()`,
and `shares.get(..., include_shared_data=True)`) only return objects
where the caller is the **owner** or holds `USE SHARE` / `USE RECIPIENT`.
Objects the caller can't see are silently skipped by the API — no
exception is raised.

The migration SPN connects to both workspaces via OAuth M2M (see
`spn_client_id` in config). For Delta Sharing discovery to find your
customer-defined shares and recipients, the SPN must either:

1. **Own** each share + recipient to be migrated (recommended), or
2. Hold `USE SHARE` on each share and `USE RECIPIENT` on each recipient.

Both can be granted via SQL on the source workspace before running
`discovery`:

```sql
-- Transfer ownership (strongest, implies USE SHARE / USE RECIPIENT):
ALTER SHARE `my_customer_share` OWNER TO `<spn-application-id>`;
ALTER RECIPIENT `my_customer_recipient` OWNER TO `<spn-application-id>`;

-- Or grant the minimum required privileges:
GRANT USE SHARE ON SHARE `my_customer_share` TO `<spn-application-id>`;
GRANT USE RECIPIENT ON RECIPIENT `my_customer_recipient` TO `<spn-application-id>`;
```

If this step is skipped, `discovery` silently omits the share / recipient
from `discovery_inventory`, no migration_status row is written, and the
object won't be recreated on target. `pre_check`'s `check_source_sharing`
only verifies that the SPN can call `shares.list()` at all — not that
any specific customer share is visible.

The migration tool's own internal `cp_migration_share` is created by
`setup_sharing` under the SPN's own identity, so it's always owned by
the SPN and never affected by this.

## Row filter / column mask on managed tables

Delta Sharing providers cannot share tables protected by legacy
row-level security or column masks — i.e. anything applied via
`ALTER TABLE ... SET ROW FILTER` or `ALTER COLUMN ... SET MASK`. The
Delta Sharing API rejects such tables with:

```
InvalidParameterValue: Table <fqn> has row level security or column masks,
which is not supported by Delta Sharing.
```

Because this tool uses Delta Sharing to move managed-table data between
workspaces, affected tables can't flow through the standard path.

### Default behavior (safe skip)

With `rls_cm_strategy: ""` (the default), discovery surfaces a warning
listing the affected tables, and `setup_sharing` excludes them from the
share. `migration_status` records one row per skipped table with
`status = skipped_by_rls_cm_policy` so the skip is auditable from the
dashboard and the test suite. **The skipped tables' data does not move
to target.** Schema and grants on those tables still migrate, but the
table itself arrives on target empty (or doesn't arrive at all,
depending on whether a prior migration created it).

### Your options

1. **Migrate governance to ABAC first.** Delta Sharing *does* support
   sharing tables protected by Unity Catalog ABAC row filter and column
   mask policies (the caller must be exempt from the policy). Rewrite
   the affected tables' RLS/CM as ABAC policies on source before
   running this tool. Recipients can also apply their own ABAC-based
   RLS/CM on the shared tables on target.

2. **Accept the skip** and re-populate the affected tables by other
   means after the migration (e.g. point queries at source during
   cutover, or rebuild from upstream).

3. **Opt into `rls_cm_strategy: staging_copy`** (Path A). For each
   affected table, the tool creates a staging copy in
   `<tracking_catalog>.cp_migration_staging.stg_<sha12>` via
   `CREATE OR REPLACE TABLE ... AS SELECT * FROM <original>`, adds the
   staging FQN to `cp_migration_share`, and `managed_table_worker`
   DEEP CLONEs the staging table on target. Source RLS/CM is **never**
   mutated — there is no maintenance window in which the source is
   unprotected. After `migrate_uc` completes, the `cleanup_staging`
   task (gated on `run_if: ALL_DONE` inside `migrate_uc_workflow.yml`)
   drops the staging tables. The filter / mask itself is reapplied on
   target by `migrate_governance` (`row_filters_worker` /
   `column_masks_worker`) reading from `discovery_inventory`.

   **Pre-conditions** (both enforced by `pre_check`):

   - The migration SPN **must be a workspace admin** on the source
     workspace. The CTAS into staging reads through the source's row
     filter; without admin status, the SPN gets filtered data and the
     staging copy is incomplete.
   - **Every active row filter / column mask function body must
     contain an admin-bypass call** — one of `is_account_group_member(`,
     `is_member(`, or `is_user_in_group(`. Without this, even an admin
     SPN's CTAS returns filtered data.

   `pre_check` validates both invariants before any side-effecting
   work. If either fails, `staging_copy` is rejected and the operator
   must either grant admin status / add the bypass clause, or fall
   back to skip / ABAC.

## Architecture

- All workflows run on serverless compute
- Delta Sharing is used to move managed-table bytes between workspaces
  (`DEEP CLONE` from a share-consumer catalog on target)
- Three Delta tracking tables in `migration_tracking.cp_migration`:
  `discovery_inventory`, `migration_status`, `pre_check_results`
- A Lakeview dashboard surfaces counts, failures, and durations per
  object type

## Support

Databricks does not offer official support for Databricks Solutions and its
repository. For any issue with these assets or the demos installed, please open
an issue using GitHub and the team will have a look on a best-effort basis.

## License

Released under the Databricks License — see [LICENSE.md](LICENSE.md). Third-party
dependency attributions are in [NOTICE.md](NOTICE.md).
