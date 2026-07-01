# DLT / Lakeflow Declarative Pipeline Migration

**Status:** guidance (no dedicated tool — by design).
**Decision (2026-06-19, empirically validated):** DLT pipelines are migrated with the
**Databricks Terraform exporter**, *not* a worker in this repo. Live end-to-end testing
(source → export → apply → run on target) showed the exporter recreates pipelines with
full configuration fidelity and the migrated pipelines run correctly on the target. A
custom `migrate_dlt` worker would only duplicate the exporter. See
`docs/terraform_exporter_analysis.md` and `docs/stateful_services_phase.md`.

This tool's role for DLT is the **Unity Catalog side only** — the catalogs, schemas, and
tables the pipelines read and write. The exporter does the pipeline definitions and source
files. The operator runs the pipelines at cutover.

## What the exporter does and does not do (validated on the lab)

| Concern | Exporter | Notes |
|---|---|---|
| Pipeline config (`configuration`, `notifications`, `photon`, `channel`, `catalog`/`schema`, serverless/clusters) | ✅ full fidelity | round-tripped export→apply→read-on-target intact |
| Source notebooks + pipeline↔notebook wiring | ✅ | `library { notebook { path = databricks_notebook.…id } }`; TF orders creation |
| Migrated pipeline **runs correctly** on target | ✅ | proven: correct output once input tables exist |
| Inter-pipeline **dependency ordering** | ❌ | dependency lives only in notebook SQL/code; pipelines are independent in HCL |
| Transitive Python imports / non-library workspace files | ❌ with `-listing=dlt` | only the *declared library* notebook is pulled — see step 2 |
| UC catalog / schema / input tables | ❌ | out of the exporter's scope — **this tool migrates them** |
| Pipeline **state / checkpoints** | ❌ never | full refresh required on the new workspace |
| Running / full refresh | ❌ | operator triggers at cutover |

## Prerequisites (before exporting DLT)

1. **Migrate the UC objects the pipelines depend on** using this tool: the target
   catalogs and schemas, plus every input table the pipelines read and (for validation)
   any reference tables. A recreated pipeline cannot run until its input tables exist on
   target. The exporter will **not** create these.
2. Decide the target workspace and have a Databricks auth profile for it
   (e.g. `target-migration`).

## Step-by-step

### 1. Migrate UC objects (this tool)
Run the normal UC migration (`discovery` + `migrate_uc` …) so the catalogs, schemas, and
input tables exist on the target.

### 2. Export from the source — use the FULL asset listing, not `dlt` alone
`-listing=dlt` pulls only the *directly referenced* library notebook. It will **miss**
transitive `import`s of sibling modules and any non-library workspace files, so a Python
pipeline that imports a helper module migrates broken (`ModuleNotFoundError` on the
target — validated). Always include the workspace-asset listings:

```bash
export DATABRICKS_CONFIG_PROFILE=<source-profile>
terraform-provider-databricks exporter -skip-interactive \
  -directory=./dlt_export \
  -listing=dlt,notebooks,wsfiles,repos
```

(The exporter binary ships inside `terraform-provider-databricks`; find it under the
provider plugin cache, e.g.
`~/.../plugins/registry.terraform.io/databricks/databricks/<ver>/.../terraform-provider-databricks_v<ver>`.)

Benign log noise observed and safe to ignore: a `databricks_schema#<catalog>.` read error
and `setting state: Invalid address to set: []string{"url"}` from unrelated resources.

### 3. Point the generated config at the target and trim
- The generated `databricks.tf` provider block is empty (env/profile driven) — set
  `DATABRICKS_CONFIG_PROFILE=<target-profile>` for the apply.
- Optionally remove `groups.tf` (the `databricks_group_member` resources) so the apply
  does not mutate target group membership.

### 4. Apply to the target
```bash
cd dlt_export
export DATABRICKS_CONFIG_PROFILE=<target-profile>
terraform init
terraform apply        # creates directories, notebooks, pipelines
```
This recreates the notebooks and pipelines (and wires them). It does **not** run them.

### 5. Determine the cutover run order (dependency ordering)
The exporter does not capture inter-pipeline dependencies. Derive the order before
cutover:
- **UC pipelines:** query `system.access.table_lineage` on the source —
  `entity_type='PIPELINE'`, `entity_id=<pipeline_id>`, `source_type='TABLE'` gives each
  pipeline's input tables; map inputs to the producing pipeline to build the DAG and
  topologically sort it. (Lineage is UC-only and has ingestion lag; it does not cover
  `hive_metastore`.)
- **Gaps / legacy-HMS pipelines:** operator supplies the order manually.

See the optional helper below.

### 6. Cutover — run in dependency order
At cutover, trigger a **full refresh** of each pipeline in dependency order (roots first).
Each downstream pipeline resolves once its upstream's tables are materialised. For
**continuous** pipelines, run a one-shot refresh to validate, then restart them in
continuous mode. Streaming sources re-read from their configured start position — pipeline
state and checkpoints do not transfer.

### 7. Validate
Confirm the target tables are populated and row counts/transformations look right, and that
each pipeline reaches `COMPLETED`.

## Gotchas observed during testing
- **Transitive deps:** always use the full asset listing (step 2).
- **UC prerequisites:** catalogs/schemas/input tables must be migrated first (step 1).
- **No state transfer:** full refresh only; pre-cutover streaming history bounded by source
  retention; APPLY CHANGES SCD2 history rebuilt only from retained source.
- **Compiled libraries (`whl`/`jar`/Maven):** not covered by this guide's testing — verify
  the artifacts exist on / are reachable from the target before cutover.

## Optional — lineage-based dependency-ordering helper
A small read-only helper (script/notebook, **not** a migration worker) can produce the
cutover run order for UC pipelines:

1. List in-scope DLT pipelines and their output tables (each MV/ST carries its
   `pipeline_id`).
2. Query `system.access.table_lineage` for each pipeline's input tables.
3. Build edges `producer(input) → pipeline`, topologically sort, and emit an ordered
   runbook. Flag any pipeline with no detected edges as "dependencies undetermined" and
   let the operator supply the order.
