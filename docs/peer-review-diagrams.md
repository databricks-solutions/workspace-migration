# Peer Review — Architecture & Workflow at a Glance

Two-minute orientation for a reviewer landing on this repo for the first time.

---

## 1. System view — four jobs + shared tracking

```mermaid
flowchart LR
    subgraph SRC["Source workspace"]
        S_UC[("UC catalogs/schemas/tables<br/>volumes, views, funcs, models")]
        S_HIVE[("Hive metastore")]
    end

    subgraph TGT["Target workspace (tool runs here)"]
        DISC["discovery job<br/><i>scans source via Delta Sharing</i>"]
        UC["migrate_uc job<br/><i>data plane: tables/volumes/views/funcs/grants/models</i>"]
        HIVE["migrate_hive job<br/><i>Hive external + managed tables</i>"]
        GOV["migrate_governance job<br/><i>tags, RLS, column masks, comments,<br/>monitors, connections, fcat, shares</i>"]
        TRACK[("migration_tracking.cp_migration<br/>• discovery_inventory<br/>• migration_status<br/>• pre_check_results")]
    end

    SRC -- "Delta Sharing<br/>(cp_migration_share)" --> DISC
    DISC --> TRACK
    DISC -. "upstream of (operator runs first)" .-> UC
    DISC -. "upstream of" .-> HIVE
    DISC -. "upstream of" .-> GOV
    UC --> TRACK
    HIVE --> TRACK
    GOV --> TRACK
```

**Key contracts**

- `discovery` is the single source-of-truth scan — UC/Hive/Governance workflows all consume `discovery_inventory`.
- `migrate_uc`, `migrate_hive`, `migrate_governance` are **independent jobs**. Operator decides ordering. Governance trusts the operator: target tables must already exist.
- Every worker writes its own per-object row into `migration_status` (status + error_message + run_id). Re-runs filter by `get_pending_objects()`.
- All compute is serverless. Auth via migration SPN (OAuth).

---

## 2. Inside `migrate_uc` — Path A staging-copy + worker chain

```mermaid
flowchart TD
    A["<b>setup_sharing</b><br/>Path A: CTAS RLS/CM tables<br/>into cp_migration_staging<br/>(source RLS/CM stays intact)"]
    B["<b>orchestrator</b><br/>builds for_each batches<br/>by byte size"]

    C1["<b>managed_table_worker</b><br/>DEEP CLONE from share<br/>(via staging for RLS/CM)"]
    C2["<b>external_table_worker</b><br/>DDL replay"]
    C3["<b>volume_worker</b><br/>ALTER SHARE ADD VOLUME<br/>+ target-side dbutils.fs.cp"]

    D["<b>functions_worker</b><br/>SQL + Python UDF DDL"]
    E["<b>views_worker</b>"]

    F1["<b>mv_st_worker</b><br/>📌 hard-exclude<br/>→ skipped_by_stateful_service"]
    F2["<b>grants_worker</b>"]

    G1["<b>models_worker</b><br/>UC registered models<br/>+ aliases + artifacts"]
    G2["<b>online_tables_worker</b><br/>📌 hard-exclude<br/>→ skipped_by_stateful_service"]

    H["<b>cleanup_staging</b><br/><i>run_if: ALL_DONE</i><br/>drops cp_migration_staging tables"]
    I["<b>summary_uc</b><br/>per-workflow report<br/>(UC object types only)"]

    A --> B
    B --> C1
    B --> C2
    B --> C3
    C1 --> D
    C2 --> D
    D --> E
    E --> F1
    E --> F2
    C3 --> F2
    F1 --> F2
    F2 --> G1
    F2 --> G2
    C1 --> H
    C2 --> H
    G1 --> I
    G2 --> I
    H --> I

    classDef skip fill:#fff3cd,stroke:#856404
    class F1,G1,G2 default
    class F1,G2 skip
```

**Reviewer focus areas**

| Area | File | Why it matters |
|---|---|---|
| Path A staging copy | `src/migrate/setup_sharing.py`, `src/migrate/cleanup_staging.py` | RLS/CM tables — source is never stripped. Replaced the old `drop_and_restore` risk class. |
| Worker idempotency | `src/migrate/*_worker.py` + `src/common/tracking.py:get_pending_objects` | Every worker re-runnable; status driven by `migration_status` LEFT JOIN on discovery. |
| Batching | `src/migrate/batching.py` | Byte-size for_each batches; oversize objects emit terminal-failed (post H6 fix). |
| Path A target copy | `src/migrate/target_copy.py` | Share-propagation retry for volumes (PR #51, 2026-05-20). |
| Hard-excluded types | `mv_st_worker.py`, `online_tables_worker.py` | Stateful services → out of scope, deliberate skip status. |

---

## 3. Code layout (where things live)

```mermaid
flowchart LR
    subgraph src["src/"]
        subgraph common["common/ (cross-cutting)"]
            CFG["config.py"]
            AUTH["auth.py"]
            CU["catalog_utils.py"]
            TR["tracking.py<br/><i>migration_status I/O</i>"]
            VAL["validation.py"]
            SQL["sql_utils.py"]
            REG["registry.py"]
        end

        DISC["discovery/<br/>discovery.py"]

        subgraph pc["pre_check/"]
            PC["pre_check.py"]
            PCG["pre_check_governance.py"]
            CD["collision_detection.py"]
        end

        subgraph mig["migrate/"]
            ORCH["orchestrator.py<br/>hive_orchestrator.py<br/><i>build for_each batches</i>"]
            UCW["UC workers (9)<br/>managed_table · external_table<br/>volume · views · functions<br/>mv_st · online_tables<br/>models · grants"]
            HW["Hive workers (6)<br/>external · managed_dbfs<br/>managed_nondbfs · views<br/>functions · grants"]
            GW["Governance workers (9)<br/>tags · row_filters · column_masks<br/>policies · comments · monitors<br/>connections · foreign_catalogs · sharing"]
            SS["Path A scaffolding<br/>setup_sharing.py<br/>cleanup_staging.py<br/>rls_cm.py"]
            HELP["Shared helpers<br/>batching.py · target_copy.py<br/>reconciliation.py · summary.py"]
        end
    end

    UCW -.-> common
    HW -.-> common
    GW -.-> common
    DISC -.-> common
    PC -.-> common
    ORCH -.-> common
    SS -.-> common
    HELP -.-> common
```

---

## Glossary

- **Path A** — staging-copy approach for RLS/CM tables: CTAS into `cp_migration_staging`, share the staging copy, never strip source.
- **UC** vs **Hive** vs **Governance** — three independent production jobs after the 2026-05-07 workflow split (PR #46). No global `migrate_all` umbrella.
- **for_each batches** — Databricks workflow `for_each` task with byte-sized inputs from orchestrator task values.
- **Stateful services** — DLT, Lakebase, Vector Search, Model Serving, Apps, ST, MV, Online Tables. Out of scope; deliberate `skipped_by_stateful_service_migration` status.
