# LFC SaaS row_filter (Tier-1, Salesforce-first) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `migrate_lfc` to migrate Tier-1 **SaaS `row_filter`** connectors (Salesforce first; resolver generalized for GA4/ServiceNow) using the existing clone-history → recreate-with-`row_filter` → unified-view machinery.

**Architecture:** The Tier-1 recreate/clone/view flow is connector-agnostic (already built for query-based). The ONE SaaS-specific difference: the cursor column is **not discoverable from the pipeline spec** (unlike query-based `query_based_connector_config.cursor_columns`). So we add a **per-connector cursor resolver** that picks the cursor from the destination table's actual columns using the connector's documented priority order (Salesforce: `SystemModstamp → LastModifiedDate → CreatedDate → LoginTime`). The worker resolves the cursor, computes the boundary `T = MAX(cursor)`, and sets `row_filter = "<cursor> >= 'T'"` directly on `table_configuration` (NOT under `query_based_connector_config`). A SaaS table with no resolvable cursor full-loads (no filter), documented.

**Tech Stack:** Python, databricks-sdk `pipelines` (`IngestionSourceType.SALESFORCE/GA4_RAW_DATA/SERVICENOW`), DAB bundle, pytest. Reuses `lfc_worker`/`clone_lib`/`sharing_lib`/`build_unified_view_sql`.

---

## File Structure

- `src/migrate/lfc_utils.py` — MODIFY: add `_ROW_FILTER_SAAS_SOURCE_TYPES`, refine `classify_pipeline` (Tier-1 vs Tier-2 SaaS), add `resolve_saas_cursor()`, generalize the recreate-spec builder to set `row_filter` from a worker-supplied per-table cursor (keeps query-based behavior identical).
- `src/migrate/lfc_worker.py` — MODIFY: SaaS Tier-1 branch — resolve cursor from destination-table columns (new `deps.get_columns` seam), compute boundary, set `row_filter`; otherwise reuse the existing clone→recreate→trigger→view path.
- `tests/unit/test_lfc_utils.py` — MODIFY: classifier + resolver + spec-builder tests.
- `tests/unit/test_lfc_worker.py` — MODIFY: SaaS-branch worker tests (cursor resolved, no-cursor full-load).
- `tests/integration/seed_lfc_salesforce.py` — CREATE: stand up a live Salesforce UC connection + LFC pipeline on source, run it so data lands.
- `tests/integration/test_lfc_salesforce.py` — CREATE: assert clone/pipeline/view terminal states + target objects.
- `tests/integration/teardown_lfc_salesforce.py` — CREATE: drop migrated objects + seeded pipeline/connection.
- `resources/integration_tests/lfc_salesforce_integration_test_workflow.yml` — CREATE: seed → discovery → migrate_lfc → assert → teardown.

---

## Task 1: Tier-1 vs Tier-2 SaaS classification

**Files:**
- Modify: `src/migrate/lfc_utils.py`
- Test: `tests/unit/test_lfc_utils.py`

- [ ] **Step 1: Failing test** — add to `tests/unit/test_lfc_utils.py`:

```python
import pytest
from migrate.lfc_utils import classify_pipeline

def _saas_def(source_type):
    return {"spec": {"ingestion_definition": {"source_type": source_type, "connection_name": "c", "objects": []}}}

@pytest.mark.parametrize("st", ["SALESFORCE", "GA4_RAW_DATA", "SERVICENOW"])
def test_row_filter_saas_is_tier1(st):
    assert classify_pipeline(_saas_def(st)) == ("saas", "tier1")

@pytest.mark.parametrize("st", ["WORKDAY_RAAS", "SHAREPOINT", "NETSUITE", "DYNAMICS365"])
def test_non_row_filter_saas_is_tier2(st):
    assert classify_pipeline(_saas_def(st)) == ("saas", "tier2")
```

- [ ] **Step 2: Run, verify it fails** — `tests/unit` → `test_non_row_filter_saas_is_tier2` FAILS (current code returns tier1 for all SaaS).

- [ ] **Step 3: Implement** — in `src/migrate/lfc_utils.py`, add the constant after `_QUERY_BASED_SOURCE_TYPES`:

```python
# SaaS source types that support a settable per-table row_filter on their cursor
# column (Tier 1). Other SaaS connectors (Workday, SharePoint, NetSuite, …) have
# no boundary handle and are Tier 2 (full re-hydrate on recreate).
_ROW_FILTER_SAAS_SOURCE_TYPES = frozenset({"SALESFORCE", "GA4_RAW_DATA", "SERVICENOW"})
```

and change the SaaS branch of `classify_pipeline`:

```python
    if source_type in _QUERY_BASED_SOURCE_TYPES:
        return ("query_based", "tier1")
    if source_type in _ROW_FILTER_SAAS_SOURCE_TYPES:
        return ("saas", "tier1")
    if idef:
        return ("saas", "tier2")
    return ("unknown", "tier2")
```

- [ ] **Step 4: Run tests** — `tests/unit/test_lfc_utils.py` PASSES.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(lfc): tier-1 vs tier-2 SaaS classification (row_filter connectors)"`

---

## Task 2: Per-connector SaaS cursor resolver

**Files:**
- Modify: `src/migrate/lfc_utils.py`
- Test: `tests/unit/test_lfc_utils.py`

- [ ] **Step 1: Failing test**:

```python
from migrate.lfc_utils import resolve_saas_cursor

def test_salesforce_cursor_priority_picks_first_present():
    cols = ["Id", "Name", "CreatedDate", "LastModifiedDate", "SystemModstamp"]
    assert resolve_saas_cursor("SALESFORCE", cols) == "SystemModstamp"

def test_salesforce_cursor_falls_through_to_createddate():
    assert resolve_saas_cursor("SALESFORCE", ["Id", "Name", "CreatedDate"]) == "CreatedDate"

def test_salesforce_cursor_none_when_absent():
    assert resolve_saas_cursor("SALESFORCE", ["Id", "Name"]) is None

def test_servicenow_cursor():
    assert resolve_saas_cursor("SERVICENOW", ["sys_id", "sys_updated_on"]) == "sys_updated_on"

def test_ga4_cursor():
    assert resolve_saas_cursor("GA4_RAW_DATA", ["event_date", "event_name"]) == "event_date"

def test_resolver_case_insensitive_on_columns():
    assert resolve_saas_cursor("SALESFORCE", ["systemmodstamp"]) == "systemmodstamp"

def test_unknown_source_type_returns_none():
    assert resolve_saas_cursor("WORKDAY_RAAS", ["anything"]) is None
```

- [ ] **Step 2: Run, verify it fails** — `resolve_saas_cursor` not defined.

- [ ] **Step 3: Implement** — add to `src/migrate/lfc_utils.py`:

```python
# Documented incremental-cursor priority per Tier-1 SaaS connector. The connector
# auto-selects the cursor and does NOT echo it in the pipeline spec, so we resolve
# it from the destination table's actual columns. Salesforce order per the
# Salesforce ingestion FAQ; row filtering is supported only on these (or Id).
_SAAS_CURSOR_PRIORITY = {
    "SALESFORCE": ["SystemModstamp", "LastModifiedDate", "CreatedDate", "LoginTime"],
    "SERVICENOW": ["sys_updated_on", "sys_created_on"],
    "GA4_RAW_DATA": ["event_date", "event_timestamp"],
}


def resolve_saas_cursor(source_type: str, available_columns: list[str]) -> str | None:
    """Pick the incremental cursor column for a Tier-1 SaaS table.

    SaaS connectors do not expose the resolved cursor in the pipeline spec, so we
    match the connector's documented priority order against the columns that
    actually exist on the (cloned) destination table. Returns the FIRST priority
    column present (preserving its real-cased name), or None when none are present
    (that table then full-loads — no row_filter). Column matching is
    case-insensitive; the returned name is the real column name."""
    priority = _SAAS_CURSOR_PRIORITY.get(str(source_type or "").upper())
    if not priority:
        return None
    by_lower = {c.lower(): c for c in available_columns}
    for cand in priority:
        if cand.lower() in by_lower:
            return by_lower[cand.lower()]
    return None
```

- [ ] **Step 4: Run tests** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(lfc): per-connector SaaS cursor resolver (Salesforce/GA4/ServiceNow)"`

---

## Task 3: Generalize the recreate-spec builder to a worker-supplied cursor

The current `build_query_based_create_spec` reads the cursor from `query_based_connector_config.cursor_columns`. SaaS has no such field, so the worker must pass the resolved cursor in. Generalize to a single connector-agnostic builder driven by a per-table cursor map; query-based behavior stays identical (its worker computes the same map from the spec).

**Files:**
- Modify: `src/migrate/lfc_utils.py`
- Test: `tests/unit/test_lfc_utils.py`

- [ ] **Step 1: Failing test**:

```python
from migrate.lfc_utils import build_recreate_spec

def _one_table_def(source_type, dest="orders", extra_tc=None):
    tc = {"scd_type": "SCD_TYPE_1", "primary_keys": ["Id"]}
    if extra_tc:
        tc.update(extra_tc)
    return {"spec": {"catalog": "bronze", "schema": "sf", "ingestion_definition": {
        "source_type": source_type, "connection_name": "src_sf", "objects": [
        {"table": {"source_table": dest, "destination_catalog": "bronze",
                   "destination_schema": "sf", "destination_table": dest,
                   "table_configuration": tc}}]}}}

def test_saas_spec_sets_row_filter_and_incr_from_cursor_map():
    spec = build_recreate_spec(_one_table_def("SALESFORCE"),
                               target_connection_name="tgt_sf", name="p_migrated",
                               row_filter_by_src={"orders": "SystemModstamp >= '2026-06-10T00:00:00.000Z'"})
    t = spec["ingestion_definition"]["objects"][0]["table"]
    assert t["destination_table"] == "orders_incr"
    assert t["table_configuration"]["row_filter"] == "SystemModstamp >= '2026-06-10T00:00:00.000Z'"
    assert "query_based_connector_config" not in t["table_configuration"]
    assert spec["catalog"] == "bronze" and spec["schema"] == "sf"
    assert spec["ingestion_definition"]["connection_name"] == "tgt_sf"

def test_table_absent_from_map_keeps_canonical_name_no_filter():
    spec = build_recreate_spec(_one_table_def("SALESFORCE"),
                               target_connection_name="tgt_sf", name="p_migrated",
                               row_filter_by_src={})
    t = spec["ingestion_definition"]["objects"][0]["table"]
    assert t["destination_table"] == "orders"
    assert "row_filter" not in t["table_configuration"]
```

- [ ] **Step 2: Run, verify it fails** — `build_recreate_spec` not defined.

- [ ] **Step 3: Implement** — add `build_recreate_spec` and re-express `build_query_based_create_spec` on top of it (no behavior change for query-based):

```python
def build_recreate_spec(
    definition: dict, *, target_connection_name: str, name: str,
    row_filter_by_src: dict[str, str],
) -> dict:
    """Connector-agnostic Tier-1 recreate spec. ``row_filter_by_src`` maps
    source_table -> the full row_filter predicate ("<cursor> >= 'T'"). A table in
    the map gets destination ``<table>_incr`` + that row_filter; a table NOT in the
    map keeps its canonical destination and no filter (full-load). The caller
    supplies the cursor/boundary — query-based reads it from the spec, SaaS resolves
    it via resolve_saas_cursor()."""
    spec = (definition or {}).get("spec") or {}
    idef = copy.deepcopy(_ingestion_def(definition))
    idef["connection_name"] = target_connection_name
    for o in idef.get("objects") or []:
        t = o.get("table") or {}
        tc = t.setdefault("table_configuration", {})
        rf = row_filter_by_src.get(t.get("source_table"))
        if rf:
            t["destination_table"] = f"{t['destination_table']}_incr"
            tc["row_filter"] = rf
    out = {"name": name, "channel": "PREVIEW", "ingestion_definition": idef}
    if spec.get("catalog"):
        out["catalog"] = spec["catalog"]
    if spec.get("schema"):
        out["schema"] = spec["schema"]
    return out


def build_query_based_create_spec(
    definition: dict, *, target_connection_name: str,
    boundaries: dict[str, str], name: str,
) -> dict:
    """Query-based recreate spec: cursor comes from the spec's nested
    cursor_columns. Thin wrapper over build_recreate_spec."""
    row_filter_by_src: dict[str, str] = {}
    for o in _ingestion_def(definition).get("objects") or []:
        t = o.get("table") or {}
        src = t.get("source_table")
        cursors = _cursor_columns(t.get("table_configuration") or {})
        if src in boundaries and cursors:
            row_filter_by_src[src] = f"{cursors[0]} >= '{boundaries[src]}'"
    return build_recreate_spec(definition, target_connection_name=target_connection_name,
                               name=name, row_filter_by_src=row_filter_by_src)
```

- [ ] **Step 4: Run tests** — new tests PASS **and** the existing `test_migrate_query_based_pipeline_happy_path` (asserts query-based `row_filter == "updated_at >= '...'"`) still PASSES. Run `tests/unit/test_lfc_utils.py tests/unit/test_lfc_worker.py`.

- [ ] **Step 5: Commit** — `git commit -am "refactor(lfc): connector-agnostic build_recreate_spec; query-based wraps it"`

---

## Task 4: Worker SaaS Tier-1 branch (resolve cursor → boundary → row_filter → reuse flow)

**Files:**
- Modify: `src/migrate/lfc_worker.py`
- Test: `tests/unit/test_lfc_worker.py`

The worker currently handles only `query_based`. Add a `saas`/`tier1` branch that, per table: gets the destination table's columns (`deps.get_columns`), resolves the cursor, computes the boundary, and builds `row_filter_by_src`. Then it reuses the SAME clone → `build_recreate_spec` → create → trigger → view path. Tables with no resolved cursor are cloned but get no filter (full-load); record a tracked note.

- [ ] **Step 1: Failing tests** — add to `tests/unit/test_lfc_worker.py`:

```python
def _sf_row(dest="orders", source_type="SALESFORCE"):
    DEF = {"spec": {"catalog": "bronze", "schema": "sf", "ingestion_definition": {
        "source_type": source_type, "connection_name": "src_sf", "objects": [
        {"table": {"source_table": dest, "destination_catalog": "bronze",
                   "destination_schema": "sf", "destination_table": dest,
                   "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["Id"]}}}]}}}
    return {"object_name": "sf_orders", "object_type": "lfc_pipeline",
            "metadata_json": json.dumps({"definition": DEF})}

def test_saas_tier1_resolves_cursor_and_sets_row_filter():
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.get_columns.return_value = ["Id", "Name", "SystemModstamp", "CreatedDate"]
    deps.compute_boundary.return_value = "2026-06-10T00:00:00.000Z"
    deps.clone_history.return_value = {"status": "validated"}
    deps.target_table_exists.return_value = True
    deps.create_pipeline.return_value = "pid-1"
    results = migrate_pipeline(_sf_row(), deps=deps, target_connection_name="tgt_sf")
    # cursor resolved + boundary computed against SystemModstamp
    deps.compute_boundary.assert_called_once()
    assert deps.compute_boundary.call_args.args[1] == "SystemModstamp"
    # recreate spec carries the SaaS row_filter
    spec = deps.create_pipeline.call_args.args[0]
    tc = spec["ingestion_definition"]["objects"][0]["table"]["table_configuration"]
    assert tc["row_filter"] == "SystemModstamp >= '2026-06-10T00:00:00.000Z'"
    assert any(r["object_type"] == "lfc_view" and r["status"] == "lfc_view_created" for r in results)

def test_saas_tier1_no_cursor_clones_and_fullloads_no_filter():
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.get_columns.return_value = ["Id", "Name"]   # no SF cursor column
    deps.clone_history.return_value = {"status": "validated"}
    deps.create_pipeline.return_value = "pid-1"
    results = migrate_pipeline(_sf_row(), deps=deps, target_connection_name="tgt_sf")
    deps.compute_boundary.assert_not_called()
    spec = deps.create_pipeline.call_args.args[0]
    tc = spec["ingestion_definition"]["objects"][0]["table"]["table_configuration"]
    assert "row_filter" not in tc
    # a no-cursor table full-loads on the recreated pipeline: no unified view, tracked note
    assert any(r["object_type"] == "lfc_view" and r["status"] == "lfc_view_skipped_no_cursor"
               for r in results)
```

- [ ] **Step 2: Run, verify failure** — `deps.get_columns` path / SaaS branch not implemented; `migrate_pipeline` returns `[]` for SaaS (classify → saas, current code only proceeds for query_based — CONFIRM current early-return) or builds no row_filter.

- [ ] **Step 3: Implement** — in `migrate_pipeline` (`src/migrate/lfc_worker.py`), generalize the kind gate and add the cursor source. Replace the `if kind != "query_based": return []` guard with handling for `("query_based","tier1")` and `("saas","tier1")`; defer cdc/tier2/unknown:

```python
    kind, tier = classify_pipeline(definition)
    if tier != "tier1" or kind not in ("query_based", "saas"):
        logger.info("[lfc] %s is %s/%s — deferred to a later stage.", obj_name, kind, tier)
        return []
    source_type = str(_ingestion_def(definition).get("source_type") or "").upper()
```

Then, where the per-table cursor is determined for cloning + the recreate spec, branch by kind. For each table config `c` with a destination table:
- query_based: cursor = `c["cursor_column"]` (from spec, as today).
- saas: `cursor = resolve_saas_cursor(source_type, deps.get_columns(src_table_fqn))`.

Compute `boundaries[src] = deps.compute_boundary(src_fqn, cursor)` only when `cursor` is set, and clone history when `cursor` is set (a filtered table needs a history floor). Build the recreate spec via `build_recreate_spec(..., row_filter_by_src=...)` where `row_filter_by_src[src] = f"{cursor} >= '{boundaries[src]}'"`. For a SaaS table with NO cursor: still clone? No — a full-loading table needs no history clone (the recreate reloads it); record `lfc_view_skipped_no_cursor` for that table and skip its clone+view.

Concretely, adapt the existing loop so cursor-resolution is kind-aware and the view/clone gating keys off "has cursor" rather than `c.get("cursor_column")`. Keep the existing trigger-then-view (`run_pipeline_and_await`) and SCD-aware `build_unified_view_sql` unchanged.

Add `_import` for the new helpers at top: `from migrate.lfc_utils import (..., resolve_saas_cursor, build_recreate_spec, _ingestion_def)` (or expose a public `ingestion_def`).

- [ ] **Step 4: Wire `get_columns` into live `run()` deps** — add:

```python
    def _get_columns(fqn: str) -> list[str]:
        """Column names of an existing table on the SOURCE workspace (used to
        resolve a SaaS cursor before migration)."""
        res = execute_and_poll(auth, src_wh_id, f"DESCRIBE TABLE {_bt(fqn)}", use_source=True)
        cols = []
        for r in res.get("rows") or []:
            name = r[0] if isinstance(r, (list, tuple)) else r.get("col_name")
            if name and not str(name).startswith("#") and str(name).strip():
                cols.append(name)
        return cols
```

and add `get_columns=_get_columns` to the `deps` namespace.

- [ ] **Step 5: Run tests** — `tests/unit/test_lfc_worker.py` all PASS (query-based tests unchanged, 2 new SaaS tests green).

- [ ] **Step 6: Commit** — `git commit -am "feat(lfc): SaaS tier-1 worker branch (resolve cursor, set row_filter, reuse flow)"`

---

## Task 5: Add `lfc_view_skipped_no_cursor` terminal status

**Files:**
- Modify: `src/common/tracking.py` (terminal status list)
- Test: covered by Task 4 worker tests + existing tracking tests.

- [ ] **Step 1** Locate the LFC terminal-status set (where `lfc_view_created`/`lfc_pipeline_created_incremental` live) and add `lfc_view_skipped_no_cursor`.
- [ ] **Step 2** Run `tests/unit/test_tracking.py` — PASS.
- [ ] **Step 3** Commit — `git commit -am "feat(lfc): add lfc_view_skipped_no_cursor terminal status"`

---

## Task 6: Live Salesforce integration test — seed

**Files:**
- Create: `tests/integration/seed_lfc_salesforce.py`

Stand up a real Salesforce Tier-1 pipeline on the SOURCE workspace. Prereqs (handled here / documented): a Salesforce UC connection (OAuth; the connected app's FIRST auth must be admin — see reference_lfc_salesforce_connected_app_install), pointed at the seeded `lfc-test` dev org.

- [ ] **Step 1** Notebook creates (idempotently) on SOURCE: UC connection `integration_test_salesforce` (type SALESFORCE), catalog `integration_test_lfc_sf` + schema `sf`.
- [ ] **Step 2** Create an LFC Salesforce ingestion pipeline `lfc_it_sf_account` ingesting an object with a cursor (e.g. `Account`, which has `SystemModstamp`), SCD_TYPE_1, PK `Id`, channel PREVIEW, into `integration_test_lfc_sf.sf.account`. Do NOT set a row_filter on the source pipeline (the migration adds it).
- [ ] **Step 3** Run the pipeline; wait for COMPLETED; `dbutils.jobs.taskValues.set` the row count of `account`.
- [ ] **Step 4** Commit.

---

## Task 7: Live Salesforce integration test — assert + teardown + workflow

**Files:**
- Create: `tests/integration/test_lfc_salesforce.py`, `tests/integration/teardown_lfc_salesforce.py`, `resources/integration_tests/lfc_salesforce_integration_test_workflow.yml`

- [ ] **Step 1** `test_lfc_salesforce.py` (mirror `test_lfc.py`): assert migration_status rows — `lfc_table` `account_history` validated, `lfc_pipeline` `lfc_it_sf_account` `lfc_pipeline_created_incremental`, `lfc_view` `account` `lfc_view_created` (accept `lfc_view_pending_forward_ingest` only if target can't ingest). Assert on TARGET: `account_history` count == seeded source count, unified view `account` exists + queryable, recreated pipeline exists. Coverage guard: zero `lfc_*` rows ⇒ RED.
- [ ] **Step 2** `teardown_lfc_salesforce.py`: delete recreated + seed pipelines, drop `integration_test_lfc_sf` catalog, drop the SF connection.
- [ ] **Step 3** Workflow yml (mirror `lfc_integration_test_workflow.yml`): setup_test_config (`lfc_target_connection_name=integration_test_salesforce`, `catalog_filter=integration_test_lfc_sf`) → seed_lfc_salesforce → discovery → migrate_lfc → test_lfc_salesforce → teardown_lfc_salesforce.
- [ ] **Step 4** Deploy (`DATABRICKS_TF_VERSION=1.15.5 DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform databricks bundle deploy -t dev --var migration_spn_id=d0354350-... --profile source-migration`) + run; iterate to GREEN.
- [ ] **Step 5** Commit.

**Note on target ingestion:** the recreated SF pipeline must RUN on the target to materialize `account_incr` (for the view). The target workspace reaches Salesforce over the internet (no NCC PE needed for SaaS), but its UC connection must exist + be admin-authorized on the target metastore. If target ingestion isn't available, the worker defers the view (`lfc_view_pending_forward_ingest`) — the test accepts that, same as the query-based lab.

---

## Self-Review notes
- **Spec coverage:** Tier-1 SaaS (D2/D3/D5/D7) — classification (T1), cursor handle (T2, the SaaS-specific delta), recreate `row_filter` (T3), worker flow reuse (T4), per-table no-cursor full-load (T4/T5), real-resource SF test + coverage guard (T6/T7, D11). ✓
- **Type consistency:** `build_recreate_spec(row_filter_by_src)` used identically by the query-based wrapper and the SaaS worker branch; `resolve_saas_cursor(source_type, columns)` returns real-cased name or None; new status `lfc_view_skipped_no_cursor`. ✓
- **GA4/ServiceNow:** resolver + classifier already include them; only their live integration tests are deferred (separate sources). The Salesforce test is the Tier-1 SaaS coverage for this stage.
