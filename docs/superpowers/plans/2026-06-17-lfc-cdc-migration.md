# LFC CDC (Tier-2, SQL Server) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Migrate CDC/gateway-based LFC pipelines (SQL Server) — recreate the gateway(s) + ingestion pipeline(s) on the target, mirroring source topology, **create + dry-validate but do NOT start**. Spec: `docs/superpowers/specs/2026-06-17-lfc-cdc-migration-design.md`.

**Architecture:** A CDC ingestion-pipeline row carries its gateway spec **nested** (discovery follows `ingestion_gateway_id` → embeds the `gateway_definition`). The worker recreates each **unique** gateway once via a **shared `gateway_id_map`** (source-gateway-id → new-target-gateway-id) threaded across rows in `run()`, then recreates each ingestion pipeline pointing at its mapped new gateway (full-reload, no `row_filter`), and runs `start_update(validate_only=True)` on each. No clone, no view, no real run. Discovery also **tags the gateway staging volume excluded** so the volume worker skips it.

**Tech stack:** Python, databricks-sdk `pipelines` (`IngestionGatewayPipelineDefinition`, `IngestionPipelineDefinition`, `start_update(validate_only=True)`), DAB, pytest. Reuses `lfc_worker`/`lfc_utils`/discovery.

**Live-validation items (confirm against the live SDK/workspace during Tasks 6–8, like the query-based `pipelines.create` shape):**
- Exact gateway `pipelines.create` shape (likely `pipelines.create(name=, gateway_definition=IngestionGatewayPipelineDefinition.from_dict(...))`, no top-level catalog/schema).
- `validate_only=True` behavior for a gateway vs an ingestion pipeline, and whether the ingestion validate depends on the gateway. Treat validation as **record-and-assert; hard-fail only on a genuine config-resolution error**.

---

## Task 1: Terminal statuses

**Files:** Modify `src/common/tracking.py`; Test `tests/unit/test_tracking.py`.

- [ ] **Step 1: Failing test** — add to `tests/unit/test_tracking.py` (mirror the existing terminal-status assertion):
```python
def test_cdc_statuses_are_terminal():
    from common.tracking import _TERMINAL_STATUSES
    for s in ("lfc_gateway_created", "lfc_pipeline_created_fullreload", "lfc_pipeline_validated"):
        assert s in _TERMINAL_STATUSES
```
- [ ] **Step 2: Run** → FAIL. `.venv/bin/python -m pytest tests/unit/test_tracking.py -q`
- [ ] **Step 3: Implement** — add `"lfc_gateway_created"`, `"lfc_pipeline_created_fullreload"`, `"lfc_pipeline_validated"` to `_TERMINAL_STATUSES` (and update the hard-coded SQL-string assertions in `test_tracking.py` if present, as the SaaS stage did).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(lfc): CDC terminal statuses" ` (+ trailing `Co-authored-by: Isaac`).

---

## Task 2: `extract_gateway_def` + gateway staging volume FQN (pure, lfc_utils)

**Files:** Modify `src/migrate/lfc_utils.py`; Test `tests/unit/test_lfc_utils.py`.

The discovered CDC ingestion row's `definition` has its gateway spec embedded at `definition["gateway_spec"]` (a full pipeline get() dict whose `spec.gateway_definition` holds the gateway). Provide pure accessors.

- [ ] **Step 1: Failing tests**:
```python
from migrate.lfc_utils import extract_gateway_def, gateway_staging_volume_fqn

_GW_SPEC = {"spec": {"gateway_definition": {
    "connection_name": "src_sql", "gateway_storage_catalog": "stg",
    "gateway_storage_schema": "cdc", "gateway_storage_name": "gw_vol"}}}

def test_extract_gateway_def():
    assert extract_gateway_def(_GW_SPEC)["gateway_storage_name"] == "gw_vol"

def test_extract_gateway_def_none_when_absent():
    assert extract_gateway_def({"spec": {"ingestion_definition": {}}}) is None

def test_gateway_staging_volume_fqn():
    assert gateway_staging_volume_fqn(extract_gateway_def(_GW_SPEC)) == "stg.cdc.gw_vol"

def test_gateway_staging_volume_fqn_none_on_incomplete():
    assert gateway_staging_volume_fqn({"gateway_storage_catalog": "stg"}) is None
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** in `lfc_utils.py`:
```python
def extract_gateway_def(gateway_spec: dict) -> dict | None:
    """The gateway_definition from a gateway pipeline's get()-dict, or None."""
    return ((gateway_spec or {}).get("spec") or {}).get("gateway_definition") or None


def gateway_staging_volume_fqn(gateway_def: dict) -> str | None:
    """`cat.schema.volume` of the gateway's staging volume, or None if incomplete."""
    gd = gateway_def or {}
    cat, sch, vol = gd.get("gateway_storage_catalog"), gd.get("gateway_storage_schema"), gd.get("gateway_storage_name")
    return f"{cat}.{sch}.{vol}" if (cat and sch and vol) else None
```
- [ ] **Step 4: Run** → PASS. **Step 5: Commit.**

---

## Task 3: `build_gateway_recreate_spec` (pure, lfc_utils)

**Files:** Modify `src/migrate/lfc_utils.py`; Test `tests/unit/test_lfc_utils.py`.

- [ ] **Step 1: Failing test**:
```python
from migrate.lfc_utils import build_gateway_recreate_spec

def test_build_gateway_recreate_spec():
    spec = build_gateway_recreate_spec(
        extract_gateway_def(_GW_SPEC), target_connection_name="tgt_sql", name="gw_migrated")
    assert spec["name"] == "gw_migrated"
    gd = spec["gateway_definition"]
    assert gd["connection_name"] == "tgt_sql"          # remapped to target connection
    # storage location mirrored from source
    assert gd["gateway_storage_catalog"] == "stg"
    assert gd["gateway_storage_schema"] == "cdc"
    assert gd["gateway_storage_name"] == "gw_vol"
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement**:
```python
def build_gateway_recreate_spec(gateway_def: dict, *, target_connection_name: str, name: str) -> dict:
    """pipelines.create spec for the recreated ingestion gateway: target connection,
    staging storage location mirrored from source. The gateway creates its own
    staging volume at that location on the target."""
    gd = copy.deepcopy(gateway_def or {})
    gd["connection_name"] = target_connection_name
    gd.pop("connection_id", None)  # source id; let the connection_name resolve on target
    return {"name": name, "gateway_definition": gd}
```
- [ ] **Step 4: Run** → PASS. **Step 5: Commit.**

---

## Task 4: `build_cdc_ingestion_recreate_spec` (pure, lfc_utils)

**Files:** Modify `src/migrate/lfc_utils.py`; Test `tests/unit/test_lfc_utils.py`.

Recreate the CDC ingestion pipeline: remap `ingestion_gateway_id` to the new target gateway id, full-reload (no `row_filter`), top-level catalog/schema carried (direct publishing).

- [ ] **Step 1: Failing test**:
```python
from migrate.lfc_utils import build_cdc_ingestion_recreate_spec

_CDC_ING = {"spec": {"catalog": "bronze", "schema": "cdc", "ingestion_definition": {
    "ingestion_gateway_id": "src-gw-1", "objects": [
    {"table": {"source_schema": "dbo", "source_table": "orders",
               "destination_catalog": "bronze", "destination_schema": "cdc", "destination_table": "orders",
               "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["id"]}}}]}}}

def test_build_cdc_ingestion_recreate_spec():
    spec = build_cdc_ingestion_recreate_spec(
        {"spec": _CDC_ING["spec"]}, target_gateway_id="tgt-gw-9", name="ing_migrated")
    idef = spec["ingestion_definition"]
    assert idef["ingestion_gateway_id"] == "tgt-gw-9"     # remapped
    assert spec["catalog"] == "bronze" and spec["schema"] == "cdc"
    tc = idef["objects"][0]["table"]["table_configuration"]
    assert "row_filter" not in tc                          # full reload, no boundary
    assert spec["name"] == "ing_migrated"
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement**:
```python
def build_cdc_ingestion_recreate_spec(definition: dict, *, target_gateway_id: str, name: str) -> dict:
    """pipelines.create spec for the recreated CDC ingestion pipeline: gateway id
    remapped to the new target gateway, NO row_filter (full re-hydrate)."""
    spec = (definition or {}).get("spec") or {}
    idef = copy.deepcopy(_ingestion_def(definition))
    idef["ingestion_gateway_id"] = target_gateway_id
    out = {"name": name, "ingestion_definition": idef}
    if spec.get("catalog"):
        out["catalog"] = spec["catalog"]
    if spec.get("schema"):
        out["schema"] = spec["schema"]
    return out
```
- [ ] **Step 4: Run** → PASS. **Step 5: Commit.**

---

## Task 5: Worker CDC branch (gateway-first via shared map, recreate ingestion, validate)

**Files:** Modify `src/migrate/lfc_worker.py`; Test `tests/unit/test_lfc_worker.py`.

`migrate_pipeline` currently defers `tier != "tier1"`. Add CDC handling. Signature gains `gateway_id_map: dict[str,str] | None = None` (shared across rows by `run()` so a gateway shared by N pipelines is recreated once).

- [ ] **Step 1: Failing tests** (add to `tests/unit/test_lfc_worker.py`):
```python
def _cdc_row(name="cdc_orders", gw_id="src-gw-1"):
    d = {"spec": {"catalog": "bronze", "schema": "cdc", "ingestion_definition": {
        "ingestion_gateway_id": gw_id, "objects": [
        {"table": {"source_schema": "dbo", "source_table": "orders",
                   "destination_catalog": "bronze", "destination_schema": "cdc", "destination_table": "orders",
                   "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["id"]}}}]}},
         "gateway_spec": {"spec": {"gateway_definition": {
             "connection_name": "src_sql", "gateway_storage_catalog": "stg",
             "gateway_storage_schema": "cdc", "gateway_storage_name": "gw_vol", "id": gw_id}}}}
    return {"object_name": name, "object_type": "lfc_pipeline",
            "metadata_json": json.dumps({"definition": d})}

def test_cdc_recreates_gateway_then_ingestion_and_validates():
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.create_gateway.return_value = "tgt-gw-9"
    deps.create_pipeline.return_value = "tgt-ing-9"
    gmap = {}
    results = migrate_pipeline(_cdc_row(), deps=deps, target_connection_name="tgt_sql", gateway_id_map=gmap)
    deps.create_gateway.assert_called_once()                       # gateway recreated
    assert gmap["src-gw-1"] == "tgt-gw-9"                           # mapping cached
    spec = deps.create_pipeline.call_args.args[0]
    assert spec["ingestion_definition"]["ingestion_gateway_id"] == "tgt-gw-9"   # remapped
    deps.validate_pipeline.assert_called()                          # dry-validate run
    assert any(r["object_type"] == "lfc_gateway" and r["status"] == "lfc_gateway_created" for r in results)
    assert any(r["status"] == "lfc_pipeline_created_fullreload" for r in results)
    # NOT started:
    deps.run_pipeline_and_await.assert_not_called()

def test_cdc_shared_gateway_recreated_once():
    deps = MagicMock()
    deps.target_view_exists.return_value = False
    deps.create_gateway.return_value = "tgt-gw-9"
    deps.create_pipeline.side_effect = ["i1", "i2"]
    gmap = {}
    migrate_pipeline(_cdc_row("cdc_a"), deps=deps, target_connection_name="t", gateway_id_map=gmap)
    migrate_pipeline(_cdc_row("cdc_b"), deps=deps, target_connection_name="t", gateway_id_map=gmap)
    deps.create_gateway.assert_called_once()    # same source gateway → recreated once
    assert deps.create_pipeline.call_count == 2  # both ingestion pipelines recreated
```
- [ ] **Step 2: Run** → FAIL (CDC currently returns `[]`).
- [ ] **Step 3: Implement** — in `migrate_pipeline`, before the tier-1 gate, branch on `kind == "cdc"`:
```python
    if kind == "cdc":
        return _migrate_cdc(row, definition, deps=deps, target_connection_name=target_connection_name,
                            gateway_id_map=gateway_id_map if gateway_id_map is not None else {})
```
Add `_migrate_cdc(...)`:
```python
def _migrate_cdc(row, definition, *, deps, target_connection_name, gateway_id_map):
    from migrate.lfc_utils import (build_cdc_ingestion_recreate_spec, build_gateway_recreate_spec,
                                   extract_gateway_def)
    obj = row["object_name"]
    results = []
    src_gw_id = (_ingestion_def(definition).get("ingestion_gateway_id"))
    gw_def = extract_gateway_def((definition or {}).get("gateway_spec") or {})
    if not src_gw_id or not gw_def:
        return [{"object_name": obj, "object_type": "lfc_pipeline", "status": "failed",
                 "error_message": "CDC pipeline missing gateway id / nested gateway_spec"}]
    # Gateway-first: recreate each unique source gateway once.
    tgt_gw_id = gateway_id_map.get(src_gw_id)
    if tgt_gw_id is None:
        gw_name = f"{(gw_def.get('gateway_storage_name') or 'gateway')}_migrated"
        gw_spec = build_gateway_recreate_spec(gw_def, target_connection_name=target_connection_name, name=gw_name)
        tgt_gw_id = deps.create_gateway(gw_spec)
        gateway_id_map[src_gw_id] = tgt_gw_id
        deps.validate_pipeline(tgt_gw_id)   # dry-validate; record-and-continue
        results.append({"object_name": gw_name, "object_type": "lfc_gateway",
                        "status": "lfc_gateway_created", "error_message": None})
    ing_spec = build_cdc_ingestion_recreate_spec(definition, target_gateway_id=tgt_gw_id, name=f"{obj}_migrated")
    tgt_ing_id = deps.create_pipeline(ing_spec)
    deps.validate_pipeline(tgt_ing_id)
    results.append({"object_name": obj, "object_type": "lfc_pipeline",
                    "status": "lfc_pipeline_created_fullreload", "error_message": None})
    return results
```
(Idempotency: if the target gateway/pipeline already exist by name, the live `deps.create_gateway`/`create_pipeline` should detect + return the existing id and the worker records `skipped_target_pipeline_exists` — refine in the live wiring, Task 6.)
- [ ] **Step 4** Add the `gateway_id_map` param to `migrate_pipeline`'s signature (default `None`). Run unit tests → PASS; query-based + SaaS tests unchanged.
- [ ] **Step 5: Commit.**

---

## Task 6: Live wiring in `run()` — deps + shared map

**Files:** Modify `src/migrate/lfc_worker.py`.

- [ ] **Step 1** Add deps:
```python
    def _create_gateway(spec):
        from databricks.sdk.service.pipelines import IngestionGatewayPipelineDefinition
        created = auth.target_client.pipelines.create(
            name=spec["name"],
            gateway_definition=IngestionGatewayPipelineDefinition.from_dict(spec["gateway_definition"]),
        )                                      # LIVE-VALIDATION: confirm shape (no top-level catalog/schema)
        return created.pipeline_id

    def _validate_pipeline(pipeline_id):
        """Dry-validate: analysis-only update, no data, no continuous run. Record-and-return;
        the caller decides hard-fail only on a genuine config error."""
        try:
            auth.target_client.pipelines.start_update(pipeline_id, validate_only=True)  # LIVE-VALIDATION
        except Exception as exc:  # noqa: BLE001
            logger.warning("[lfc] validate_only(%s) failed: %s", pipeline_id, exc)
```
Add `create_gateway=_create_gateway, validate_pipeline=_validate_pipeline` to `deps`.
- [ ] **Step 2** Thread a shared map through the row loop:
```python
    gateway_id_map: dict[str, str] = {}
    for row in rows:
        ... migrate_pipeline(row, deps=deps, target_connection_name=target_connection_name,
                             saas_cursor_columns=saas_cursor_columns, gateway_id_map=gateway_id_map)
```
- [ ] **Step 3** Run `tests/unit` + `ruff check src tests` → green. **Commit.**

---

## Task 7: Discovery — nest gateway spec + exclude staging volume

**Files:** Modify `src/common/stateful_utils.py`, `src/discovery/discovery.py`; Test `tests/unit/test_discovery*.py` (or the stateful discovery test).

- [ ] **Step 1** In `StatefulExplorer.list_lfc_pipelines`: for an ingestion pipeline whose `ingestion_definition.ingestion_gateway_id` is set, `get()` the gateway pipeline and embed it: `result["definition"]["gateway_spec"] = _as_dict(gateway_full)`. (Skip-and-log if the gateway get() fails.)
- [ ] **Step 2** In `discovery.py` `run()` (after `_discover_stateful`, before the single `write_discovery_inventory`): collect gateway staging volume FQNs from the CDC rows (`gateway_staging_volume_fqn(extract_gateway_def(row...gateway_spec))`), and for each matching volume row in `inventory` set `object_type = "gateway_staging_volume"` (excluded from the volume worker's `object_type=volume` slice). Print a line per exclusion.
- [ ] **Step 3** Unit test the reconcile helper (pure): given volume rows + gateway storage FQNs, the matching volume row is retagged, others untouched. Add a `_exclude_gateway_staging_volumes(inventory, gateway_fqns)` pure helper in `discovery.py` (or `lfc_utils.py`) and test it.
- [ ] **Step 4** Run `tests/unit` + ruff → green. **Commit.**

---

## Task 8: Live integration test (SQL Server + Change Tracking)

**Files:** Create `tests/integration/seed_lfc_cdc.py`, `tests/integration/test_lfc_cdc.py`, `tests/integration/teardown_lfc_cdc.py`, `resources/integration_tests/lfc_cdc_integration_test_workflow.yml`. Infra: extend `infra/azure-sql-test` to enable Change Tracking.

- [ ] **Step 1: Enable Change Tracking on the source SQL DB + a table** (TF `azurerm_mssql_*` or a run-command / sqlcmd step): `ALTER DATABASE ... SET CHANGE_TRACKING = ON (...)` + `ALTER TABLE dbo.<t> ENABLE CHANGE_TRACKING`. (No SQL Agent needed for CT.)
- [ ] **Step 2: `seed_lfc_cdc.py`** — verify the `integration_test_sqlserver` connection exists on source+target; create a **gateway** pipeline (`gateway_definition`: connection + a staging catalog/schema/volume) + an **ingestion** pipeline (`ingestion_gateway_id` → the gateway, dest `integration_test_lfc_cdc.cdc.<t>`); start the gateway continuous + run ingestion so the source side lands rows; emit source counts. Idempotent (delete+recreate by name).
- [ ] **Step 3: `test_lfc_cdc.py`** — assert migration_status: `lfc_gateway_created`, `lfc_pipeline_created_fullreload`; the recreated **target** gateway + ingestion pipeline exist with the gateway id remapped + target connection + mirrored storage; the **dry-validate** result recorded (validated); the **gateway staging volume excluded** (tagged `gateway_staging_volume` in discovery_inventory / not migrated as a volume). Coverage guard: zero CDC rows ⇒ RED. **Do NOT** assert target destination-table data (pipelines not started — D2).
- [ ] **Step 4: `teardown_lfc_cdc.py`** — delete recreated + seed gateway/ingestion pipelines; drop the CDC catalog cascade; leave SQL server + connection.
- [ ] **Step 5: workflow yml** — `setup_test_config` (`catalog_filter=integration_test_lfc_cdc`, `lfc_target_connection_name=integration_test_sqlserver`) → seed → discovery → migrate_lfc → test → teardown.
- [ ] **Step 6** Deploy (`DATABRICKS_TF_VERSION=1.15.5 DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform databricks bundle deploy -t dev --var migration_spn_id=d0354350-... --profile source-migration`) + run; drive to GREEN, confirming the LIVE-VALIDATION items (gateway create shape, validate_only behavior).
- [ ] **Step 7: Commit.**

---

## Self-review
- **Spec coverage:** D1 (Tier-2 full-reload, no clone/view) → Tasks 4/5; D2 (create + validate, don't start) → Tasks 5/6 + test asserts no-data; D3 (mirror topology, shared gateway once) → Task 5 shared map + test; D4 (exclude staging volume) → Tasks 2/7; D5 (reuse `lfc_target_connection_name`) → Tasks 3/6; statuses → Task 1. ✓
- **Type consistency:** `gateway_id_map: dict[str,str]` threaded run()→migrate_pipeline→_migrate_cdc; `build_gateway_recreate_spec`/`build_cdc_ingestion_recreate_spec`/`extract_gateway_def`/`gateway_staging_volume_fqn` signatures match their call sites. ✓
- **Live-unknowns** isolated to Tasks 6/8 and flagged (gateway create shape, `validate_only` semantics) — same approach that worked for the query-based stage.
- **No data-landing assertion** in the test is intentional (D2), and called out so it doesn't read as a coverage gap.
