# Stateful-Service Discovery Extension — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the core `discovery` job to enumerate the five stateful-service object types it does not yet capture (Vector Search indexes, Apps, Lakebase instances + synced tables, Model Serving endpoints, LFC ingestion pipelines) into the existing `discovery_inventory`, tagged `source_type='stateful'` + a `capability` subtype, storing each object's raw spec for the future Stateful Services Phase.

**Architecture:** A new `StatefulExplorer` class (`src/common/stateful_utils.py`) wraps the WorkspaceClient SDK calls for the new surfaces, each method returning `list[dict]` with the full raw spec under a `definition` key and a known name key. `discovery.py` gains `_discover_stateful()`, called from `run()`, which converts those dicts into `discovery_inventory` rows tagged `source_type='stateful'` with `capability` in `metadata_json`. The pre-existing `online_table` row is reclassified to `source_type='stateful'`. Discovery-only — no dependency graph, no migration.

**Tech Stack:** Python 3.11, `databricks-sdk` 0.102.0 (`WorkspaceClient`), pytest, ruff. Tests mock `auth_manager.source_client`. Spec: `docs/superpowers/specs/2026-06-03-stateful-discovery-design.md`.

**Verified SDK surface (0.102.0):**
- `source_client.vector_search_endpoints.list_endpoints()` → items with `.name`
- `source_client.vector_search_indexes.list_indexes(endpoint_name=...)` → `MiniVectorIndex(.name)`; `get_index(index_name=...)` → full `VectorIndex` (`.as_dict()`)
- `source_client.apps.list()` → `App(.name, .resources, .as_dict())`
- `source_client.database.list_database_instances()` → `DatabaseInstance(.name, .as_dict())`
- `source_client.database.list_synced_database_tables(instance_name=...)` → `SyncedDatabaseTable(.name, .database_instance_name, .as_dict())`
- `source_client.serving_endpoints.list()` → `ServingEndpoint(.name, .config, .as_dict())`
- `source_client.pipelines.list_pipelines()` → `PipelineStateInfo(.pipeline_id, .name)` (NO spec); `pipelines.get(pipeline_id)` → `GetPipelineResponse(.name, .spec)`; LFC iff `spec.ingestion_definition is not None`

---

## File Structure

- **Create** `src/common/stateful_utils.py` — `StatefulExplorer` class, `CAPABILITY` map, `_as_dict` + `_safe` helpers, six `list_*` methods.
- **Modify** `src/discovery/discovery.py` — add `_discover_stateful()`, call it in `run()`, reclassify the `online_table` row.
- **Create** `tests/unit/test_stateful_utils.py` — unit tests for all six `list_*` and the skip-on-error behaviour.
- **Modify** `tests/unit/test_governance_discovery.py` is NOT touched; instead **create** `tests/unit/test_discover_stateful.py` — unit tests for `_discover_stateful()` row shapes + the `online_table` reclassification pin.
- **Modify** `tests/integration/test_uc_end_to_end.py` — tolerant assertions that any `stateful` rows present are well-formed (zero allowed).

---

## Task 1: Module scaffold — `StatefulExplorer`, `CAPABILITY`, `_safe`

**Files:**
- Create: `src/common/stateful_utils.py`
- Test: `tests/unit/test_stateful_utils.py`

- [ ] **Step 1: Write the failing test**

```python
"""Unit tests for StatefulExplorer (stateful-service discovery helpers)."""

from __future__ import annotations

from unittest.mock import MagicMock

from common.stateful_utils import CAPABILITY, StatefulExplorer


def _sdk_obj(as_dict=None, **attrs):
    """A stand-in for a databricks-sdk dataclass: attributes + .as_dict()."""
    obj = MagicMock()
    for k, v in attrs.items():
        setattr(obj, k, v)
    obj.as_dict.return_value = as_dict if as_dict is not None else dict(attrs)
    return obj


class TestScaffold:
    def test_capability_map_covers_every_stateful_object_type(self):
        assert CAPABILITY == {
            "vector_search_index": "vector",
            "app": "compute",
            "database_instance": "lakebase",
            "synced_table": "lakebase",
            "model_serving_endpoint": "compute",
            "lfc_pipeline": "ingestion",
            "online_table": "online_store",
        }

    def test_safe_returns_empty_and_warns_on_error(self, capsys):
        explorer = StatefulExplorer(MagicMock())

        def _boom():
            raise PermissionError("nope")

        result = explorer._safe("vector search", _boom)
        assert result == []
        out = capsys.readouterr().out
        assert "[stateful][warn]" in out
        assert "vector search" in out

    def test_safe_passes_through_on_success(self):
        explorer = StatefulExplorer(MagicMock())
        assert explorer._safe("x", lambda: [1, 2]) == [1, 2]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'common.stateful_utils'`

- [ ] **Step 3: Write minimal implementation**

```python
"""Discovery helpers for stateful-service object types.

Separate from CatalogExplorer because these surfaces use the WorkspaceClient
SDK rather than spark.sql catalog traversal, and they are tagged
source_type='stateful' for the future Stateful Services Phase. Each list_*
returns list[dict] carrying the full raw spec under a "definition" key so a
later dependency-analysis step can parse edges without re-fetching.
"""

from __future__ import annotations

from typing import Callable

# capability subtype (runtime-state class) per stateful object_type.
CAPABILITY: dict[str, str] = {
    "vector_search_index": "vector",
    "app": "compute",
    "database_instance": "lakebase",
    "synced_table": "lakebase",
    "model_serving_endpoint": "compute",
    "lfc_pipeline": "ingestion",
    "online_table": "online_store",
}


def _as_dict(obj: object) -> dict:
    """Best-effort SDK dataclass -> dict; never raises."""
    try:
        return obj.as_dict()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return {}


class StatefulExplorer:
    """Enumerate stateful-service objects via the source WorkspaceClient."""

    def __init__(self, auth_manager: object) -> None:
        self.auth_manager = auth_manager

    def _client(self):
        return self.auth_manager.source_client  # type: ignore[attr-defined]

    def _safe(self, surface: str, fn: Callable[[], list[dict]]) -> list[dict]:
        """Run *fn*; on preview-not-enabled / permission error log a visible
        warning naming *surface* and return []. Never raises, so one disabled
        surface never aborts the others or the UC/Hive scans."""
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            print(
                f"[stateful][warn] {surface} not enabled or not permitted — "
                f"skipping ({type(exc).__name__}: {exc})"
            )
            return []
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/common/stateful_utils.py tests/unit/test_stateful_utils.py
git commit -m "feat(stateful): StatefulExplorer scaffold + capability map + _safe"
```

---

## Task 2: `list_vector_search_indexes()`

**Files:**
- Modify: `src/common/stateful_utils.py`
- Test: `tests/unit/test_stateful_utils.py`

- [ ] **Step 1: Write the failing test** (append to the test file)

```python
class TestVectorSearch:
    def test_lists_indexes_across_endpoints_with_full_spec(self):
        auth = MagicMock()
        client = auth.source_client
        client.vector_search_endpoints.list_endpoints.return_value = [
            _sdk_obj(name="ep1")
        ]
        client.vector_search_indexes.list_indexes.return_value = [
            _sdk_obj(name="cat.sch.idx")
        ]
        client.vector_search_indexes.get_index.return_value = _sdk_obj(
            as_dict={"name": "cat.sch.idx", "primary_key": "id",
                     "delta_sync_index_spec": {"source_table": "cat.sch.src"}},
            name="cat.sch.idx",
        )

        rows = StatefulExplorer(auth).list_vector_search_indexes()

        assert len(rows) == 1
        assert rows[0]["index_name"] == "cat.sch.idx"
        assert rows[0]["endpoint_name"] == "ep1"
        # full spec captured so the later dep step sees the source table
        assert rows[0]["definition"]["delta_sync_index_spec"]["source_table"] == "cat.sch.src"
        client.vector_search_indexes.list_indexes.assert_called_once_with(endpoint_name="ep1")

    def test_falls_back_to_mini_when_get_index_fails(self):
        auth = MagicMock()
        client = auth.source_client
        client.vector_search_endpoints.list_endpoints.return_value = [_sdk_obj(name="ep1")]
        client.vector_search_indexes.list_indexes.return_value = [
            _sdk_obj(as_dict={"name": "cat.sch.idx"}, name="cat.sch.idx")
        ]
        client.vector_search_indexes.get_index.side_effect = RuntimeError("boom")

        rows = StatefulExplorer(auth).list_vector_search_indexes()
        assert rows[0]["definition"] == {"name": "cat.sch.idx"}

    def test_returns_empty_and_warns_when_vs_not_enabled(self, capsys):
        auth = MagicMock()
        auth.source_client.vector_search_endpoints.list_endpoints.side_effect = Exception("404")
        rows = StatefulExplorer(auth).list_vector_search_indexes()
        assert rows == []
        assert "vector search" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestVectorSearch -v`
Expected: FAIL — `AttributeError: 'StatefulExplorer' object has no attribute 'list_vector_search_indexes'`

- [ ] **Step 3: Write minimal implementation** (add method to `StatefulExplorer`)

```python
    def list_vector_search_indexes(self) -> list[dict]:
        """VS indexes across all endpoints. list_indexes returns a *mini*
        view without the source table, so fetch the full index via get_index
        (best-effort) to capture the source-table dependency in the spec."""

        def _run() -> list[dict]:
            client = self._client()
            results: list[dict] = []
            for ep in client.vector_search_endpoints.list_endpoints():
                for idx in client.vector_search_indexes.list_indexes(endpoint_name=ep.name):
                    try:
                        full = client.vector_search_indexes.get_index(index_name=idx.name)
                        definition = _as_dict(full)
                    except Exception:  # noqa: BLE001
                        definition = _as_dict(idx)
                    results.append(
                        {
                            "index_name": idx.name,
                            "endpoint_name": ep.name,
                            "definition": definition,
                        }
                    )
            return results

        return self._safe("vector search", _run)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestVectorSearch -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/common/stateful_utils.py tests/unit/test_stateful_utils.py
git commit -m "feat(stateful): list_vector_search_indexes with full-spec capture"
```

---

## Task 3: `list_apps()`

**Files:**
- Modify: `src/common/stateful_utils.py`
- Test: `tests/unit/test_stateful_utils.py`

- [ ] **Step 1: Write the failing test**

```python
class TestApps:
    def test_lists_apps_with_full_spec(self):
        auth = MagicMock()
        auth.source_client.apps.list.return_value = [
            _sdk_obj(
                as_dict={"name": "myapp", "resources": [{"database": {"instance_name": "lb1"}}]},
                name="myapp",
            )
        ]
        rows = StatefulExplorer(auth).list_apps()
        assert len(rows) == 1
        assert rows[0]["app_name"] == "myapp"
        # resources (App->Lakebase dep) preserved in the raw spec
        assert rows[0]["definition"]["resources"][0]["database"]["instance_name"] == "lb1"

    def test_returns_empty_and_warns_when_apps_unavailable(self, capsys):
        auth = MagicMock()
        auth.source_client.apps.list.side_effect = Exception("403")
        assert StatefulExplorer(auth).list_apps() == []
        assert "apps" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestApps -v`
Expected: FAIL — no attribute `list_apps`

- [ ] **Step 3: Write minimal implementation**

```python
    def list_apps(self) -> list[dict]:
        """Databricks Apps. App.resources carries dependencies (Lakebase,
        SQL warehouse, serving endpoint, secrets) — preserved in definition."""

        def _run() -> list[dict]:
            return [
                {"app_name": a.name, "definition": _as_dict(a)}
                for a in self._client().apps.list()
            ]

        return self._safe("apps", _run)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestApps -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/common/stateful_utils.py tests/unit/test_stateful_utils.py
git commit -m "feat(stateful): list_apps"
```

---

## Task 4: `list_database_instances()` + `list_synced_tables()` (Lakebase)

**Files:**
- Modify: `src/common/stateful_utils.py`
- Test: `tests/unit/test_stateful_utils.py`

- [ ] **Step 1: Write the failing test**

```python
class TestLakebase:
    def test_lists_database_instances(self):
        auth = MagicMock()
        auth.source_client.database.list_database_instances.return_value = [
            _sdk_obj(as_dict={"name": "lb1", "pg_version": "16"}, name="lb1")
        ]
        rows = StatefulExplorer(auth).list_database_instances()
        assert rows == [{"instance_name": "lb1", "definition": {"name": "lb1", "pg_version": "16"}}]

    def test_lists_synced_tables_per_instance(self):
        auth = MagicMock()
        client = auth.source_client
        client.database.list_database_instances.return_value = [_sdk_obj(name="lb1")]
        client.database.list_synced_database_tables.return_value = [
            _sdk_obj(
                as_dict={"name": "cat.sch.synced", "database_instance_name": "lb1"},
                name="cat.sch.synced",
                database_instance_name="lb1",
            )
        ]
        rows = StatefulExplorer(auth).list_synced_tables()
        assert len(rows) == 1
        assert rows[0]["synced_table_name"] == "cat.sch.synced"
        assert rows[0]["instance_name"] == "lb1"
        assert rows[0]["definition"]["database_instance_name"] == "lb1"
        client.database.list_synced_database_tables.assert_called_once_with(instance_name="lb1")

    def test_lakebase_surfaces_warn_and_empty_when_unavailable(self, capsys):
        auth = MagicMock()
        auth.source_client.database.list_database_instances.side_effect = Exception("404")
        assert StatefulExplorer(auth).list_database_instances() == []
        assert StatefulExplorer(auth).list_synced_tables() == []
        assert "lakebase" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestLakebase -v`
Expected: FAIL — no attribute `list_database_instances`

- [ ] **Step 3: Write minimal implementation**

```python
    def list_database_instances(self) -> list[dict]:
        """Lakebase Postgres instances."""

        def _run() -> list[dict]:
            return [
                {"instance_name": i.name, "definition": _as_dict(i)}
                for i in self._client().database.list_database_instances()
            ]

        return self._safe("lakebase instances", _run)

    def list_synced_tables(self) -> list[dict]:
        """Lakebase synced tables, enumerated per instance. Each row keeps
        instance_name so the later dep step links synced_table -> instance."""

        def _run() -> list[dict]:
            client = self._client()
            results: list[dict] = []
            for inst in client.database.list_database_instances():
                for t in client.database.list_synced_database_tables(instance_name=inst.name):
                    results.append(
                        {
                            "synced_table_name": t.name,
                            "instance_name": inst.name,
                            "definition": _as_dict(t),
                        }
                    )
            return results

        return self._safe("lakebase synced tables", _run)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestLakebase -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/common/stateful_utils.py tests/unit/test_stateful_utils.py
git commit -m "feat(stateful): list_database_instances + list_synced_tables (Lakebase)"
```

---

## Task 5: `list_model_serving_endpoints()`

**Files:**
- Modify: `src/common/stateful_utils.py`
- Test: `tests/unit/test_stateful_utils.py`

- [ ] **Step 1: Write the failing test**

```python
class TestServing:
    def test_lists_serving_endpoints_with_full_config(self):
        auth = MagicMock()
        auth.source_client.serving_endpoints.list.return_value = [
            _sdk_obj(
                as_dict={"name": "ep", "config": {"served_entities": [{"entity_name": "cat.sch.model"}]}},
                name="ep",
            )
        ]
        rows = StatefulExplorer(auth).list_model_serving_endpoints()
        assert len(rows) == 1
        assert rows[0]["endpoint_name"] == "ep"
        # served-model dependency preserved
        assert rows[0]["definition"]["config"]["served_entities"][0]["entity_name"] == "cat.sch.model"

    def test_returns_empty_and_warns_when_unavailable(self, capsys):
        auth = MagicMock()
        auth.source_client.serving_endpoints.list.side_effect = Exception("403")
        assert StatefulExplorer(auth).list_model_serving_endpoints() == []
        assert "serving" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestServing -v`
Expected: FAIL — no attribute `list_model_serving_endpoints`

- [ ] **Step 3: Write minimal implementation**

```python
    def list_model_serving_endpoints(self) -> list[dict]:
        """Model Serving endpoints. config.served_entities carries the served
        model dependency — preserved in definition."""

        def _run() -> list[dict]:
            return [
                {"endpoint_name": e.name, "definition": _as_dict(e)}
                for e in self._client().serving_endpoints.list()
            ]

        return self._safe("serving endpoints", _run)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestServing -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/common/stateful_utils.py tests/unit/test_stateful_utils.py
git commit -m "feat(stateful): list_model_serving_endpoints"
```

---

## Task 6: `list_lfc_pipelines()` (list → get → filter ingestion)

**Files:**
- Modify: `src/common/stateful_utils.py`
- Test: `tests/unit/test_stateful_utils.py`

- [ ] **Step 1: Write the failing test**

```python
class TestLfcPipelines:
    def test_keeps_only_ingestion_pipelines(self):
        auth = MagicMock()
        client = auth.source_client
        client.pipelines.list_pipelines.return_value = [
            _sdk_obj(pipeline_id="p1", name="lfc_one"),
            _sdk_obj(pipeline_id="p2", name="plain_dlt"),
        ]

        def _get(pipeline_id):
            if pipeline_id == "p1":
                spec = MagicMock()
                spec.ingestion_definition = MagicMock()  # present -> LFC
                return _sdk_obj(
                    as_dict={"name": "lfc_one", "spec": {"ingestion_definition": {"connection_name": "sf_conn"}}},
                    name="lfc_one",
                    spec=spec,
                )
            spec = MagicMock()
            spec.ingestion_definition = None  # not LFC
            return _sdk_obj(as_dict={"name": "plain_dlt"}, name="plain_dlt", spec=spec)

        client.pipelines.get.side_effect = _get

        rows = StatefulExplorer(auth).list_lfc_pipelines()
        assert len(rows) == 1
        assert rows[0]["pipeline_name"] == "lfc_one"
        assert rows[0]["pipeline_id"] == "p1"
        assert rows[0]["definition"]["spec"]["ingestion_definition"]["connection_name"] == "sf_conn"

    def test_skips_pipeline_whose_get_fails_without_aborting(self, capsys):
        auth = MagicMock()
        client = auth.source_client
        client.pipelines.list_pipelines.return_value = [
            _sdk_obj(pipeline_id="bad", name="bad"),
            _sdk_obj(pipeline_id="p1", name="lfc_one"),
        ]

        def _get(pipeline_id):
            if pipeline_id == "bad":
                raise RuntimeError("gone")
            spec = MagicMock()
            spec.ingestion_definition = MagicMock()
            return _sdk_obj(as_dict={"name": "lfc_one"}, name="lfc_one", spec=spec)

        client.pipelines.get.side_effect = _get

        rows = StatefulExplorer(auth).list_lfc_pipelines()
        assert [r["pipeline_name"] for r in rows] == ["lfc_one"]
        assert "pipeline bad" in capsys.readouterr().out

    def test_returns_empty_and_warns_when_pipelines_unavailable(self, capsys):
        auth = MagicMock()
        auth.source_client.pipelines.list_pipelines.side_effect = Exception("403")
        assert StatefulExplorer(auth).list_lfc_pipelines() == []
        assert "lakeflow connect" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestLfcPipelines -v`
Expected: FAIL — no attribute `list_lfc_pipelines`

- [ ] **Step 3: Write minimal implementation**

```python
    def list_lfc_pipelines(self) -> list[dict]:
        """Lakeflow Connect ingestion pipelines. list_pipelines() returns no
        spec, so get() each pipeline and keep only those whose spec has an
        ingestion_definition. A single pipeline's get() failing is logged and
        skipped, never aborting the surface."""

        def _run() -> list[dict]:
            client = self._client()
            results: list[dict] = []
            for p in client.pipelines.list_pipelines():
                try:
                    full = client.pipelines.get(p.pipeline_id)
                except Exception as exc:  # noqa: BLE001
                    print(f"[stateful][warn] pipeline {p.pipeline_id} get() failed — skipping ({exc})")
                    continue
                spec = getattr(full, "spec", None)
                if spec is None or getattr(spec, "ingestion_definition", None) is None:
                    continue  # not an LFC ingestion pipeline
                results.append(
                    {
                        "pipeline_name": full.name,
                        "pipeline_id": p.pipeline_id,
                        "definition": _as_dict(full),
                    }
                )
            return results

        return self._safe("lakeflow connect pipelines", _run)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_stateful_utils.py::TestLfcPipelines -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/common/stateful_utils.py tests/unit/test_stateful_utils.py
git commit -m "feat(stateful): list_lfc_pipelines (filter to ingestion_definition)"
```

---

## Task 7: Wire `_discover_stateful()` into discovery + reclassify `online_table`

**Files:**
- Modify: `src/discovery/discovery.py` (imports near line 30-33; add `_discover_stateful`; call in `run()` near line 519-523; edit online_table block at lines 325-336)
- Test: Create `tests/unit/test_discover_stateful.py`

- [ ] **Step 1: Write the failing test**

```python
"""Unit tests for discovery._discover_stateful and online_table reclassification."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import discovery.discovery as disc


def _now():
    return datetime(2026, 6, 3, tzinfo=timezone.utc)


def test_discover_stateful_tags_rows_and_capability():
    stateful = MagicMock()
    stateful.list_vector_search_indexes.return_value = [
        {"index_name": "cat.s.idx", "endpoint_name": "ep", "definition": {"x": 1}}
    ]
    stateful.list_apps.return_value = [{"app_name": "app1", "definition": {"resources": []}}]
    stateful.list_database_instances.return_value = [{"instance_name": "lb1", "definition": {}}]
    stateful.list_synced_tables.return_value = [
        {"synced_table_name": "cat.s.syn", "instance_name": "lb1", "definition": {}}
    ]
    stateful.list_model_serving_endpoints.return_value = [{"endpoint_name": "ep1", "definition": {}}]
    stateful.list_lfc_pipelines.return_value = [
        {"pipeline_name": "lfc", "pipeline_id": "p1", "definition": {}}
    ]

    rows = disc._discover_stateful(MagicMock(), stateful, _now())

    by_type = {r["object_type"]: r for r in rows}
    assert set(by_type) == {
        "vector_search_index", "app", "database_instance",
        "synced_table", "model_serving_endpoint", "lfc_pipeline",
    }
    # every row tagged stateful
    assert all(r["source_type"] == "stateful" for r in rows)
    # object_name pulled from the right key
    assert by_type["vector_search_index"]["object_name"] == "cat.s.idx"
    assert by_type["lfc_pipeline"]["object_name"] == "lfc"
    # capability + raw spec live in metadata_json
    meta = json.loads(by_type["database_instance"]["metadata_json"])
    assert meta["capability"] == "lakebase"
    assert json.loads(by_type["app"]["metadata_json"])["capability"] == "compute"
    assert json.loads(by_type["vector_search_index"]["metadata_json"])["definition"] == {"x": 1}


def test_discover_stateful_empty_when_all_surfaces_empty():
    stateful = MagicMock()
    for m in ("list_vector_search_indexes", "list_apps", "list_database_instances",
              "list_synced_tables", "list_model_serving_endpoints", "list_lfc_pipelines"):
        getattr(stateful, m).return_value = []
    assert disc._discover_stateful(MagicMock(), stateful, _now()) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_discover_stateful.py -v`
Expected: FAIL — `AttributeError: module 'discovery.discovery' has no attribute '_discover_stateful'`

- [ ] **Step 3: Write minimal implementation**

In `src/discovery/discovery.py`, update the import block (currently lines 30-33) to add `StatefulExplorer` and `CAPABILITY`:

```python
from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.stateful_utils import CAPABILITY, StatefulExplorer
from common.tracking import TrackingManager, discovery_row, discovery_schema
```

Add this function (place it after `_discover_hive`, before the `run` cell at line 502):

```python
def _discover_stateful(config, stateful, now) -> list[dict]:
    """Discover stateful-service objects (source_type='stateful').

    Each surface's list_* returns dicts with a name key + a "definition" raw
    spec. We tag every row source_type='stateful' and stash the capability
    subtype alongside the raw spec in metadata_json. Dependency analysis is a
    later step that reads these rows; discovery only enumerates + persists.
    """
    # (object_type, list_fn, name_key)
    surfaces = [
        ("vector_search_index", stateful.list_vector_search_indexes, "index_name"),
        ("app", stateful.list_apps, "app_name"),
        ("database_instance", stateful.list_database_instances, "instance_name"),
        ("synced_table", stateful.list_synced_tables, "synced_table_name"),
        ("model_serving_endpoint", stateful.list_model_serving_endpoints, "endpoint_name"),
        ("lfc_pipeline", stateful.list_lfc_pipelines, "pipeline_name"),
    ]
    rows: list[dict] = []
    for obj_type, list_fn, name_key in surfaces:
        for item in list_fn():
            meta = dict(item)
            meta["capability"] = CAPABILITY[obj_type]
            rows.append(
                discovery_row(
                    source_type="stateful",
                    object_type=obj_type,
                    object_name=item[name_key],
                    catalog_name=None,
                    schema_name=None,
                    discovered_at=now,
                    metadata=meta,
                )
            )
    return rows
```

Reclassify the existing `online_table` block (currently lines 325-336 inside `_discover_uc`) from `source_type="uc"` to `source_type="stateful"` and add the capability to its metadata:

```python
    for ot in explorer.list_online_tables():
        ot_meta = dict(ot)
        ot_meta["capability"] = CAPABILITY["online_table"]
        rows.append(
            discovery_row(
                source_type="stateful",
                object_type="online_table",
                object_name=ot["online_table_fqn"],
                catalog_name=None,
                schema_name=None,
                discovered_at=now,
                metadata=ot_meta,
            )
        )
```

Wire the call into `run()` (currently after the Hive scan at lines 522-523). Add after `inventory.extend(_discover_hive(...))`:

```python
    print("[stateful] Scanning stateful-service surfaces...")
    stateful_explorer = StatefulExplorer(auth)
    inventory.extend(_discover_stateful(config, stateful_explorer, now))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_discover_stateful.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/discovery/discovery.py tests/unit/test_discover_stateful.py
git commit -m "feat(stateful): wire _discover_stateful into discovery + reclassify online_table"
```

---

## Task 8: Tolerant integration assertion for stateful rows

**Files:**
- Modify: `tests/integration/test_uc_end_to_end.py`

Rationale: VS/Apps/Lakebase/serving/LFC mostly require previews or external systems, so we do NOT seed them. This assertion proves discovery runs cleanly with the new surfaces wired in and that any `stateful` rows that DO appear are well-formed — zero rows is a valid no-op (same contract as an empty Hive metastore).

- [ ] **Step 1: Write the failing test** (add a test function; match the file's existing helpers for reading `discovery_inventory` — reuse the same status/inventory DataFrame accessor already used by sibling tests, e.g. `full_status` / the inventory query helper)

```python
def test_stateful_rows_are_wellformed_when_present(discovery_inventory_df):
    """Stateful-service rows are optional (preview-gated) but, when present,
    must be tagged source_type='stateful' with a known capability + object_type."""
    import json

    valid_caps = {"vector", "lakebase", "online_store", "compute", "ingestion"}
    valid_types = {
        "vector_search_index", "app", "database_instance", "synced_table",
        "model_serving_endpoint", "lfc_pipeline", "online_table",
    }
    stateful = [r for r in discovery_inventory_df.collect() if r["source_type"] == "stateful"]
    for r in stateful:
        assert r["object_type"] in valid_types
        assert r["object_name"]
        meta = json.loads(r["metadata_json"])
        assert meta["capability"] in valid_caps
    # zero stateful rows is acceptable (no previews enabled in the test workspace)
```

> NOTE: `discovery_inventory_df` here stands for whatever fixture/accessor the
> file already uses to load `discovery_inventory` (the implementer must reuse
> the existing one rather than introduce a new fixture — grep the file for how
> sibling tests read the inventory table). If sibling tests read it inline,
> inline the same read here instead of adding a fixture.

- [ ] **Step 2: Run test to verify it fails (or skips without infra)**

Run: `.venv/bin/python -m pytest tests/integration/test_uc_end_to_end.py::test_stateful_rows_are_wellformed_when_present -v`
Expected: Without a live workspace + prior discovery run, this errors/skips like the other integration tests (they are gated on a deployed workspace). The unit suite is the gate that must pass in CI.

- [ ] **Step 3: Implementation** — none beyond Task 7; this test only reads existing output.

- [ ] **Step 4: Verify shape locally**

Run: `.venv/bin/python -m pytest tests/integration/test_uc_end_to_end.py -k stateful --collect-only`
Expected: the test is collected without import/syntax errors.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_uc_end_to_end.py
git commit -m "test(stateful): tolerant integration assertion for stateful inventory rows"
```

---

## Task 9: Full suite + lint + open PR

- [ ] **Step 1: Run the full unit suite**

Run: `.venv/bin/python -m pytest tests/unit -q`
Expected: PASS — prior count (per backlog ~791) + the new `test_stateful_utils.py` and `test_discover_stateful.py` tests, zero failures.

- [ ] **Step 2: Lint**

Run: `.venv/bin/ruff check src/common/stateful_utils.py src/discovery/discovery.py tests/unit/test_stateful_utils.py tests/unit/test_discover_stateful.py`
Expected: no errors. (Repo uses ruff; match existing `# noqa` conventions for broad `except Exception`.)

- [ ] **Step 3: Commit any lint fixups**

```bash
git add -A && git commit -m "chore(stateful): lint fixups" || echo "nothing to fix"
```

- [ ] **Step 4: Push + open PR**

```bash
git push -u origin feat/stateful-discovery
gh pr create --title "feat: stateful-service discovery extension" \
  --body "Extends the core discovery job to capture the 5 missing stateful-service surfaces (Vector Search indexes, Apps, Lakebase instances + synced tables, Model Serving endpoints, LFC ingestion pipelines) into discovery_inventory, tagged source_type='stateful' + capability subtype. Reclassifies online_table to source_type='stateful'. Discovery-only — dependency analysis and migration are later specs. Spec: docs/superpowers/specs/2026-06-03-stateful-discovery-design.md.

This pull request and its description were written by Isaac."
```

> Ask the user which merge strategy before merging (do not default to squash), and pass `--delete-branch`.

---

## Self-Review

**Spec coverage:**
- 5 missing surfaces → Tasks 2–6. ✓
- online_table reclassification → Task 7. ✓
- source_type='stateful' + capability taxonomy (vector/lakebase/online_store/compute/ingestion) → Task 1 (`CAPABILITY`) + Task 7. ✓
- Raw spec in metadata_json (defer deps) → every list_* stores `definition`; Task 7 nests it under metadata. ✓
- StatefulExplorer in src/common/stateful_utils.py → Task 1. ✓
- Louder error handling (warn, not silent []) → `_safe` (Task 1) + per-surface tests. ✓
- Unit + integration tests → Tasks 1–6, 7, 8. ✓
- No discovery_schema change → confirmed; capability rides metadata. ✓

**Placeholder scan:** Task 8 intentionally references the file's existing inventory accessor rather than inventing one, with an explicit instruction to reuse it — this is a real constraint, not a placeholder. All code steps contain full code.

**Type consistency:** name keys are consistent between each list_* (Tasks 2–6) and the `surfaces` table in `_discover_stateful` (Task 7): index_name, app_name, instance_name, synced_table_name, endpoint_name, pipeline_name. `CAPABILITY` keys match the object_type strings used in Task 7. `_safe`/`_as_dict`/`_client` defined in Task 1 and used unchanged thereafter.
