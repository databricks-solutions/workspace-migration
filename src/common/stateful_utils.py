"""Discovery helpers for stateful-service object types.

Separate from CatalogExplorer because these surfaces use the WorkspaceClient
SDK rather than spark.sql catalog traversal, and they are tagged
source_type='stateful' for the future Stateful Services Phase. Each list_*
returns list[dict] carrying the full raw spec under a "definition" key so a
later dependency-analysis step can parse edges without re-fetching.
"""

from __future__ import annotations

from collections.abc import Callable

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

    def list_vector_search_indexes(self) -> list[dict]:
        """VS indexes across all endpoints. list_indexes returns a *mini*
        view without the source table, so fetch the full index via get_index
        (best-effort) to capture the source-table dependency in the spec."""

        def _run() -> list[dict]:
            client = self._client()
            results: list[dict] = []
            for ep in client.vector_search_endpoints.list_endpoints():
                try:
                    indexes = list(client.vector_search_indexes.list_indexes(endpoint_name=ep.name))
                except Exception as exc:  # noqa: BLE001
                    print(f"[stateful][warn] vector search endpoint {ep.name} list_indexes failed — skipping ({exc})")
                    continue
                for idx in indexes:
                    try:
                        full = client.vector_search_indexes.get_index(index_name=idx.name)
                        definition = _as_dict(full)
                    except Exception as exc:  # noqa: BLE001
                        print(f"[stateful][debug] get_index({idx.name}) failed, using mini-view ({exc})")
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

    def list_apps(self) -> list[dict]:
        """Databricks Apps. App.resources carries dependencies (Lakebase,
        SQL warehouse, serving endpoint, secrets) — preserved in definition."""

        def _run() -> list[dict]:
            return [
                {"app_name": a.name, "definition": _as_dict(a)}
                for a in self._client().apps.list()
            ]

        return self._safe("apps", _run)

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
                try:
                    synced = list(client.database.list_synced_database_tables(instance_name=inst.name))
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"[stateful][warn] lakebase instance {inst.name} "
                        f"list_synced_database_tables failed — skipping ({exc})"
                    )
                    continue
                for t in synced:
                    results.append(
                        {
                            "synced_table_name": t.name,
                            "instance_name": inst.name,
                            "definition": _as_dict(t),
                        }
                    )
            return results

        return self._safe("lakebase synced tables", _run)

    def list_model_serving_endpoints(self) -> list[dict]:
        """Model Serving endpoints. config.served_entities carries the served
        model dependency — preserved in definition."""

        def _run() -> list[dict]:
            return [
                {"endpoint_name": e.name, "definition": _as_dict(e)}
                for e in self._client().serving_endpoints.list()
            ]

        return self._safe("serving endpoints", _run)

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
                definition = _as_dict(full)
                # CDC (Tier-2): the ingestion pipeline references a gateway pipeline
                # via ingestion_gateway_id. Nest the gateway's full get()-dict so the
                # worker can recreate the gateway without re-fetching. Skip-and-log on
                # a failed gateway get() — the ingestion row is still useful.
                gw_id = (((definition.get("spec") or {}).get("ingestion_definition")) or {}).get(
                    "ingestion_gateway_id"
                )
                if gw_id:
                    try:
                        definition["gateway_spec"] = _as_dict(client.pipelines.get(gw_id))
                    except Exception as exc:  # noqa: BLE001
                        print(f"[stateful][warn] gateway pipeline {gw_id} get() failed — skipping nest ({exc})")
                results.append(
                    {
                        "pipeline_name": full.name,
                        "pipeline_id": p.pipeline_id,
                        "definition": definition,
                    }
                )
            return results

        return self._safe("lakeflow connect pipelines", _run)
