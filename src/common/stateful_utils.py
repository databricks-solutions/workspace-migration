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
