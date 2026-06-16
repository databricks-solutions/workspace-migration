"""Pure transformation helpers for migrate_lfc (no side effects, unit-tested).

Stage 1 supports query-based connectors only. CDC and SaaS classify here but
are handled by later stages.
"""
from __future__ import annotations


def _ingestion_def(definition: dict) -> dict:
    return ((definition or {}).get("spec") or {}).get("ingestion_definition") or {}


def classify_pipeline(definition: dict) -> tuple[str, str]:
    """Return (connector_kind, tier).

    connector_kind in {"query_based","cdc","saas","unknown"}; tier in {"tier1","tier2"}.
    Query-based: ingestion_definition, no gateway, >=1 object with a cursor_column.
    CDC: ingestion_gateway_id present. Otherwise SaaS/unknown (later stages).
    """
    idef = _ingestion_def(definition)
    if idef.get("ingestion_gateway_id"):
        return ("cdc", "tier2")
    objs = idef.get("objects") or []
    has_cursor = any(
        (o.get("table") or {}).get("table_configuration", {}).get("cursor_column")
        for o in objs
    )
    if has_cursor:
        return ("query_based", "tier1")
    if idef:
        return ("saas", "tier1")
    return ("unknown", "tier2")


def extract_table_configs(definition: dict) -> list[dict]:
    """Flatten ingestion_definition.objects[].table into plain dicts."""
    out: list[dict] = []
    for o in _ingestion_def(definition).get("objects") or []:
        t = o.get("table") or {}
        tc = t.get("table_configuration") or {}
        out.append({
            "source_catalog": t.get("source_catalog"),
            "source_schema": t.get("source_schema"),
            "source_table": t.get("source_table"),
            "destination_catalog": t.get("destination_catalog"),
            "destination_schema": t.get("destination_schema"),
            "destination_table": t.get("destination_table"),
            "scd_type": tc.get("scd_type"),
            "primary_keys": tc.get("primary_keys") or [],
            "cursor_column": tc.get("cursor_column"),
        })
    return out


def build_unified_view_sql(
    *, canonical: str, history: str, incr: str,
    scd_type: str, primary_keys: list[str], cursor_column: str,
) -> str:
    """CREATE OR REPLACE VIEW at `canonical` over history+incr.

    SCD1 -> keep one current row per PK (latest cursor wins). SCD2/APPEND_ONLY ->
    UNION ALL (history segments are distinct rows by design).
    """
    if str(scd_type).upper() == "SCD_TYPE_1":
        pk = ", ".join(primary_keys)
        return (
            f"CREATE OR REPLACE VIEW {canonical} AS\n"
            f"SELECT * EXCEPT(_rn) FROM (\n"
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk} "
            f"ORDER BY {cursor_column} DESC) AS _rn\n"
            f"  FROM (SELECT * FROM {history} UNION ALL SELECT * FROM {incr})\n"
            f") WHERE _rn = 1"
        )
    return (
        f"CREATE OR REPLACE VIEW {canonical} AS\n"
        f"SELECT * FROM {history} UNION ALL SELECT * FROM {incr}"
    )
