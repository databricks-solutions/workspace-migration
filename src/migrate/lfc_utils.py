"""Pure transformation helpers for migrate_lfc (no side effects, unit-tested).

Stage 1 supports query-based connectors only. CDC and SaaS classify here but
are handled by later stages.
"""
from __future__ import annotations

import copy

# Source types whose ingestion is query-based (cursor-driven, no gateway).
# A CDC pipeline for the same engine carries a gateway and is caught first.
_QUERY_BASED_SOURCE_TYPES = frozenset(
    {"SQLSERVER", "MYSQL", "POSTGRESQL", "ORACLE", "TERADATA", "MARIADB"}
)


def _ingestion_def(definition: dict) -> dict:
    return ((definition or {}).get("spec") or {}).get("ingestion_definition") or {}


def _cursor_columns(table_configuration: dict) -> list[str]:
    """Cursor column(s) for a query-based table. The platform persists them under
    the NESTED ``query_based_connector_config.cursor_columns`` (plural) — the flat
    ``cursor_column`` is silently dropped on create (see DOC-26044). Returns []
    when none are set (e.g. connector auto-selected, or a SaaS/CDC table)."""
    qbc = (table_configuration or {}).get("query_based_connector_config") or {}
    return qbc.get("cursor_columns") or []


def classify_pipeline(definition: dict) -> tuple[str, str]:
    """Return (connector_kind, tier).

    connector_kind in {"query_based","cdc","saas","unknown"}; tier in {"tier1","tier2"}.
    CDC: an ``ingestion_gateway_id`` is present (tier2). Query-based: no gateway and
    a database ``source_type`` (tier1). Otherwise SaaS/unknown. The cursor column is
    NOT used as the signal — it can be auto-selected and absent from the spec.
    """
    idef = _ingestion_def(definition)
    if idef.get("ingestion_gateway_id"):
        return ("cdc", "tier2")
    source_type = str(idef.get("source_type") or "").upper()
    if source_type in _QUERY_BASED_SOURCE_TYPES:
        return ("query_based", "tier1")
    if idef:
        return ("saas", "tier1")
    return ("unknown", "tier2")


def extract_table_configs(definition: dict) -> list[dict]:
    """Flatten ingestion_definition.objects[].table into plain dicts.

    ``cursor_column`` is the first of the nested ``cursor_columns`` (or None) — the
    common case is a single cursor; multi-cursor query-based tables are rare.
    """
    out: list[dict] = []
    for o in _ingestion_def(definition).get("objects") or []:
        t = o.get("table") or {}
        tc = t.get("table_configuration") or {}
        cursors = _cursor_columns(tc)
        out.append({
            "source_catalog": t.get("source_catalog"),
            "source_schema": t.get("source_schema"),
            "source_table": t.get("source_table"),
            "destination_catalog": t.get("destination_catalog"),
            "destination_schema": t.get("destination_schema"),
            "destination_table": t.get("destination_table"),
            "scd_type": tc.get("scd_type"),
            "primary_keys": tc.get("primary_keys") or [],
            "cursor_column": cursors[0] if cursors else None,
        })
    return out


def build_query_based_create_spec(
    definition: dict, *, target_connection_name: str,
    boundaries: dict[str, str], name: str,
) -> dict:
    """Build the pipelines.create spec for the recreated query-based pipeline.

    boundaries maps source_table -> cursor boundary T. A table with a boundary
    gets destination `<table>_incr` + row_filter `<cursor> >= 'T'`; a table
    WITHOUT a boundary (batch/no-cursor) keeps the canonical destination and no
    filter (full-load — its normal behaviour).
    """
    spec = (definition or {}).get("spec") or {}
    idef = copy.deepcopy(_ingestion_def(definition))
    idef["connection_name"] = target_connection_name
    for o in idef.get("objects") or []:
        t = o.get("table") or {}
        tc = t.setdefault("table_configuration", {})
        src = t.get("source_table")
        cursors = _cursor_columns(tc)  # nested query_based_connector_config.cursor_columns (preserved by deepcopy)
        if src in boundaries and cursors:
            t["destination_table"] = f"{t['destination_table']}_incr"
            tc["row_filter"] = f"{cursors[0]} >= '{boundaries[src]}'"
    out = {"name": name, "channel": "PREVIEW", "ingestion_definition": idef}
    # Top-level catalog/schema are REQUIRED on create (direct publishing mode);
    # carry them over from the discovered source pipeline spec.
    if spec.get("catalog"):
        out["catalog"] = spec["catalog"]
    if spec.get("schema"):
        out["schema"] = spec["schema"]
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
