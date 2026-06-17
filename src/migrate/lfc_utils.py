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

# SaaS source types that support a settable per-table row_filter on their cursor
# column (Tier 1). Other SaaS connectors (Workday, SharePoint, NetSuite, …) have
# no boundary handle and are Tier 2 (full re-hydrate on recreate).
_ROW_FILTER_SAAS_SOURCE_TYPES = frozenset({"SALESFORCE", "GA4_RAW_DATA", "SERVICENOW"})


def _ingestion_def(definition: dict) -> dict:
    return ((definition or {}).get("spec") or {}).get("ingestion_definition") or {}


def _cursor_columns(table_configuration: dict) -> list[str]:
    """Cursor column(s) for a query-based table. The platform persists them under
    the NESTED ``query_based_connector_config.cursor_columns`` (plural) — the flat
    ``cursor_column`` is silently dropped on create (see DOC-26044). Returns []
    when none are set (e.g. connector auto-selected, or a SaaS/CDC table)."""
    qbc = (table_configuration or {}).get("query_based_connector_config") or {}
    return qbc.get("cursor_columns") or []


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
    """DISCOVERY SUGGESTION ONLY — the doc-recommended cursor for a Tier-1 SaaS
    table. NOT used for selection: the cursor is a MANDATORY operator-supplied
    input (``config.lfc_saas_cursor_columns``); the tool never auto-guesses it.

    Matches the connector's documented priority order against the columns that
    actually exist on the destination table and returns the FIRST priority column
    present (preserving its real-cased name), or None when none are present.
    Column matching is case-insensitive. Used by discovery to annotate which
    candidate column an operator would typically pick."""
    priority = _SAAS_CURSOR_PRIORITY.get(str(source_type or "").upper())
    if not priority:
        return None
    by_lower = {c.lower(): c for c in available_columns}
    for cand in priority:
        if cand.lower() in by_lower:
            return by_lower[cand.lower()]
    return None


def parse_describe_columns(rows: list) -> list[str]:
    """Column names from a ``DESCRIBE TABLE`` result (rows are
    ``[col_name, data_type, comment]``). Collection STOPS at the first metadata
    section — a blank or ``#``-prefixed ``col_name`` (e.g. ``# Partition
    Information``) — so partition columns re-listed there aren't double-counted.
    Used to resolve a SaaS cursor from a table's real columns."""
    cols: list[str] = []
    for r in rows or []:
        if isinstance(r, (list, tuple)):
            name = r[0] if r else None
        elif isinstance(r, dict):
            name = r.get("col_name")
        else:
            name = None
        if name is None:
            continue
        name = str(name).strip()
        if not name or name.startswith("#"):
            break
        cols.append(name)
    return cols


# DESCRIBE TABLE data_type prefixes that are plausible incremental cursors
# (monotonic / orderable): temporal and numeric. Matched case-insensitively on
# the leading token so parameterised types (decimal(10,2)) and *_ntz/_ltz
# variants are caught. "int"/"integer" handled explicitly so "interval" (a
# duration, not a cursor) is NOT matched.
_CURSOR_TYPE_PREFIXES = (
    "timestamp", "date", "bigint", "smallint", "tinyint",
    "long", "short", "byte", "decimal", "numeric", "double", "float",
)
_CURSOR_TYPE_EXACT = frozenset({"int", "integer"})


def _describe_rows(rows: list):
    """Yield (col_name, data_type) from a ``DESCRIBE TABLE`` result, STOPPING at
    the first metadata section (blank or ``#``-prefixed col_name), mirroring
    :func:`parse_describe_columns`."""
    for r in rows or []:
        if isinstance(r, (list, tuple)):
            name = r[0] if r else None
            dtype = r[1] if len(r) > 1 else None
        elif isinstance(r, dict):
            name = r.get("col_name")
            dtype = r.get("data_type")
        else:
            name = dtype = None
        if name is None:
            continue
        name = str(name).strip()
        if not name or name.startswith("#"):
            break
        yield name, str(dtype or "").strip().lower()


def candidate_cursor_columns(rows: list) -> list[str]:
    """Plausible cursor columns from a ``DESCRIBE TABLE`` result — those whose
    type is temporal (timestamp/date) or numeric. A pure discovery aid so an
    operator can see candidates before declaring ``lfc_saas_cursor_columns``;
    NOT used for selection. Preserves real column casing and DESCRIBE order."""
    out: list[str] = []
    for name, dtype in _describe_rows(rows):
        head = dtype.split("(", 1)[0]
        if head in _CURSOR_TYPE_EXACT or dtype.startswith(_CURSOR_TYPE_PREFIXES):
            out.append(name)
    return out


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
    if source_type in _ROW_FILTER_SAAS_SOURCE_TYPES:
        return ("saas", "tier1")
    if idef:
        return ("saas", "tier2")
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


def build_recreate_spec(
    definition: dict, *, target_connection_name: str, name: str,
    row_filter_by_src: dict[str, str],
) -> dict:
    """Connector-agnostic Tier-1 recreate spec. ``row_filter_by_src`` maps
    source_table -> the full row_filter predicate ("<cursor> >= 'T'"). A table in
    the map gets destination ``<table>_incr`` + that row_filter; a table NOT in the
    map keeps its canonical destination and no filter (full-load). The caller
    supplies the cursor/boundary — query-based reads it from the spec, SaaS reads
    it from the operator-supplied lfc_saas_cursor_columns map."""
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
    # Top-level catalog/schema are REQUIRED on create (direct publishing mode);
    # carry them over from the discovered source pipeline spec.
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
    cursor_columns. Thin wrapper over build_recreate_spec.

    boundaries maps source_table -> cursor boundary T. A table with a boundary
    (and a cursor) gets destination `<table>_incr` + row_filter `<cursor> >= 'T'`;
    a table WITHOUT a boundary (batch/no-cursor) keeps the canonical destination
    and no filter (full-load — its normal behaviour).
    """
    row_filter_by_src: dict[str, str] = {}
    for o in _ingestion_def(definition).get("objects") or []:
        t = o.get("table") or {}
        src = t.get("source_table")
        cursors = _cursor_columns(t.get("table_configuration") or {})
        if src in boundaries and cursors:
            row_filter_by_src[src] = f"{cursors[0]} >= '{boundaries[src]}'"
    return build_recreate_spec(definition, target_connection_name=target_connection_name,
                               name=name, row_filter_by_src=row_filter_by_src)


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
