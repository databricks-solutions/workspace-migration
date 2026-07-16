"""Resolve which tables are protected by legacy RLS/CM or ABAC policies.

Scope decision (findings #21/#16): the tool does NOT migrate tables protected
by a row filter, column mask, or ABAC policy — copying them risks silent data
loss (the copy reads *through* the policy) and there is no safe source-untouched
way to read the raw data (DEEP CLONE is blocked for policy-bound tables). So
discovery instead **excludes** these tables from migration and **surfaces** them
in the dashboard for the customer to handle manually.

This module is the pure logic that computes the affected-table set. It's kept
free of Spark/SDK so it's unit-testable in isolation. Discovery supplies:
  * legacy RLS/CM table FQNs (already bound to a table via information_schema),
  * ABAC policy definitions (from ``CatalogExplorer.list_policies``), and
  * per-table column tags (already collected during discovery),
and this module returns ``{table_fqn: [reasons]}``.

ABAC definition shape (captured live from
``GET /api/2.1/unity-catalog/policies/{type}/{fullname}``):

    {"policy_type": "POLICY_TYPE_COLUMN_MASK",   # or POLICY_TYPE_ROW_FILTER
     "on_securable_type": "SCHEMA",              # CATALOG | SCHEMA | TABLE
     "on_securable_fullname": "cat.schema",
     "match_columns": [{"alias": "c",
                        "condition": "has_tag_value('access_class','bu_column')"}],
     ...}
"""

from __future__ import annotations

import json
import re

# has_tag_value('key','value') — the only predicate ABAC MATCH COLUMNS accepts.
_TAG_COND = re.compile(r"has_tag_value\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)")


def parse_tag_conditions(condition: str | None) -> list[tuple[str, str]]:
    """Extract ``(tag_key, tag_value)`` pairs from a policy condition string."""
    return [(m.group(1), m.group(2)) for m in _TAG_COND.finditer(condition or "")]


def _norm(fqn: str) -> str:
    """Canonicalise an FQN to unbackticked ``a.b.c`` for prefix comparisons."""
    return (fqn or "").replace("`", "")


def tables_in_scope(on_type: str, on_fullname: str, all_tables: list[str]) -> list[str]:
    """Tables governed by a policy attached at ``on_type``/``on_fullname``.

    TABLE → just that table; SCHEMA/CATALOG → every table under that prefix.
    """
    on = _norm(on_fullname)
    tables = [t for t in all_tables]
    ot = (on_type or "").upper()
    if ot == "TABLE":
        return [t for t in tables if _norm(t) == on]
    if ot in ("SCHEMA", "CATALOG"):
        return [t for t in tables if _norm(t).startswith(on + ".")]
    return []


def resolve_abac_affected_tables(
    policies: list[dict],
    column_tags: dict[str, set[tuple[str, str]]],
    all_tables: list[str],
) -> dict[str, list[dict]]:
    """Map each ABAC-policy-protected table to the policy/policies protecting it.

    A TABLE-scoped policy protects that table outright. A SCHEMA/CATALOG-scoped
    policy protects an in-scope table only if one of the table's columns carries
    a tag matching the policy's ``match_columns`` ``has_tag_value`` condition
    (``column_tags[table_fqn]`` is the set of ``(key, value)`` on that table's
    columns, which discovery already collects).
    """
    affected: dict[str, list[dict]] = {}
    for p in policies:
        on_type = (p.get("on_securable_type") or "").upper()
        scope = tables_in_scope(on_type, p.get("on_securable_fullname", ""), all_tables)
        conds: list[tuple[str, str]] = []
        for mc in p.get("match_columns") or []:
            conds.extend(parse_tag_conditions(mc.get("condition")))
        reason = {
            "policy_name": p.get("name"),
            "policy_type": p.get("policy_type"),
            "on_securable": p.get("on_securable_fullname"),
        }
        for t in scope:
            if on_type == "TABLE" or any(c in column_tags.get(t, set()) for c in conds):
                affected.setdefault(t, []).append(reason)
    return affected


def affected_tables(
    *,
    rls_cm_table_fqns: list[str],
    abac_policies: list[dict],
    column_tags: dict[str, set[tuple[str, str]]],
    all_tables: list[str],
) -> dict[str, list[dict]]:
    """Union of legacy RLS/CM-protected tables (bound directly to a table) and
    ABAC-policy-protected tables. Returns ``{table_fqn: [reasons]}`` — the set
    discovery excludes from migration and reports in the dashboard."""
    out: dict[str, list[dict]] = {}
    for fqn in rls_cm_table_fqns:
        out.setdefault(_norm(fqn), []).append(
            {"policy_name": None, "policy_type": "LEGACY_RLS_CM", "on_securable": _norm(fqn)}
        )
    for fqn, reasons in resolve_abac_affected_tables(abac_policies, column_tags, all_tables).items():
        out.setdefault(_norm(fqn), []).extend(reasons)
    return out


def affected_tables_from_inventory(rows: list[dict]) -> dict[str, list[dict]]:
    """Compute policy-protected tables directly from discovery_inventory row
    dicts (each with ``object_type`` + ``metadata_json``). Extracts the legacy
    RLS/CM table FQNs (row_filter / column_mask rows), ABAC policy definitions
    (policy rows), per-table column tags (COLUMN tag rows), and the table
    universe (managed/external_table rows), then delegates to
    :func:`affected_tables`. Kept here (not in discovery) so it's unit-testable.
    """
    rls_cm: list[str] = []
    abac: list[dict] = []
    column_tags: dict[str, set[tuple[str, str]]] = {}
    # Map canonical (unbackticked) FQN -> the ORIGINAL managed_table object_name
    # (backticked). The returned keys MUST be the original form so they match
    # the managed_table discovery rows downstream — setup_sharing's share
    # exclusion and the orchestrator's terminal skip both key off the exact
    # managed_table object_name (backticked). Emitting the normalized form let
    # ABAC-only tables slip past the share exclusion (found in live validation).
    managed_orig: dict[str, str] = {}
    for r in rows:
        ot = r.get("object_type")
        md: dict = {}
        mj = r.get("metadata_json")
        if mj:
            try:
                md = json.loads(mj)
            except Exception:  # noqa: BLE001
                md = {}
        # Only MANAGED tables carry the Delta-Sharing data-loss risk. External
        # tables migrate via DDL replay (no data copy) with their policies
        # re-applied on target, so they are NOT excluded.
        if ot == "managed_table":
            orig = r.get("object_name", "")
            managed_orig[_norm(orig)] = orig
        elif ot == "row_filter":
            rls_cm.append(_norm(r.get("object_name", "")))  # object_name is the table FQN
        elif ot == "column_mask":
            rls_cm.append(_norm(md.get("table_fqn") or r.get("object_name", "")))
        elif ot == "policy":
            definition = md.get("definition") or {}
            if definition:
                abac.append(definition)
        elif ot == "tag" and md.get("securable_type") == "COLUMN":
            column_tags.setdefault(_norm(md.get("securable_fqn", "")), set()).add(
                (md.get("tag_name"), md.get("tag_value"))
            )
    resolved = affected_tables(
        rls_cm_table_fqns=rls_cm,
        abac_policies=abac,
        column_tags=column_tags,
        all_tables=sorted(managed_orig),  # canonical keys
    )
    # Scope to managed tables only + return the ORIGINAL (backticked) FQN so it
    # matches the managed_table rows everywhere downstream.
    return {managed_orig[t]: reasons for t, reasons in resolved.items() if t in managed_orig}
