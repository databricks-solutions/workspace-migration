"""Unit tests for policy_affected_tables — the pure resolver that decides which
tables are excluded from migration because they're protected by legacy RLS/CM
or an ABAC policy (findings #21/#16 scope decision: exclude + report, not
migrate). Fixtures use the real ABAC ``definition`` JSON shape captured from
``GET /api/2.1/unity-catalog/policies/...``.
"""

from __future__ import annotations

import json

from common.policy_affected_tables import (
    affected_tables,
    affected_tables_from_inventory,
    parse_tag_conditions,
    resolve_abac_affected_tables,
    tables_in_scope,
)

TABLES = [
    "retail_prod.sales.customers",
    "retail_prod.sales.orders",
    "retail_prod.ops.inventory",
    "retail_archive.cold.old_orders",
]


def _mask_policy(on_type, on_full, key="access_class", val="bu_column", name="p_mask"):
    return {
        "policy_type": "POLICY_TYPE_COLUMN_MASK",
        "on_securable_type": on_type,
        "on_securable_fullname": on_full,
        "name": name,
        "match_columns": [{"alias": "c", "condition": f"has_tag_value('{key}','{val}')"}],
    }


class TestParsing:
    def test_parses_single_condition(self):
        assert parse_tag_conditions("has_tag_value('pii','email')") == [("pii", "email")]

    def test_parses_multiple_and_whitespace(self):
        got = parse_tag_conditions("has_tag_value( 'a' , 'b' ) OR has_tag_value('c','d')")
        assert got == [("a", "b"), ("c", "d")]

    def test_empty(self):
        assert parse_tag_conditions(None) == [] and parse_tag_conditions("") == []


class TestScope:
    def test_table_scope(self):
        assert tables_in_scope("TABLE", "retail_prod.sales.orders", TABLES) == ["retail_prod.sales.orders"]

    def test_schema_scope(self):
        got = set(tables_in_scope("SCHEMA", "retail_prod.sales", TABLES))
        assert got == {"retail_prod.sales.customers", "retail_prod.sales.orders"}

    def test_catalog_scope(self):
        got = set(tables_in_scope("CATALOG", "retail_prod", TABLES))
        assert got == {"retail_prod.sales.customers", "retail_prod.sales.orders", "retail_prod.ops.inventory"}

    def test_backtick_insensitive(self):
        assert tables_in_scope("TABLE", "`retail_prod`.`sales`.`orders`", TABLES) == ["retail_prod.sales.orders"]


class TestAbacResolution:
    def test_table_scoped_policy_affects_that_table_directly(self):
        pols = [_mask_policy("TABLE", "retail_prod.sales.orders")]
        out = resolve_abac_affected_tables(pols, column_tags={}, all_tables=TABLES)
        assert set(out) == {"retail_prod.sales.orders"}
        assert out["retail_prod.sales.orders"][0]["policy_type"] == "POLICY_TYPE_COLUMN_MASK"

    def test_schema_scoped_matches_only_tables_with_tagged_column(self):
        pols = [_mask_policy("SCHEMA", "retail_prod.sales")]
        column_tags = {"retail_prod.sales.customers": {("access_class", "bu_column")}}  # orders has no matching tag
        out = resolve_abac_affected_tables(pols, column_tags, TABLES)
        assert set(out) == {"retail_prod.sales.customers"}

    def test_schema_scoped_no_match_when_tag_absent(self):
        pols = [_mask_policy("SCHEMA", "retail_prod.sales")]
        out = resolve_abac_affected_tables(pols, column_tags={}, all_tables=TABLES)
        assert out == {}

    def test_catalog_scoped_spans_schemas(self):
        pols = [_mask_policy("CATALOG", "retail_prod", key="pii", val="email")]
        column_tags = {
            "retail_prod.sales.customers": {("pii", "email")},
            "retail_prod.ops.inventory": {("pii", "email")},
        }
        out = resolve_abac_affected_tables(pols, column_tags, TABLES)
        assert set(out) == {"retail_prod.sales.customers", "retail_prod.ops.inventory"}

    def test_row_filter_policy_type_preserved(self):
        p = _mask_policy("TABLE", "retail_prod.sales.orders", name="rf")
        p["policy_type"] = "POLICY_TYPE_ROW_FILTER"
        out = resolve_abac_affected_tables([p], {}, TABLES)
        assert out["retail_prod.sales.orders"][0]["policy_type"] == "POLICY_TYPE_ROW_FILTER"


def _row(object_type, object_name, metadata=None):
    return {
        "object_type": object_type,
        "object_name": object_name,
        "metadata_json": json.dumps(metadata) if metadata else None,
    }


class TestFromInventory:
    """Adapter that reads discovery_inventory row dicts (metadata_json strings)."""

    def test_extracts_legacy_and_abac_from_rows(self):
        rows = [
            _row("managed_table", "`retail_prod`.`sales`.`orders`"),
            _row("managed_table", "`retail_prod`.`sales`.`customers`"),
            _row("managed_table", "`retail_prod`.`ops`.`inventory`"),
            # legacy row filter bound to orders
            _row("row_filter", "`retail_prod`.`sales`.`orders`", {"table_fqn": "`retail_prod`.`sales`.`orders`"}),
            # legacy column mask on customers.email
            _row("column_mask", "`retail_prod`.`sales`.`customers`.email",
                 {"table_fqn": "`retail_prod`.`sales`.`customers`", "column_name": "email"}),
            # ABAC schema-scoped column mask matching access_class=bu_column
            _row("policy", "retail_prod.ops::p1", {"definition": {
                "policy_type": "POLICY_TYPE_COLUMN_MASK",
                "on_securable_type": "SCHEMA", "on_securable_fullname": "retail_prod.ops",
                "name": "p1",
                "match_columns": [{"alias": "c", "condition": "has_tag_value('access_class','bu_column')"}],
            }}),
            # column tag that the ABAC policy matches, on inventory.sku
            _row("tag", "`retail_prod`.`ops`.`inventory`.sku:access_class",
                 {"securable_type": "COLUMN", "securable_fqn": "`retail_prod`.`ops`.`inventory`",
                  "column_name": "sku", "tag_name": "access_class", "tag_value": "bu_column"}),
        ]
        out = affected_tables_from_inventory(rows)
        # Keys are the ORIGINAL (backticked) managed_table FQNs so they match
        # the managed_table rows downstream (setup_sharing / orchestrator).
        assert set(out) == {
            "`retail_prod`.`sales`.`orders`",       # legacy row filter
            "`retail_prod`.`sales`.`customers`",    # legacy column mask
            "`retail_prod`.`ops`.`inventory`",      # ABAC (tagged column in scope)
        }

    def test_no_policies_no_affected(self):
        rows = [_row("managed_table", "`c`.`s`.`t`"), _row("view", "`c`.`s`.`v`")]
        assert affected_tables_from_inventory(rows) == {}

    def test_external_table_with_policy_is_not_flagged(self):
        # External tables migrate via DDL replay (no data copy) with policies
        # re-applied on target — they must NOT be excluded, only managed tables.
        rows = [
            _row("external_table", "`c`.`s`.`ext`"),
            _row("row_filter", "`c`.`s`.`ext`", {"table_fqn": "`c`.`s`.`ext`"}),
            _row("column_mask", "`c`.`s`.`ext`.email", {"table_fqn": "`c`.`s`.`ext`", "column_name": "email"}),
        ]
        assert affected_tables_from_inventory(rows) == {}


class TestCombined:
    def test_union_of_legacy_and_abac(self):
        # legacy row filter/mask bound to orders + customers; ABAC schema policy
        # on a pii-tagged column of inventory.
        pols = [_mask_policy("SCHEMA", "retail_prod.ops", key="pii", val="email")]
        column_tags = {"retail_prod.ops.inventory": {("pii", "email")}}
        out = affected_tables(
            rls_cm_table_fqns=["`retail_prod`.`sales`.`orders`", "retail_prod.sales.customers"],
            abac_policies=pols,
            column_tags=column_tags,
            all_tables=TABLES,
        )
        assert set(out) == {
            "retail_prod.sales.orders",
            "retail_prod.sales.customers",
            "retail_prod.ops.inventory",
        }
        assert out["retail_prod.sales.orders"][0]["policy_type"] == "LEGACY_RLS_CM"
        assert out["retail_prod.ops.inventory"][0]["policy_type"] == "POLICY_TYPE_COLUMN_MASK"
