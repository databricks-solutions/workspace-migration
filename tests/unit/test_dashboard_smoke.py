"""Smoke test for the Lakeview migration dashboard.

The dashboard at ``dashboards/migration_dashboard.lvdash.json`` is deployed
by the DAB and reads from the tracking tables in
``migration_tracking.cp_migration.*`` (``discovery_inventory``,
``migration_status``, ``pre_check_results``). When a worker adds a new
column or renames one, the dashboard silently breaks on the next deploy.

These tests catch that drift at unit-test time by:

- Parsing the dashboard JSON and extracting every dataset's SQL query.
- Loading the canonical tracking-table schemas directly from the
  ``CREATE TABLE`` DDL emitted by ``TrackingManager.init_tracking_tables``
  (the source-of-truth) - we do NOT duplicate the schema here.
- Asserting every column referenced by a dashboard dataset exists in the
  matching tracking table, and that every widget's ``datasetName`` points
  at an actual dataset.

Pure unit test - no workspace access, no Databricks SDK calls.

Fall-back strategy: if ``sqlglot`` is installed we use it to parse the
SQL (dialect ``databricks``) and extract columns via the AST. If not, we
use a regex scan that pulls identifiers out of ``SELECT ...`` / ``WHERE
... IN ...`` / ``ORDER BY ...`` / ``GROUP BY ...`` clauses. The regex
path is coarser but still catches the common drift cases (renamed or
deleted columns referenced by name in the query body).
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_PATH = REPO_ROOT / "dashboards" / "migration_dashboard.lvdash.json"
TRACKING_SOURCE = REPO_ROOT / "src" / "common" / "tracking.py"

TRACKING_TABLES = ("discovery_inventory", "migration_status", "pre_check_results")

# Reserved tokens that can appear in SELECT / expression contexts but are
# never column names. Anything matching these is filtered out before we
# cross-check against the tracking schema.
SQL_RESERVED: frozenset[str] = frozenset(
    {
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
        "AS", "ASC", "DESC", "ON", "BY", "ORDER", "GROUP", "HAVING", "LIMIT",
        "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
        "UNION", "ALL", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END",
        "CAST", "COUNT", "SUM", "AVG", "MIN", "MAX", "ROW_NUMBER", "OVER",
        "PARTITION", "WITH", "TRUE", "FALSE", "BETWEEN", "LIKE", "INTERVAL",
        # Plain aliases / expressions that appear naked in SELECT but are
        # not schema columns.
        "RN", "N",
    }
)


# ---------------------------------------------------------------- schema loader


def _extract_sql_strings_from_init_tables() -> list[str]:
    """Pull every CREATE TABLE SQL body out of ``init_tracking_tables``
    by walking the AST of ``src/common/tracking.py``.

    Using AST (not regex) so we correctly handle f-string substitutions
    like ``{self._fqn}``. The FQN prefix is replaced with ``<EXPR>`` -
    we only care about the column definitions, not the table prefix.
    """
    source = TRACKING_SOURCE.read_text()
    tree = ast.parse(source)

    def render(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            parts: list[str] = []
            for v in node.values:
                if isinstance(v, ast.Constant):
                    parts.append(v.value)
                else:
                    parts.append("<EXPR>")
            return "".join(parts)
        return None

    results: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "init_tracking_tables":
            for inner in ast.walk(node):
                if isinstance(inner, (ast.Constant, ast.JoinedStr)):
                    rendered = render(inner)
                    if rendered and "CREATE TABLE" in rendered:
                        results.append(rendered)
            break
    return results


_DDL_COL_RE = re.compile(
    r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+(STRING|LONG|DOUBLE|BOOLEAN|TIMESTAMP|INT|BIGINT|FLOAT|DECIMAL)\b",
    re.IGNORECASE,
)


def _columns_from_ddl(ddl: str) -> set[str]:
    """Extract column names from a ``CREATE TABLE ... (col TYPE, ...)`` body."""
    m = re.search(r"\(\s*(.+)\)\s*USING", ddl, flags=re.DOTALL | re.IGNORECASE)
    if not m:
        return set()
    body = m.group(1)
    depth = 0
    buf: list[str] = []
    parts: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf))
    cols: set[str] = set()
    for part in parts:
        line_match = _DDL_COL_RE.match(part)
        if line_match:
            cols.add(line_match.group(1).lower())
    return cols


def _load_tracking_schemas() -> dict[str, set[str]]:
    """Return ``{table_name: set(column_names_lower)}`` derived from the
    canonical ``init_tracking_tables`` DDL. No duplication - this is the
    source of truth inside the tool."""
    ddls = _extract_sql_strings_from_init_tables()
    schema: dict[str, set[str]] = {}
    for ddl in ddls:
        for table in TRACKING_TABLES:
            if re.search(rf"\.{re.escape(table)}\s*\(", ddl):
                cols = _columns_from_ddl(ddl)
                if cols:
                    schema[table] = cols
                break
    return schema


# ---------------------------------------------------------------- dashboard loader


def _load_dashboard() -> dict:
    return json.loads(DASHBOARD_PATH.read_text())


def _dataset_queries(dashboard: dict) -> list[tuple[str, str]]:
    """Return ``[(dataset_name, sql_text), ...]`` for every dataset."""
    out: list[tuple[str, str]] = []
    for ds in dashboard.get("datasets", []):
        name = ds.get("name")
        lines = ds.get("queryLines") or []
        sql = "".join(lines)
        out.append((name, sql))
    return out


# ---------------------------------------------------------------- column extractor


def _try_sqlglot():
    try:
        import sqlglot  # type: ignore

        return sqlglot
    except ImportError:
        return None


_FROM_TABLE_RE = re.compile(
    r"FROM\s+[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)
_IDENT_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")


def _columns_from_sql_regex(sql: str, allowed_tables: set[str]) -> dict[str, set[str]]:
    """Regex fallback. Attributes every identifier to every tracking
    table named in the FROM clause - caller validates the union."""
    cleaned = sql.replace("`", "")

    tables_in_query: set[str] = set()
    for m in _FROM_TABLE_RE.finditer(cleaned):
        tables_in_query.add(m.group(1).lower())

    allowed_here = tables_in_query & allowed_tables
    if not allowed_here:
        return {}

    # Remove string literals so 'match' / 'mismatch' etc. don't leak in
    # as identifiers.
    without_strings = re.sub(r"'[^']*'", "", cleaned)
    idents: set[str] = set()
    for m in _IDENT_RE.finditer(without_strings):
        tok = m.group(0)
        if tok.upper() in SQL_RESERVED:
            continue
        if tok.isdigit():
            continue
        if tok.lower() in ("migration_tracking", "cp_migration"):
            continue
        if tok.lower() in allowed_tables:
            continue
        idents.add(tok.lower())

    result: dict[str, set[str]] = {}
    for t in allowed_here:
        result[t] = set(idents)
    return result


def _columns_from_sql(sql: str, allowed_tables: set[str]) -> dict[str, set[str]]:
    """Return ``{table_name: set(columns_referenced)}``.

    Uses sqlglot's AST if available, else falls back to the regex scan.
    """
    sqlglot = _try_sqlglot()
    if sqlglot is not None:
        try:
            parsed = sqlglot.parse_one(sql, read="databricks")
            alias_to_table: dict[str, str] = {}
            for t in parsed.find_all(sqlglot.exp.Table):
                real = t.name
                alias = t.alias_or_name
                alias_to_table[alias] = real
            # Aliases declared inside the query (e.g. ``COUNT(*) AS n`` or
            # ``ROW_NUMBER() OVER (...) AS rn``) reference derived values,
            # not base-table columns. Collect them so we can filter any
            # downstream ``col.name`` occurrences - those are references
            # to the alias, not a claim that the tracking table has a
            # column by that name.
            query_aliases: set[str] = set()
            for a in parsed.find_all(sqlglot.exp.Alias):
                alias_name = a.alias
                if alias_name:
                    query_aliases.add(alias_name.lower())
            table_cols: dict[str, set[str]] = {}
            for col in parsed.find_all(sqlglot.exp.Column):
                col_name = col.name.lower()
                if col_name in query_aliases:
                    continue  # reference to a derived alias, not a base column
                tbl = col.table
                real_table = alias_to_table.get(tbl, tbl) if tbl else None
                if real_table and real_table in allowed_tables:
                    table_cols.setdefault(real_table, set()).add(col_name)
                elif not real_table:
                    for rt in alias_to_table.values():
                        if rt in allowed_tables:
                            table_cols.setdefault(rt, set()).add(col_name)
            return table_cols
        except Exception:
            pass

    return _columns_from_sql_regex(sql, allowed_tables)


# ---------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def dashboard() -> dict:
    return _load_dashboard()


@pytest.fixture(scope="module")
def tracking_schema() -> dict[str, set[str]]:
    schema = _load_tracking_schemas()
    assert set(schema.keys()) == set(TRACKING_TABLES), (
        f"Failed to load all tracking schemas from DDL - got {sorted(schema.keys())}"
    )
    for tbl, cols in schema.items():
        assert cols, f"No columns parsed for {tbl}"
    return schema


# ---------------------------------------------------------------- tests


class TestDashboardJSON:
    """Parse + structural assertions on the dashboard JSON."""

    def test_dashboard_json_is_parseable(self):
        data = _load_dashboard()
        assert isinstance(data, dict)
        assert "datasets" in data
        assert "pages" in data

    def test_dashboard_has_expected_shape(self, dashboard):
        assert len(dashboard["datasets"]) >= 1
        assert len(dashboard["pages"]) >= 1

    def test_every_dataset_has_name_and_query(self, dashboard):
        for ds in dashboard["datasets"]:
            assert ds.get("name"), f"dataset missing name: {ds}"
            assert ds.get("queryLines"), (
                f"dataset {ds.get('name')} has no queryLines"
            )


class TestTrackingSchemaLoader:
    """Sanity checks on the DDL parser - if this breaks the downstream
    drift checks are meaningless."""

    def test_all_three_tracking_tables_loaded(self, tracking_schema):
        assert set(tracking_schema.keys()) == set(TRACKING_TABLES)

    def test_known_columns_present(self, tracking_schema):
        # Anchors - if someone reorders columns these key ones must stay.
        assert "object_name" in tracking_schema["discovery_inventory"]
        assert "object_type" in tracking_schema["discovery_inventory"]
        assert "migrated_at" in tracking_schema["migration_status"]
        assert "source_row_count" in tracking_schema["migration_status"]
        assert "target_row_count" in tracking_schema["migration_status"]
        assert "check_name" in tracking_schema["pre_check_results"]
        assert "checked_at" in tracking_schema["pre_check_results"]


class TestDatasetQueries:
    """Every dataset query references only existing columns on the
    tracking tables it reads from."""

    def test_every_query_references_a_tracking_table(self, dashboard):
        for name, sql in _dataset_queries(dashboard):
            assert "FROM" in sql.upper(), f"dataset {name} has no FROM clause"
            assert any(t in sql for t in TRACKING_TABLES), (
                f"dataset {name} does not reference any of the tracking tables "
                f"{TRACKING_TABLES}"
            )

    def test_every_query_is_sql_parseable(self, dashboard):
        """With sqlglot: every query must parse in databricks dialect.
        Without sqlglot: skipped; the regex smoke check above covers the
        lighter contract."""
        sqlglot = _try_sqlglot()
        if sqlglot is None:
            pytest.skip("sqlglot not installed - relying on regex smoke check")
        for name, sql in _dataset_queries(dashboard):
            try:
                parsed = sqlglot.parse_one(sql, read="databricks")
            except Exception as exc:
                pytest.fail(f"dataset {name} did not parse as databricks SQL: {exc}")
            assert parsed is not None, f"dataset {name} parsed to None"

    def test_every_referenced_column_exists_in_tracking_schema(
        self, dashboard, tracking_schema
    ):
        allowed_tables = set(tracking_schema.keys())
        failures: list[str] = []
        for name, sql in _dataset_queries(dashboard):
            table_cols = _columns_from_sql(sql, allowed_tables)
            if not table_cols:
                failures.append(f"{name}: no columns resolved from query")
                continue
            for table, cols in table_cols.items():
                valid = tracking_schema.get(table, set())
                unknown = cols - valid
                if unknown:
                    failures.append(
                        f"{name}: unknown columns on {table}: "
                        f"{sorted(unknown)} (valid: {sorted(valid)})"
                    )
        assert not failures, (
            "Dashboard references columns that don't exist in the tracking "
            "schema - the dashboard will fail on deploy. Fix the dashboard "
            "JSON or update the schema.\n  "
            + "\n  ".join(failures)
        )


class TestWidgetDatasetRefs:
    """Every widget's ``datasetName`` must resolve to a dataset in the
    dashboard's ``datasets`` array. A dangling ref renders an empty
    widget on deploy with no visible error."""

    def test_every_widget_dataset_ref_exists(self, dashboard):
        dataset_names = {ds["name"] for ds in dashboard["datasets"]}
        failures: list[str] = []
        for page in dashboard["pages"]:
            for layout_entry in page.get("layout", []):
                widget = layout_entry.get("widget", {})
                widget_name = widget.get("name", "<unnamed>")
                for q in widget.get("queries", []) or []:
                    ref = q.get("query", {}).get("datasetName")
                    if ref is None:
                        continue  # text / markdown widget
                    if ref not in dataset_names:
                        failures.append(
                            f"widget {widget_name!r} on page "
                            f"{page.get('name')!r} references missing "
                            f"dataset {ref!r}"
                        )
        assert not failures, (
            "Widget -> dataset refs broken:\n  " + "\n  ".join(failures)
        )

    def test_every_widget_field_expression_resolves_to_its_dataset_columns(
        self, dashboard, tracking_schema
    ):
        """Each widget declares ``fields: [{name, expression}]``. The
        expression is evaluated against its referenced dataset's output
        columns. If the dataset renames a column (or the underlying
        tracking table drops one) the field expression becomes a dangling
        reference."""
        datasets_by_name = {ds["name"]: ds for ds in dashboard["datasets"]}
        failures: list[str] = []

        allowed_tables = set(tracking_schema.keys())
        dataset_outputs: dict[str, set[str]] = {}
        for name, ds in datasets_by_name.items():
            sql = "".join(ds.get("queryLines") or [])
            outputs: set[str] = set()
            if re.search(r"SELECT\s+\*", sql, flags=re.IGNORECASE):
                for tbl in allowed_tables:
                    if tbl in sql:
                        outputs |= tracking_schema[tbl]
                outputs.add("rn")  # subquery ROW_NUMBER() alias
            outputs |= _explicit_select_aliases(sql)
            dataset_outputs[name] = outputs

        for page in dashboard["pages"]:
            for layout_entry in page.get("layout", []):
                widget = layout_entry.get("widget", {})
                widget_name = widget.get("name", "<unnamed>")
                for q in widget.get("queries", []) or []:
                    ref = q.get("query", {}).get("datasetName")
                    if ref is None:
                        continue
                    fields = q.get("query", {}).get("fields") or []
                    allowed = dataset_outputs.get(ref, set())
                    for field in fields:
                        expr = field.get("expression", "")
                        refs = re.findall(r"`([^`]+)`", expr)
                        for col in refs:
                            if col.lower() not in allowed:
                                failures.append(
                                    f"widget {widget_name!r} field "
                                    f"{field.get('name')!r} references "
                                    f"`{col}` which is not produced by "
                                    f"dataset {ref!r}"
                                )
        assert not failures, (
            "Widget field expressions reference columns that the dataset "
            "doesn't expose:\n  " + "\n  ".join(failures)
        )


def _explicit_select_aliases(sql: str) -> set[str]:
    """Return the set of output column names / aliases from the outermost
    SELECT of a query. Best-effort - returns empty set for ``SELECT *``
    (caller handles star expansion separately)."""
    m = re.search(r"SELECT\s+(.*?)\s+FROM\s", sql, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return set()
    select_list = m.group(1)
    if select_list.strip() == "*":
        return set()
    depth = 0
    parts: list[str] = []
    buf: list[str] = []
    for ch in select_list:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf))
    aliases: set[str] = set()
    for raw in parts:
        p = raw.strip()
        if not p:
            continue
        alias_match = re.search(
            r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$",
            p,
            flags=re.IGNORECASE,
        )
        if alias_match:
            aliases.add(alias_match.group(1).lower())
            continue
        bare = p.replace("`", "").strip()
        if "." in bare:
            bare = bare.rsplit(".", 1)[-1]
        if re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", bare):
            aliases.add(bare.lower())
    return aliases
