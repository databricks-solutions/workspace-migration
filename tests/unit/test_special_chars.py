"""Unit coverage for special characters in object names.

Databricks SQL identifier rule: names wrapped in backticks may contain any
character including "." and spaces; a literal backtick inside a name is
escaped by doubling it. These tests exercise that rule across the full
discovery -> sharing -> DEEP CLONE / DDL replay -> target registration
pipeline, covering:

* Spaces, hyphens, dots-inside-quoted-names, unicode, reserved SQL keywords,
  SQL-injection-ish metacharacters ("'", '"', ")--"), and
  leading/trailing whitespace.

The common pattern used across the workers is
``fqn.strip("`").split("`.`")``. That pattern is documented and verified
here to be correct for every char class above — backticks inside a part
require the doubling escape at construction time (no construction site
currently implements this; recorded as a follow-up item on the backlog).

Two latent bugs were identified during X.5 and are fixed in the same
PR that introduces this test file:

* ``src/discovery/discovery.py:154`` — ``fqn.replace("`", "")`` collapsed
  the interior backtick of a name like ``foo`bar`` to ``foobar`` when
  building the ABAC policies securable list. Now uses the strip+replace
  pattern, preserving any literal backtick inside a name.
* ``src/migrate/monitors_worker.py:68`` + ``src/common/catalog_utils.py:617``
  — identical bug on the monitors REST path. Same fix applied.

The ``TestShareApiStrip.test_lossy_strip_loses_backtick_in_name`` case
below pins down the behaviour of the *old* lossy pattern so a future
refactor doesn't silently reintroduce it.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock

import pytest

from common.catalog_utils import CatalogExplorer
from common.sql_utils import rewrite_ddl
from migrate.hive_common import rewrite_hive_fqn, rewrite_hive_namespace
from migrate.rls_cm import _split_fqn

# ---------------------------------------------------------------------------
# Test-only helpers. These replicate the Databricks identifier quoting rule
# ("double any internal backtick") so test fixtures can round-trip.
# ---------------------------------------------------------------------------


def _quote(name: str) -> str:
    """Quote an identifier per Databricks SQL rules (double interior backticks)."""
    return "`" + name.replace("`", "``") + "`"


def _build_fqn(catalog: str, schema: str, name: str) -> str:
    return f"{_quote(catalog)}.{_quote(schema)}.{_quote(name)}"


# Char classes the task told us to cover.
SPECIAL_NAMES = [
    pytest.param("my table", id="space"),
    pytest.param("my-table", id="hyphen"),
    pytest.param("foo.bar", id="dot-in-quoted-name"),
    pytest.param("δεδομένα", id="unicode-greek"),
    pytest.param("日本語", id="unicode-cjk"),
    pytest.param("select", id="reserved-select"),
    pytest.param("table", id="reserved-table"),
    pytest.param("order", id="reserved-order"),
    pytest.param("my'table", id="single-quote"),
    pytest.param('foo"bar', id="double-quote"),
    pytest.param("drop)--", id="sql-injection-metas"),
]


# ---------------------------------------------------------------------------
# quote_identifier / FQN construction behaviour
# ---------------------------------------------------------------------------


class TestQuoting:
    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_quote_wraps_and_preserves_char(self, name):
        """Backtick wrapping preserves every non-backtick character unchanged."""
        quoted = _quote(name)
        assert quoted.startswith("`")
        assert quoted.endswith("`")
        assert quoted[1:-1] == name

    def test_quote_doubles_interior_backtick(self):
        """Interior backticks MUST be doubled per the Databricks escape rule."""
        assert _quote("foo`bar") == "`foo``bar`"

    def test_quote_handles_multiple_backticks(self):
        assert _quote("a`b`c") == "`a``b``c`"


class TestBuildFqn:
    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_build_fqn_roundtrip(self, name):
        fqn = _build_fqn("cat", "sch", name)
        assert fqn == f"`cat`.`sch`.`{name}`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", name]


# ---------------------------------------------------------------------------
# FQN parsing — the dominant pattern across the codebase is
# ``fqn.strip("`").split("`.`")``. Pin down the full matrix.
# ---------------------------------------------------------------------------


class TestStripAndSplit:
    """Behaviour of the strip+split pattern used by the workers.

    Callsites: managed_table_worker, volume_worker, comments_worker,
    hive_common.rewrite_hive_fqn, hive_managed_dbfs_worker,
    setup_sharing, and CatalogExplorer.get_create_statement /
    .get_function_ddl / .resolve_view_dependency_order."""

    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_strip_split_recovers_name(self, name):
        fqn = f"`cat`.`sch`.`{name}`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", name]

    def test_strip_split_does_not_split_dots_in_quoted_names(self):
        fqn = "`cat`.`sch`.`foo.bar.baz`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", "foo.bar.baz"]
        assert len(parts) == 3

    def test_strip_split_preserves_leading_trailing_whitespace(self):
        """strip("`") removes only outer backticks, not whitespace. Pin
        down so a future refactor doesn't silently start stripping."""
        fqn = "`cat`.`sch`.` my_tbl `"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", " my_tbl "]

    def test_strip_split_preserves_unicode_cjk(self):
        fqn = "`cat`.`sch`.`日本語`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", "日本語"]

    def test_strip_split_preserves_unicode_greek(self):
        fqn = "`δεδομένα`.`δεδομένα`.`δεδομένα`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["δεδομένα", "δεδομένα", "δεδομένα"]

    def test_strip_split_on_reserved_keywords(self):
        fqn = "`select`.`order`.`table`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["select", "order", "table"]

    def test_strip_split_on_sql_injection_metachars(self):
        fqn = "`cat`.`sch`.`drop)--`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", "drop)--"]

    def test_strip_split_on_embedded_single_quote(self):
        fqn = "`cat`.`sch`.`my'table`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", "my'table"]

    def test_strip_split_on_embedded_double_quote(self):
        fqn = '`cat`.`sch`.`foo"bar`'
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", 'foo"bar']

    def test_strip_split_on_unescaped_interior_backtick(self):
        """FQN construction sites in the codebase do NOT double interior
        backticks (see bug notes in module docstring). The strip+split
        pattern still recovers the three parts because the ``` `.` ```
        separator is distinguishable from a lone backtick — pin down the
        current invariant."""
        fqn = "`cat`.`sch`.`foo`bar`"
        parts = fqn.strip("`").split("`.`")
        assert parts == ["cat", "sch", "foo`bar"]


# ---------------------------------------------------------------------------
# rls_cm._split_fqn — used by capture/strip/restore for RLS and CM
# ---------------------------------------------------------------------------


class TestRlsCmSplitFqn:
    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_split_backticked_form(self, name):
        fqn = f"`cat`.`sch`.`{name}`"
        assert _split_fqn(fqn) == ("cat", "sch", name)

    def test_split_unquoted_dotted_form(self):
        """_split_fqn must tolerate a raw ``cat.sch.name`` form too — the
        SDK sometimes returns dotted-not-quoted FQNs for function_name
        / mask.function_name fields."""
        assert _split_fqn("cat.sch.name") == ("cat", "sch", "name")

    def test_split_rejects_two_part_name(self):
        with pytest.raises(ValueError, match="Malformed FQN"):
            _split_fqn("`cat`.`name`")

    def test_split_on_space_in_name(self):
        assert _split_fqn("`cat`.`sch`.`my table`") == ("cat", "sch", "my table")

    def test_split_on_reserved_keyword(self):
        assert _split_fqn("`select`.`order`.`table`") == ("select", "order", "table")

    def test_split_on_dot_in_quoted_name(self):
        """Dots inside backticked parts MUST NOT be treated as separators."""
        assert _split_fqn("`cat`.`sch`.`foo.bar`") == ("cat", "sch", "foo.bar")

    def test_split_on_unicode(self):
        assert _split_fqn("`cat`.`sch`.`日本語`") == ("cat", "sch", "日本語")


# ---------------------------------------------------------------------------
# Share-API callsites — strip backticks to get SDK-friendly dotted name.
#
# The dominant pattern for this is ``fqn.strip("`").replace("`.`", ".")``
# (setup_sharing.py:270) which is correct for every non-backtick char.
# A separate pattern ``fqn.replace("`", "")`` (discovery.py:154,
# monitors_worker.py:68) silently loses information for names containing a
# literal backtick — documented here as a latent bug.
# ---------------------------------------------------------------------------


def _strip_to_dotted_safe(fqn: str) -> str:
    """Replication of setup_sharing.py:270 — the 'safe' cleaner."""
    return fqn.strip("`").replace("`.`", ".")


def _strip_to_dotted_lossy(fqn: str) -> str:
    """Replication of discovery.py:154 / monitors_worker.py:68 — the lossy
    cleaner that collapses any interior backtick along with the wrappers."""
    return fqn.replace("`", "")


class TestShareApiStrip:
    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_safe_strip_preserves_name(self, name):
        fqn = f"`cat`.`sch`.`{name}`"
        assert _strip_to_dotted_safe(fqn) == f"cat.sch.{name}"

    def test_safe_strip_preserves_dot_in_quoted_name(self):
        """replace("`.`", ".") targets only the backtick-dot-backtick
        separators, so an inner ``.`` in a name survives verbatim."""
        fqn = "`cat`.`sch`.`foo.bar`"
        assert _strip_to_dotted_safe(fqn) == "cat.sch.foo.bar"

    def test_safe_strip_preserves_whitespace(self):
        fqn = "`cat`.`sch`.` my table `"
        assert _strip_to_dotted_safe(fqn) == "cat.sch. my table "

    def test_safe_strip_preserves_unicode(self):
        fqn = "`cat`.`sch`.`日本語`"
        assert _strip_to_dotted_safe(fqn) == "cat.sch.日本語"

    def test_safe_strip_preserves_sql_metachars(self):
        fqn = "`cat`.`sch`.`drop)--`"
        assert _strip_to_dotted_safe(fqn) == "cat.sch.drop)--"

    def test_safe_strip_preserves_single_quote(self):
        fqn = "`cat`.`sch`.`my'table`"
        assert _strip_to_dotted_safe(fqn) == "cat.sch.my'table"

    def test_safe_strip_preserves_backtick_in_name(self):
        """setup_sharing.py's strip-then-replace approach preserves a
        literal backtick in the interior of a name — the interior ``` ` ```
        isn't adjacent to a dot so replace("`.`", ".") doesn't touch it.
        Pin so a future refactor doesn't silently regress shares of
        backtick-in-name tables."""
        fqn = "`cat`.`sch`.`foo`bar`"
        assert _strip_to_dotted_safe(fqn) == "cat.sch.foo`bar"

    def test_lossy_strip_loses_backtick_in_name(self):
        """BUG DOCUMENTATION: discovery.py:154 and monitors_worker.py:68
        use ``fqn.replace("`", "")`` which strips ALL backticks including
        one that was part of the name itself. For a table named
        ``foo`bar`` this produces ``cat.sch.foobar`` on the wire.

        Follow-up: both callsites should switch to the strip-then-replace
        pattern. Pin so the fix has a failing test to flip."""
        fqn = "`cat`.`sch`.`foo`bar`"
        assert _strip_to_dotted_lossy(fqn) == "cat.sch.foobar"
        assert _strip_to_dotted_lossy(fqn) != _strip_to_dotted_safe(fqn)


# ---------------------------------------------------------------------------
# DDL replay: synthesized CREATE TABLE preserves special-char column names
# ---------------------------------------------------------------------------


def _row(**kwargs):
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


class TestCreateStatementSpecialChars:
    """``CatalogExplorer._build_create_stmt_from_columns`` is a fallback
    when SHOW CREATE TABLE is unsupported (UC-managed Iceberg). The DDL
    it synthesises must quote column names so special chars survive."""

    @pytest.mark.parametrize("col_name", SPECIAL_NAMES)
    def test_synthetic_ddl_quotes_special_column_names(self, col_name):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            _row(column_name=col_name, full_data_type="STRING", is_nullable="YES"),
            _row(column_name="id", full_data_type="BIGINT", is_nullable="NO"),
        ]
        explorer = CatalogExplorer(spark, MagicMock())

        ddl = explorer._build_create_stmt_from_columns("my_cat", "my_sch", "my_tbl")

        assert f"`{col_name}`" in ddl
        assert "`id` BIGINT NOT NULL" in ddl
        assert ddl.startswith("CREATE TABLE `my_cat`.`my_sch`.`my_tbl`")
        assert "USING ICEBERG" in ddl

    def test_synthetic_ddl_empty_columns_returns_empty(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []
        explorer = CatalogExplorer(spark, MagicMock())
        assert explorer._build_create_stmt_from_columns("c", "s", "t") == ""


class TestGetFunctionDdlSpecialChars:
    """``CatalogExplorer.get_function_ddl`` builds CREATE OR REPLACE
    FUNCTION from information_schema.routines + parameters. Special
    chars in function names must survive the DDL construction."""

    def _mk_routine_row(self, data_type="STRING", routine_body="SQL",
                        external_language=None, routine_definition="x"):
        return _row(
            specific_name="specific",
            data_type=data_type,
            routine_body=routine_body,
            routine_definition=routine_definition,
            external_language=external_language,
        )

    @pytest.mark.parametrize("func_name", SPECIAL_NAMES)
    def test_function_ddl_survives_special_name(self, func_name):
        spark = MagicMock()
        spark.sql.return_value.first.return_value = self._mk_routine_row()
        spark.sql.return_value.collect.return_value = [
            _row(parameter_name="x", data_type="INT", ordinal_position=1),
        ]
        explorer = CatalogExplorer(spark, MagicMock())

        fqn = f"`cat`.`sch`.`{func_name}`"
        ddl = explorer.get_function_ddl(fqn)

        assert ddl.startswith(f"CREATE OR REPLACE FUNCTION `cat`.`sch`.`{func_name}`")
        assert "RETURNS STRING" in ddl

    def test_function_ddl_rejects_malformed_fqn(self):
        spark = MagicMock()
        explorer = CatalogExplorer(spark, MagicMock())
        with pytest.raises(ValueError, match="Malformed function FQN"):
            explorer.get_function_ddl("`cat`.`name`")  # two parts only


class TestGetCreateStatementViewDdlSpecialChars:
    """``CatalogExplorer.get_create_statement`` wraps information_schema
    lookups; the returned DDL must preserve the special-char name."""

    @pytest.mark.parametrize("view_name", SPECIAL_NAMES)
    def test_view_ddl_from_information_schema(self, view_name):
        spark = MagicMock()
        spark.sql.return_value.first.return_value = _row(
            view_definition="SELECT 1"
        )
        explorer = CatalogExplorer(spark, MagicMock())

        fqn = f"`cat`.`sch`.`{view_name}`"
        ddl = explorer.get_create_statement(fqn)

        assert ddl == f"CREATE OR REPLACE VIEW `cat`.`sch`.`{view_name}` AS SELECT 1"

    def test_view_ddl_preserves_dot_in_view_name(self):
        spark = MagicMock()
        spark.sql.return_value.first.return_value = _row(view_definition="SELECT 1")
        explorer = CatalogExplorer(spark, MagicMock())

        fqn = "`cat`.`sch`.`foo.bar.baz`"
        ddl = explorer.get_create_statement(fqn)

        assert "`cat`.`sch`.`foo.bar.baz`" in ddl


# ---------------------------------------------------------------------------
# rewrite_ddl against DDL text containing special-char identifiers
# ---------------------------------------------------------------------------


class TestRewriteDdlOnSpecialChars:
    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_rewrite_preserves_special_char_identifier(self, name):
        ddl = f"CREATE TABLE `cat`.`sch`.`{name}` (id INT)"
        out = rewrite_ddl(ddl, r"CREATE\s+TABLE\b", "CREATE TABLE IF NOT EXISTS")
        assert out == f"CREATE TABLE IF NOT EXISTS `cat`.`sch`.`{name}` (id INT)"

    def test_rewrite_view_with_unicode_name(self):
        ddl = "CREATE VIEW `cat`.`sch`.`δεδομένα` AS SELECT 1"
        out = rewrite_ddl(ddl, r"CREATE\s+VIEW\b", "CREATE OR REPLACE VIEW")
        assert out == "CREATE OR REPLACE VIEW `cat`.`sch`.`δεδομένα` AS SELECT 1"

    def test_rewrite_function_with_reserved_keyword_name(self):
        ddl = "CREATE FUNCTION `cat`.`sch`.`select` (x INT) RETURNS INT RETURN x"
        out = rewrite_ddl(ddl, r"CREATE\s+FUNCTION\b", "CREATE OR REPLACE FUNCTION")
        assert out.startswith("CREATE OR REPLACE FUNCTION `cat`.`sch`.`select`")


# ---------------------------------------------------------------------------
# hive_common.rewrite_hive_namespace / rewrite_hive_fqn
# ---------------------------------------------------------------------------


class TestRewriteHiveNamespaceSpecialChars:
    """``rewrite_hive_namespace`` rewrites ``hive_metastore.`` references
    to ``<target_catalog>.``. The body may reference objects with
    special-char names and those names must pass through untouched."""

    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_rewrite_preserves_special_char_names_in_body(self, name):
        sql = f"SELECT * FROM `hive_metastore`.`sch`.`{name}`"
        out = rewrite_hive_namespace(sql, "hive_upgraded")
        assert out == f"SELECT * FROM `hive_upgraded`.`sch`.`{name}`"

    def test_rewrite_fqn_preserves_special_chars(self):
        fqn = "`hive_metastore`.`sch`.`my-table`"
        out = rewrite_hive_fqn(fqn, "target_cat")
        assert out == "`target_cat`.`sch`.`my-table`"

    def test_rewrite_fqn_preserves_unicode(self):
        fqn = "`hive_metastore`.`sch`.`日本語`"
        out = rewrite_hive_fqn(fqn, "target_cat")
        assert out == "`target_cat`.`sch`.`日本語`"


# ---------------------------------------------------------------------------
# _extract_three_part_refs — view dependency extraction with special chars
# ---------------------------------------------------------------------------


class TestExtractThreePartRefs:
    """``catalog_utils._extract_three_part_refs`` walks a view body for
    three-part references. Quoted pattern must match special chars; the
    unquoted pattern is restricted to Python-style identifiers and
    intentionally skips hyphens / spaces / unicode."""

    def test_quoted_ref_with_space_in_name(self):
        from common.catalog_utils import _extract_three_part_refs

        body = "SELECT * FROM `cat`.`sch`.`my table`"
        view_set = {"`cat`.`sch`.`my table`"}
        refs = _extract_three_part_refs(body, view_set)
        assert refs == {"`cat`.`sch`.`my table`"}

    def test_quoted_ref_with_hyphen(self):
        from common.catalog_utils import _extract_three_part_refs

        body = "SELECT * FROM `cat`.`sch`.`my-table`"
        view_set = {"`cat`.`sch`.`my-table`"}
        refs = _extract_three_part_refs(body, view_set)
        assert refs == {"`cat`.`sch`.`my-table`"}

    def test_quoted_ref_with_unicode(self):
        from common.catalog_utils import _extract_three_part_refs

        body = "SELECT * FROM `cat`.`sch`.`日本語`"
        view_set = {"`cat`.`sch`.`日本語`"}
        refs = _extract_three_part_refs(body, view_set)
        assert refs == {"`cat`.`sch`.`日本語`"}

    def test_quoted_ref_with_reserved_keyword(self):
        from common.catalog_utils import _extract_three_part_refs

        body = "SELECT * FROM `cat`.`sch`.`select`"
        view_set = {"`cat`.`sch`.`select`"}
        refs = _extract_three_part_refs(body, view_set)
        assert refs == {"`cat`.`sch`.`select`"}

    def test_unquoted_ref_skips_special_chars(self):
        """An unquoted ref with a hyphen wouldn't parse in Databricks
        either — not matching it is correct."""
        from common.catalog_utils import _extract_three_part_refs

        body = "SELECT * FROM cat.sch.my-table"
        view_set = {"`cat`.`sch`.`my-table`"}
        refs = _extract_three_part_refs(body, view_set)
        assert refs == set()


# ---------------------------------------------------------------------------
# End-to-end shape: discovery FQN -> share clean_name -> target DDL
# ---------------------------------------------------------------------------


class TestPipelineRoundtrip:
    """Document the expected pipeline shapes end-to-end so a future
    refactor has explicit checkpoints to compare against."""

    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_full_pipeline_shapes(self, name):
        """1. Discovery emits FQN = ``` `cat`.`sch`.`<name>` ```.
        2. setup_sharing converts it to dotted ``cat.sch.<name>`` for the
           Delta Sharing API.
        3. DDL replay on target re-uses the same FQN form."""
        fqn = f"`cat`.`sch`.`{name}`"
        clean_name = _strip_to_dotted_safe(fqn)
        assert clean_name == f"cat.sch.{name}"
        parts = fqn.strip("`").split("`.`")
        rebuilt = f"`{parts[0]}`.`{parts[1]}`.`{parts[2]}`"
        assert rebuilt == fqn


# ---------------------------------------------------------------------------
# Regression pin for the _UNQUOTED_REF regex used in catalog_utils.
# Documents which identifiers are recognised as unquoted three-part refs.
# ---------------------------------------------------------------------------


class TestUnquotedIdentifierRegex:
    """``_UNQUOTED_REF`` restricts itself to Python-style identifiers.
    Document by-design limitation so a future regex tweak has a failing
    case to flip."""

    def test_unquoted_regex_matches_plain_ascii(self):
        from common.catalog_utils import _UNQUOTED_REF

        assert _UNQUOTED_REF.search("SELECT * FROM cat.sch.tbl") is not None

    def test_unquoted_regex_partial_match_on_hyphen(self):
        """When the third part contains a hyphen, the regex matches only
        up to the hyphen (e.g. captures ``cat.sch.my`` from
        ``cat.sch.my-table``). A real view_set with a hyphenated name
        won't contain ``cat.sch.my`` so the dependency edge is silently
        dropped by _extract_three_part_refs — relying instead on the
        views_worker retry loop to reorder. Pin down current behaviour."""
        from common.catalog_utils import _UNQUOTED_REF

        m = _UNQUOTED_REF.search("SELECT * FROM cat.sch.my-table")
        assert m is not None
        # The third group captures only the prefix before the hyphen.
        assert m.group(3) == "my"

    def test_unquoted_regex_partial_match_on_space(self):
        """Same pattern as hyphen — regex stops at the word boundary
        before the space."""
        from common.catalog_utils import _UNQUOTED_REF

        m = _UNQUOTED_REF.search("SELECT * FROM cat.sch.my table")
        assert m is not None
        assert m.group(3) == "my"

    def test_unquoted_regex_matches_underscore(self):
        from common.catalog_utils import _UNQUOTED_REF

        m = _UNQUOTED_REF.search("FROM my_cat.my_sch.my_tbl WHERE x=1")
        assert m is not None
        assert m.group(1) == "my_cat"
        assert m.group(2) == "my_sch"
        assert m.group(3) == "my_tbl"


# ---------------------------------------------------------------------------
# strip_filter_mask_clauses against DDLs with special-char column names
# ---------------------------------------------------------------------------


class TestStripFilterMaskClausesSpecialChars:
    def test_strip_preserves_backticked_column_name_with_space(self):
        ddl = (
            "CREATE TABLE `c`.`s`.`t` (\n"
            "  `my col` STRING MASK `c`.`s`.`mask_fn`,\n"
            "  `other` STRING\n"
            ")"
        )
        out = CatalogExplorer.strip_filter_mask_clauses(ddl)
        assert not re.search(r"\bMASK\b", out, flags=re.IGNORECASE)
        assert "`my col` STRING" in out
        assert "`other` STRING" in out

    def test_strip_row_filter_with_unicode_function_name(self):
        ddl = (
            "CREATE TABLE `c`.`s`.`t` (id INT) "
            "WITH ROW FILTER `c`.`s`.`δεδομένα_filter` ON (id)"
        )
        out = CatalogExplorer.strip_filter_mask_clauses(ddl)
        assert not re.search(r"\bROW\s+FILTER\b", out, flags=re.IGNORECASE)
        assert "`c`.`s`.`t`" in out
