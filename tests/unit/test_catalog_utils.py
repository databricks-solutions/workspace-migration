from unittest.mock import MagicMock

from common.catalog_utils import CatalogExplorer


def _row(**kwargs):
    """Create a MagicMock that behaves like a Spark Row with named attributes."""
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


class TestCatalogUtils:
    """Tests for CatalogExplorer."""

    # ------------------------------------------------------------------
    # list_catalogs
    # ------------------------------------------------------------------

    def test_list_catalogs_filters(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(catalog="catalog_a"),
            _row(catalog="catalog_b"),
            _row(catalog="catalog_c"),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())

        result = explorer.list_catalogs(filter_list=["catalog_a", "catalog_c"])
        assert result == ["catalog_a", "catalog_c"]

    def test_list_catalogs_excludes_system(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(catalog="my_catalog"),
            _row(catalog="system"),
            _row(catalog="hive_metastore"),
            _row(catalog="__databricks_internal"),
            _row(catalog="samples"),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())

        result = explorer.list_catalogs()
        assert result == ["my_catalog"]

    def test_list_catalogs_no_filter(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(catalog="cat_x"),
            _row(catalog="cat_y"),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())

        result = explorer.list_catalogs()
        assert result == ["cat_x", "cat_y"]

    # ------------------------------------------------------------------
    # classify_tables
    # ------------------------------------------------------------------

    def test_classify_tables(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(table_name="orders", table_type="MANAGED", data_source_format="DELTA"),
            _row(table_name="ext_events", table_type="EXTERNAL", data_source_format="PARQUET"),
            _row(table_name="v_summary", table_type="VIEW", data_source_format=None),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())

        result = explorer.classify_tables("my_cat", "my_schema")

        assert len(result) == 3
        assert result[0] == {
            "fqn": "`my_cat`.`my_schema`.`orders`",
            "object_type": "managed_table",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
        }
        assert result[1]["object_type"] == "external_table"
        assert result[2]["object_type"] == "view"

    # ------------------------------------------------------------------
    # detect_dlt_managed
    # ------------------------------------------------------------------

    def test_detect_dlt_managed(self, mock_spark):
        detail_row = _row(properties={"pipelines.pipelineId": "abc-123"})
        mock_spark.sql.return_value.first.return_value = detail_row

        explorer = CatalogExplorer(mock_spark, MagicMock())

        is_dlt, pid = explorer.detect_dlt_managed("`cat`.`sch`.`tbl`")
        assert is_dlt is True
        assert pid == "abc-123"

    def test_detect_dlt_managed_not_dlt(self, mock_spark):
        detail_row = _row(properties={})
        mock_spark.sql.return_value.first.return_value = detail_row

        explorer = CatalogExplorer(mock_spark, MagicMock())

        is_dlt, pid = explorer.detect_dlt_managed("`cat`.`sch`.`tbl`")
        assert is_dlt is False
        assert pid is None

    def test_detect_dlt_managed_on_view_returns_false(self, mock_spark):
        """DESCRIBE DETAIL fails on views ([EXPECT_TABLE_NOT_VIEW...]).
        detect_dlt_managed must swallow the error and return (False, None)
        rather than propagate — views can't be DLT-managed.
        """
        mock_spark.sql.side_effect = Exception(
            "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'DESCRIBE DETAIL' expects a table"
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())

        is_dlt, pid = explorer.detect_dlt_managed("`cat`.`sch`.`a_view`")
        assert is_dlt is False
        assert pid is None

    # ------------------------------------------------------------------
    # get_function_ddl
    # ------------------------------------------------------------------

    def test_get_function_ddl_builds_full_create_statement(self, mock_spark):
        """Must return a complete CREATE OR REPLACE FUNCTION statement (not just
        the body) — reconstructed from information_schema.routines + parameters.
        """
        routine_row = _row(
            specific_name="double_amount_1234",
            data_type="DOUBLE",
            routine_body="SQL",
            routine_definition="x * 2",
            external_language=None,
        )
        param_rows = [_row(parameter_name="x", data_type="DOUBLE", ordinal_position=1)]

        def sql_side_effect(query):
            result = MagicMock()
            if "information_schema`.`routines" in query:
                result.first.return_value = routine_row
            elif "information_schema`.`parameters" in query:
                result.collect.return_value = param_rows
            else:
                result.first.return_value = None
                result.collect.return_value = []
            return result

        mock_spark.sql.side_effect = sql_side_effect
        explorer = CatalogExplorer(mock_spark, MagicMock())

        ddl = explorer.get_function_ddl("`cat`.`sch`.`double_amount`")

        assert ddl.upper().startswith("CREATE OR REPLACE FUNCTION"), (
            f"Expected full CREATE OR REPLACE FUNCTION statement, got: {ddl!r}"
        )
        assert "double_amount" in ddl
        assert "RETURNS DOUBLE" in ddl.upper()
        assert "x DOUBLE" in ddl  # parameter signature
        assert "x * 2" in ddl  # body
        # SQL UDF uses RETURN form (not AS $$)
        assert "RETURN x * 2" in ddl
        assert "$$" not in ddl

    def test_get_function_ddl_python_udf_uses_dollar_quote(self, mock_spark):
        """Python UDFs must be wrapped with ``LANGUAGE PYTHON AS $$...$$`` —
        the SQL-UDF ``RETURN`` form fails to parse on target.
        """
        routine_row = _row(
            specific_name="py_double_1234",
            data_type="DOUBLE",
            routine_body="EXTERNAL",
            routine_definition="def handler(x):\n    return x * 2\nreturn handler(x)",
            external_language="PYTHON",
        )
        param_rows = [_row(parameter_name="x", data_type="DOUBLE", ordinal_position=1)]

        def sql_side_effect(query):
            result = MagicMock()
            if "information_schema`.`routines" in query:
                result.first.return_value = routine_row
            elif "information_schema`.`parameters" in query:
                result.collect.return_value = param_rows
            else:
                result.first.return_value = None
                result.collect.return_value = []
            return result

        mock_spark.sql.side_effect = sql_side_effect
        explorer = CatalogExplorer(mock_spark, MagicMock())

        ddl = explorer.get_function_ddl("`cat`.`sch`.`py_double`")

        assert ddl.upper().startswith("CREATE OR REPLACE FUNCTION")
        assert "LANGUAGE PYTHON" in ddl.upper()
        assert "AS $$" in ddl
        # Closing $$ present after the AS $$
        _, after = ddl.split("AS $$", 1)
        assert "$$" in after
        assert "def handler" in ddl
        assert "return handler(x)" in ddl
        # Python UDF must NOT use the SQL RETURN form for the body
        assert " RETURN def" not in ddl
        assert " RETURN return" not in ddl

    # ------------------------------------------------------------------
    # get_create_statement (for views)
    # ------------------------------------------------------------------

    def test_get_create_statement_for_view_uses_information_schema(self, mock_spark):
        """Must return a well-formed CREATE OR REPLACE VIEW that includes the
        full catalog.schema.table path — SHOW CREATE TABLE output occasionally
        produces references with an empty catalog, which breaks replay on target.
        """
        view_row = _row(view_definition="SELECT * FROM `cat`.`sch`.`tbl` WHERE amount > 100")

        def sql_side_effect(query):
            result = MagicMock()
            if "information_schema`.`views" in query:
                result.first.return_value = view_row
            else:
                result.first.return_value = _row(createtab_stmt="IGNORED")
            return result

        mock_spark.sql.side_effect = sql_side_effect
        explorer = CatalogExplorer(mock_spark, MagicMock())

        ddl = explorer.get_create_statement("`cat`.`sch`.`my_view`")
        assert ddl.upper().startswith("CREATE OR REPLACE VIEW"), ddl
        assert "`cat`.`sch`.`my_view`" in ddl
        assert "SELECT * FROM" in ddl

    # ------------------------------------------------------------------
    # resolve_view_dependency_order
    # ------------------------------------------------------------------

    def test_resolve_view_dependency_order(self, mock_spark):
        """v_c depends on v_b, v_b depends on v_a -> order: v_a, v_b, v_c.

        Dependencies are now parsed from ``information_schema.views.view_definition``
        since ``view_table_usage`` doesn't exist in UC.
        """
        views = ["`cat`.`sch`.`v_c`", "`cat`.`sch`.`v_b`", "`cat`.`sch`.`v_a`"]

        # view_definition bodies referencing dependencies (unquoted FQN)
        view_rows = [
            _row(table_schema="sch", table_name="v_a", view_definition="SELECT * FROM cat.sch.base_table"),
            _row(table_schema="sch", table_name="v_b", view_definition="SELECT * FROM cat.sch.v_a WHERE x > 0"),
            _row(table_schema="sch", table_name="v_c", view_definition="SELECT * FROM cat.sch.v_b"),
        ]

        def sql_side_effect(query):
            mock_result = MagicMock()
            mock_result.collect.return_value = view_rows
            return mock_result

        mock_spark.sql.side_effect = sql_side_effect

        explorer = CatalogExplorer(mock_spark, MagicMock())
        result = explorer.resolve_view_dependency_order(views)

        assert result.index("`cat`.`sch`.`v_a`") < result.index("`cat`.`sch`.`v_b`")
        assert result.index("`cat`.`sch`.`v_b`") < result.index("`cat`.`sch`.`v_c`")

    def test_resolve_view_dependency_order_with_cycle(self, mock_spark):
        """v_a -> v_b -> v_a (cycle). All views should still appear in output."""
        views = ["`cat`.`sch`.`v_a`", "`cat`.`sch`.`v_b`"]

        view_rows = [
            _row(table_schema="sch", table_name="v_a", view_definition="SELECT * FROM cat.sch.v_b"),
            _row(table_schema="sch", table_name="v_b", view_definition="SELECT * FROM cat.sch.v_a"),
        ]

        def sql_side_effect(query):
            mock_result = MagicMock()
            mock_result.collect.return_value = view_rows
            return mock_result

        mock_spark.sql.side_effect = sql_side_effect

        explorer = CatalogExplorer(mock_spark, MagicMock())
        result = explorer.resolve_view_dependency_order(views)

        # Both views must be present despite the cycle
        assert set(result) == set(views)
        assert len(result) == 2


class TestExtractThreePartRefs:
    """Parse three-part FQN references from a SQL body.

    view_table_usage doesn't exist in UC, so dependency detection parses
    view_definition. Cover: backticked, unquoted, qualified vs unqualified,
    CTEs, SQL keywords that look like names.
    """

    def test_extracts_unquoted_three_part_ref(self):
        from common.catalog_utils import _extract_three_part_refs

        view_set = {"`cat`.`sch`.`upstream`"}
        refs = _extract_three_part_refs(
            "SELECT * FROM cat.sch.upstream WHERE x > 0",
            view_set,
        )
        assert refs == {"`cat`.`sch`.`upstream`"}

    def test_extracts_backticked_three_part_ref(self):
        from common.catalog_utils import _extract_three_part_refs

        view_set = {"`cat`.`sch`.`upstream`"}
        refs = _extract_three_part_refs(
            "SELECT * FROM `cat`.`sch`.`upstream`",
            view_set,
        )
        assert refs == {"`cat`.`sch`.`upstream`"}

    def test_filters_refs_not_in_view_set(self):
        from common.catalog_utils import _extract_three_part_refs

        view_set = {"`cat`.`sch`.`upstream`"}
        refs = _extract_three_part_refs(
            "SELECT * FROM other.catalog.other_table JOIN cat.sch.upstream USING (id)",
            view_set,
        )
        # only the one present in view_set is returned
        assert refs == {"`cat`.`sch`.`upstream`"}

    def test_extracts_multiple_references(self):
        from common.catalog_utils import _extract_three_part_refs

        view_set = {
            "`c1`.`s1`.`t1`",
            "`c2`.`s2`.`t2`",
            "`c3`.`s3`.`t3`",
        }
        refs = _extract_three_part_refs(
            "SELECT * FROM c1.s1.t1 UNION ALL SELECT * FROM c2.s2.t2 UNION ALL SELECT * FROM `c3`.`s3`.`t3`",
            view_set,
        )
        assert refs == view_set

    def test_empty_body_returns_empty(self):
        from common.catalog_utils import _extract_three_part_refs

        assert _extract_three_part_refs("", {"`c`.`s`.`t`"}) == set()
        assert _extract_three_part_refs("   ", {"`c`.`s`.`t`"}) == set()


class TestSqlInLiteral:
    def test_empty_returns_empty_literal(self):
        from common.catalog_utils import _sql_in_literal

        assert _sql_in_literal(set()) == "''"

    def test_escapes_single_quotes(self):
        from common.catalog_utils import _sql_in_literal

        out = _sql_in_literal({"O'Brien"})
        assert out == "'O''Brien'"

    def test_comma_separates(self):
        from common.catalog_utils import _sql_in_literal

        # set ordering isn't guaranteed; check as set of literals
        out = _sql_in_literal({"a", "b"})
        tokens = {t.strip() for t in out.split(",")}
        assert tokens == {"'a'", "'b'"}


class TestStripFilterMaskClauses:
    """Tests for CatalogExplorer.strip_filter_mask_clauses — removes
    WITH ROW FILTER and inline MASK clauses so external_table_worker /
    managed_table_worker can replay the CREATE TABLE DDL before the
    filter/mask functions are migrated to target (they don't exist on
    target yet; row_filters_worker / column_masks_worker apply them
    later).
    """

    def test_strips_trailing_with_row_filter(self):
        ddl = (
            "CREATE TABLE `c`.`s`.`t` (id INT, region STRING) "
            "USING delta "
            "LOCATION 'abfss://...' "
            "WITH ROW FILTER `c`.`s`.`region_filter` ON (region)"
        )
        out = CatalogExplorer.strip_filter_mask_clauses(ddl)
        assert "ROW FILTER" not in out
        assert "region_filter" not in out
        assert "USING delta" in out
        assert "LOCATION" in out

    def test_strips_inline_mask_on_column(self):
        ddl = "CREATE TABLE `c`.`s`.`t` (id INT MASK `c`.`s`.`mask_id`,region STRING) USING delta"
        out = CatalogExplorer.strip_filter_mask_clauses(ddl)
        assert "MASK" not in out
        assert "mask_id" not in out
        assert "id INT" in out
        assert "region STRING" in out

    def test_strips_inline_mask_with_using(self):
        ddl = "CREATE TABLE `c`.`s`.`t` (id INT MASK `c`.`s`.`mask_id` USING (region),region STRING) USING delta"
        out = CatalogExplorer.strip_filter_mask_clauses(ddl)
        assert "MASK" not in out
        assert "USING (region)" not in out
        assert "id INT" in out

    def test_strips_both_filter_and_mask(self):
        ddl = (
            "CREATE TABLE `c`.`s`.`t` ("
            "id INT MASK `c`.`s`.`m`,"
            "region STRING"
            ") USING delta "
            "WITH ROW FILTER `c`.`s`.`rf` ON (region)"
        )
        out = CatalogExplorer.strip_filter_mask_clauses(ddl)
        assert "ROW FILTER" not in out
        assert "MASK" not in out
        assert "id INT" in out
        assert "region STRING" in out
        assert "USING delta" in out

    def test_passthrough_when_no_filter_mask(self):
        ddl = "CREATE TABLE `c`.`s`.`t` (id INT) USING delta LOCATION 'abfss://...'"
        out = CatalogExplorer.strip_filter_mask_clauses(ddl)
        assert out == ddl


class TestGetFunctionDdlComplexSignatures:
    """CREATE FUNCTION reconstruction must handle complex parameter + return
    types — arrays, structs, maps, multi-param signatures. The tool's
    ``get_function_ddl`` reads information_schema which already flattens
    these to their SQL type strings (``ARRAY<STRING>``, ``STRUCT<...>``),
    but we lock in that the reconstruction doesn't drop them.
    """

    def _mock_function(self, mock_spark, *, routine_row, param_rows):
        def sql_side_effect(query):
            result = MagicMock()
            if "information_schema`.`routines" in query:
                result.first.return_value = routine_row
            elif "information_schema`.`parameters" in query:
                result.collect.return_value = param_rows
            else:
                result.first.return_value = None
                result.collect.return_value = []
            return result

        mock_spark.sql.side_effect = sql_side_effect

    def test_array_parameter_and_return(self, mock_spark):
        """SQL UDF taking ``ARRAY<STRING>`` and returning ``ARRAY<STRING>``."""
        self._mock_function(
            mock_spark,
            routine_row=_row(
                specific_name="normalize_tags_1",
                data_type="ARRAY<STRING>",
                routine_body="SQL",
                routine_definition="transform(x, v -> lower(v))",
                external_language=None,
            ),
            param_rows=[
                _row(parameter_name="x", data_type="ARRAY<STRING>", ordinal_position=1),
            ],
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`c`.`s`.`normalize_tags`")

        assert "x ARRAY<STRING>" in ddl
        assert "RETURNS ARRAY<STRING>" in ddl.upper()
        assert "transform(x, v -> lower(v))" in ddl

    def test_struct_parameter(self, mock_spark):
        """SQL UDF taking ``STRUCT<id: INT, name: STRING>``."""
        self._mock_function(
            mock_spark,
            routine_row=_row(
                specific_name="fmt_person_1",
                data_type="STRING",
                routine_body="SQL",
                routine_definition="concat(p.id, '-', p.name)",
                external_language=None,
            ),
            param_rows=[
                _row(
                    parameter_name="p",
                    data_type="STRUCT<id: INT, name: STRING>",
                    ordinal_position=1,
                ),
            ],
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`c`.`s`.`fmt_person`")

        assert "p STRUCT<id: INT, name: STRING>" in ddl
        assert "RETURNS STRING" in ddl.upper()

    def test_map_parameter(self, mock_spark):
        """SQL UDF taking ``MAP<STRING, INT>``."""
        self._mock_function(
            mock_spark,
            routine_row=_row(
                specific_name="sum_vals_1",
                data_type="INT",
                routine_body="SQL",
                routine_definition="aggregate(map_values(m), 0, (a, v) -> a + v)",
                external_language=None,
            ),
            param_rows=[
                _row(parameter_name="m", data_type="MAP<STRING, INT>", ordinal_position=1),
            ],
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`c`.`s`.`sum_vals`")

        assert "m MAP<STRING, INT>" in ddl
        assert "RETURNS INT" in ddl.upper()

    def test_multi_parameter_signature_preserves_order(self, mock_spark):
        """Multiple parameters must appear in ordinal_position order —
        information_schema ORDER BY already handles this on the query
        side, and the DDL must concatenate them comma-separated without
        dropping order."""
        self._mock_function(
            mock_spark,
            routine_row=_row(
                specific_name="calc_fee_1",
                data_type="DOUBLE",
                routine_body="SQL",
                routine_definition="amount * rate + fixed",
                external_language=None,
            ),
            param_rows=[
                _row(parameter_name="amount", data_type="DOUBLE", ordinal_position=1),
                _row(parameter_name="rate", data_type="DOUBLE", ordinal_position=2),
                _row(parameter_name="fixed", data_type="DOUBLE", ordinal_position=3),
            ],
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`c`.`s`.`calc_fee`")

        # Parameters in original order, comma-separated
        assert "amount DOUBLE, rate DOUBLE, fixed DOUBLE" in ddl

    def test_python_udf_with_array_return(self, mock_spark):
        """Python UDF returning ``ARRAY<STRING>`` — uses ``AS $$...$$``
        form (not ``RETURN``) and carries the Python body verbatim."""
        body = "return [x.upper() for x in values]"
        self._mock_function(
            mock_spark,
            routine_row=_row(
                specific_name="upper_all_1",
                data_type="ARRAY<STRING>",
                routine_body="EXTERNAL",
                routine_definition=body,
                external_language="PYTHON",
            ),
            param_rows=[
                _row(parameter_name="values", data_type="ARRAY<STRING>", ordinal_position=1),
            ],
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`c`.`s`.`upper_all`")

        assert "LANGUAGE PYTHON" in ddl
        assert "AS $$" in ddl
        assert body in ddl
        # SQL-UDF ``RETURN`` keyword must NOT appear for Python UDFs.
        assert "RETURN return" not in ddl
        # Return type preserved.
        assert "RETURNS ARRAY<STRING>" in ddl.upper()

    def test_zero_parameter_function(self, mock_spark):
        """Parameterless SQL UDF — no parameters, empty sig."""
        self._mock_function(
            mock_spark,
            routine_row=_row(
                specific_name="current_time_1",
                data_type="TIMESTAMP",
                routine_body="SQL",
                routine_definition="current_timestamp()",
                external_language=None,
            ),
            param_rows=[],
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`c`.`s`.`current_time`")
        # Empty parameter signature — just `()`
        assert "()" in ddl
        assert "RETURNS TIMESTAMP" in ddl.upper()
        assert "RETURN current_timestamp()" in ddl


class TestPythonUdfEnvironmentClause:
    """Python UDFs with an ``ENVIRONMENT`` spec must carry that clause
    into the replayed DDL — otherwise pip dependencies silently drop
    and the UDF fails at first invocation on target."""

    @staticmethod
    def _mock_sql(mock_spark, *, routine_row, param_rows, show_create_ddl=None):
        def sql_side_effect(query):
            result = MagicMock()
            if "SHOW CREATE FUNCTION" in query.upper():
                if show_create_ddl is None:
                    result.collect.return_value = []
                else:
                    row = MagicMock()
                    row.__getitem__ = lambda self, i: show_create_ddl if i == 0 else None
                    row.__len__ = lambda self: 1
                    result.collect.return_value = [row]
            elif "information_schema`.`routines" in query:
                result.first.return_value = routine_row
            elif "information_schema`.`parameters" in query:
                result.collect.return_value = param_rows
            else:
                result.first.return_value = None
                result.collect.return_value = []
            return result

        mock_spark.sql.side_effect = sql_side_effect

    def _python_routine_row(self):
        return _row(
            specific_name="py_fn_1",
            data_type="DOUBLE",
            routine_body="EXTERNAL",
            routine_definition="return x * 2",
            external_language="PYTHON",
        )

    def test_environment_clause_appended_when_present(self, mock_spark):
        show_create = (
            "CREATE FUNCTION `cat`.`sch`.`py_fn`(x DOUBLE) "
            "RETURNS DOUBLE "
            "LANGUAGE PYTHON "
            "ENVIRONMENT (\n"
            "  dependencies = ['pandas==2.0.0', 'numpy'],\n"
            "  environment_version = 'None'\n"
            ") "
            "AS $$return x * 2$$"
        )
        self._mock_sql(
            mock_spark,
            routine_row=self._python_routine_row(),
            param_rows=[_row(parameter_name="x", data_type="DOUBLE", ordinal_position=1)],
            show_create_ddl=show_create,
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`cat`.`sch`.`py_fn`")

        assert "ENVIRONMENT (" in ddl
        assert "pandas==2.0.0" in ddl
        assert "environment_version = 'None'" in ddl
        # The clause lands between LANGUAGE PYTHON and AS $$.
        lang_idx = ddl.find("LANGUAGE PYTHON")
        env_idx = ddl.find("ENVIRONMENT (")
        body_idx = ddl.find("AS $$")
        assert lang_idx < env_idx < body_idx

    def test_no_environment_clause_when_absent(self, mock_spark):
        """Python UDF without an ENVIRONMENT — DDL carries no ``ENVIRONMENT`` token."""
        show_create = (
            "CREATE FUNCTION `cat`.`sch`.`py_fn`(x DOUBLE) "
            "RETURNS DOUBLE LANGUAGE PYTHON AS $$return x * 2$$"
        )
        self._mock_sql(
            mock_spark,
            routine_row=self._python_routine_row(),
            param_rows=[_row(parameter_name="x", data_type="DOUBLE", ordinal_position=1)],
            show_create_ddl=show_create,
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`cat`.`sch`.`py_fn`")
        assert "ENVIRONMENT" not in ddl.upper()

    def test_show_create_unavailable_falls_through(self, mock_spark):
        """If SHOW CREATE FUNCTION raises (older runtime / edge case), the
        synthesized DDL is returned unchanged — no ENVIRONMENT block."""

        def sql_side_effect(query):
            result = MagicMock()
            if "SHOW CREATE FUNCTION" in query.upper():
                raise RuntimeError("SHOW CREATE FUNCTION not supported")
            if "information_schema`.`routines" in query:
                result.first.return_value = self._python_routine_row()
            elif "information_schema`.`parameters" in query:
                result.collect.return_value = [_row(parameter_name="x", data_type="DOUBLE", ordinal_position=1)]
            else:
                result.first.return_value = None
                result.collect.return_value = []
            return result

        mock_spark.sql.side_effect = sql_side_effect
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`cat`.`sch`.`py_fn`")
        assert "LANGUAGE PYTHON" in ddl
        assert "ENVIRONMENT" not in ddl.upper()

    def test_environment_with_nested_parens_balanced(self, mock_spark):
        """ENVIRONMENT clause may contain nested parens (e.g., a tuple
        expression in dependencies). The extractor must balance them
        correctly and close at the matching outer paren."""
        show_create = (
            "CREATE FUNCTION `c`.`s`.`f`(x DOUBLE) "
            "RETURNS DOUBLE "
            "LANGUAGE PYTHON "
            "ENVIRONMENT (\n"
            "  dependencies = ['pkg (extra)'],\n"
            "  environment_version = 'None'\n"
            ") "
            "AS $$return x$$"
        )
        self._mock_sql(
            mock_spark,
            routine_row=self._python_routine_row(),
            param_rows=[_row(parameter_name="x", data_type="DOUBLE", ordinal_position=1)],
            show_create_ddl=show_create,
        )
        explorer = CatalogExplorer(mock_spark, MagicMock())
        ddl = explorer.get_function_ddl("`c`.`s`.`f`")
        assert "pkg (extra)" in ddl
        # Only one AS $$ — we haven't prematurely closed the ENVIRONMENT block.
        assert ddl.count("AS $$") == 1


class TestListForeignCatalogs:
    """list_foreign_catalogs() — M4 caches per instance, M5 propagates errors."""

    def _explorer_with_one_foreign(self):
        auth_mgr = MagicMock()
        fc = MagicMock()
        fc.name = "fc1"
        fc.catalog_type = "FOREIGN_CATALOG"
        fc.connection_name = "conn1"
        fc.options = {}
        fc.comment = None
        auth_mgr.source_client.catalogs.list.return_value = [fc]
        return CatalogExplorer(MagicMock(), auth_mgr), auth_mgr

    def test_list_foreign_catalogs_caches_result_across_calls(self):
        """M4: repeat calls hit the SDK only once per explorer instance."""
        explorer, auth_mgr = self._explorer_with_one_foreign()

        a = explorer.list_foreign_catalogs()
        b = explorer.list_foreign_catalogs()
        names = explorer.list_foreign_catalog_names()

        assert a == b
        assert names == {"fc1"}
        assert auth_mgr.source_client.catalogs.list.call_count == 1, (
            "list_foreign_catalogs must be memoized per explorer instance"
        )

    def test_list_foreign_catalogs_propagates_permission_denied(self):
        """M5: a permission failure raises rather than silently returning []."""
        import pytest
        from databricks.sdk.errors import PermissionDenied

        auth_mgr = MagicMock()
        auth_mgr.source_client.catalogs.list.side_effect = PermissionDenied(
            "User does not have USE CATALOG"
        )
        explorer = CatalogExplorer(MagicMock(), auth_mgr)

        with pytest.raises(PermissionDenied):
            explorer.list_foreign_catalogs()


class TestListHiveFunctions:
    """list_hive_functions parses SHOW USER FUNCTIONS IN <db>. Regression:
    the old 'skip if no dot' filter dropped BARE user-function names, so
    hive_function discovery came back empty even when a UDF existed."""

    def test_keeps_bare_user_function_name(self, mock_spark):
        # SHOW USER FUNCTIONS IN <db> can return the UDF name unqualified.
        mock_spark.sql.return_value.collect.return_value = [_row(function="triple")]
        explorer = CatalogExplorer(mock_spark, MagicMock())
        result = explorer.list_hive_functions("integration_test_hive")
        assert result == ["`hive_metastore`.`integration_test_hive`.`triple`"]

    def test_keeps_qualified_name_for_this_db(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(function="integration_test_hive.triple"),
            _row(function="spark_catalog.integration_test_hive.quad"),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())
        result = explorer.list_hive_functions("integration_test_hive")
        assert result == [
            "`hive_metastore`.`integration_test_hive`.`triple`",
            "`hive_metastore`.`integration_test_hive`.`quad`",
        ]

    def test_drops_function_qualified_to_other_db(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(function="other_db.somefn"),
            _row(function="triple"),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())
        result = explorer.list_hive_functions("integration_test_hive")
        assert result == ["`hive_metastore`.`integration_test_hive`.`triple`"]

    def test_returns_empty_on_error(self, mock_spark):
        mock_spark.sql.side_effect = Exception("SHOW failed")
        explorer = CatalogExplorer(mock_spark, MagicMock())
        assert explorer.list_hive_functions("db") == []
