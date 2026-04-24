from unittest.mock import MagicMock, patch

from common.config import MigrationConfig
from discovery.discovery import run


def _make_config(**overrides) -> MigrationConfig:
    defaults = dict(
        source_workspace_url="https://source.test",
        target_workspace_url="https://target.test",
        spn_client_id="test-id",
        spn_secret_scope="scope",
        spn_secret_key="key",
        catalog_filter=["test_catalog"],
    )
    defaults.update(overrides)
    return MigrationConfig(**defaults)


class TestDiscovery:
    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_discovers_objects(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()

        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["test_catalog"]
        explorer.list_schemas.return_value = ["schema_a"]
        explorer.classify_tables.return_value = [
            {
                "fqn": "`test_catalog`.`schema_a`.`t1`",
                "object_type": "managed_table",
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
            },
        ]
        explorer.detect_dlt_managed.return_value = (False, None)
        explorer.get_table_row_count.return_value = 100
        explorer.get_table_size_bytes.return_value = 5000
        explorer.get_create_statement.return_value = "CREATE TABLE t1 (...)"
        explorer.list_functions.return_value = ["`test_catalog`.`schema_a`.`fn1`"]
        explorer.get_function_ddl.return_value = "CREATE FUNCTION fn1..."
        explorer.list_volumes.return_value = [{"fqn": "`test_catalog`.`schema_a`.`vol1`", "volume_type": "MANAGED"}]

        inventory = run(dbutils, spark)

        assert len(inventory) == 3
        types = {obj["object_type"] for obj in inventory}
        assert types == {"managed_table", "function", "volume"}

        tracker = mock_tracker_cls.return_value
        tracker.init_tracking_tables.assert_called_once()
        tracker.write_discovery_inventory.assert_called_once()

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_empty_catalog(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()

        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["test_catalog"]
        explorer.list_schemas.return_value = []

        inventory = run(dbutils, spark)
        assert inventory == []


class TestFilterSemantics:
    """1.9 catalog_filter / schema_filter semantics.

    The two filters narrow the discovery scope in distinct places:
      - ``catalog_filter`` is passed through to ``explorer.list_catalogs``
        as its ``filter_list`` argument — the CatalogExplorer is the one
        that enforces it, so the contract is just "the config value
        reaches that call".
      - ``schema_filter`` is applied locally in ``_discover_uc`` as an
        in-place Python filter after ``explorer.list_schemas``.

    These tests exercise both paths via the module-level ``run()`` with
    mocked explorer so any regression in the wiring shows up loud.
    """

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_catalog_filter_reaches_list_catalogs(
        self,
        mock_explorer_cls,
        mock_tracker_cls,
        mock_auth_cls,
        mock_from_file,
    ):
        """``config.catalog_filter`` must be passed as ``filter_list=``
        to ``explorer.list_catalogs``. Without this, the filter is
        silently ignored and every catalog is enumerated."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(
            catalog_filter=["cat_a", "cat_b"],
        )
        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = []

        run(dbutils, spark)

        # The ``filter_list`` kwarg must be exactly the configured filter.
        call_kwargs = explorer.list_catalogs.call_args.kwargs
        assert call_kwargs.get("filter_list") == ["cat_a", "cat_b"]

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_empty_catalog_filter_passes_none_to_list_catalogs(
        self,
        mock_explorer_cls,
        mock_tracker_cls,
        mock_auth_cls,
        mock_from_file,
    ):
        """Empty ``catalog_filter`` must become ``filter_list=None`` so
        the explorer returns everything — passing an empty list would
        match nothing."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(catalog_filter=[])
        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = []

        run(dbutils, spark)

        call_kwargs = explorer.list_catalogs.call_args.kwargs
        assert call_kwargs.get("filter_list") is None

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_schema_filter_narrows_schemas_per_catalog(
        self,
        mock_explorer_cls,
        mock_tracker_cls,
        mock_auth_cls,
        mock_from_file,
    ):
        """``schema_filter`` is applied after ``list_schemas`` as a
        Python-level intersect — only schemas present in both the
        discovered list AND the filter should be iterated."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(
            catalog_filter=["cat_a"],
            schema_filter=["schema_keep"],
        )
        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["cat_a"]
        # Source has three schemas; only schema_keep should be processed.
        explorer.list_schemas.return_value = [
            "schema_keep",
            "schema_drop_a",
            "schema_drop_b",
        ]
        explorer.classify_tables.return_value = []
        explorer.list_functions.return_value = []
        explorer.list_volumes.return_value = []

        run(dbutils, spark)

        # classify_tables must be called once (for schema_keep only),
        # not three times (once per source schema).
        call_schemas = [c.args[1] for c in explorer.classify_tables.call_args_list]
        assert call_schemas == ["schema_keep"]

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_empty_schema_filter_includes_all_schemas(
        self,
        mock_explorer_cls,
        mock_tracker_cls,
        mock_auth_cls,
        mock_from_file,
    ):
        """Empty ``schema_filter`` must leave the schema list untouched —
        not accidentally filter to zero."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(
            catalog_filter=["cat_a"],
            schema_filter=[],
        )
        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["cat_a"]
        explorer.list_schemas.return_value = ["schema_a", "schema_b"]
        explorer.classify_tables.return_value = []
        explorer.list_functions.return_value = []
        explorer.list_volumes.return_value = []

        run(dbutils, spark)

        # All schemas must be iterated.
        call_schemas = [c.args[1] for c in explorer.classify_tables.call_args_list]
        assert set(call_schemas) == {"schema_a", "schema_b"}

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_schema_filter_with_no_matches_discovers_nothing(
        self,
        mock_explorer_cls,
        mock_tracker_cls,
        mock_auth_cls,
        mock_from_file,
    ):
        """``schema_filter`` naming a schema that doesn't exist on source
        must produce an empty discovery inventory — not fall through to
        "discover everything"."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(
            catalog_filter=["cat_a"],
            schema_filter=["nonexistent"],
        )
        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["cat_a"]
        explorer.list_schemas.return_value = ["schema_a", "schema_b"]
        explorer.classify_tables.return_value = []
        explorer.list_functions.return_value = []
        explorer.list_volumes.return_value = []

        inventory = run(dbutils, spark)

        # No schema iterated, therefore no classify_tables calls.
        explorer.classify_tables.assert_not_called()
        assert inventory == []


class TestForeignCatalogExclusion:
    """Foreign catalogs must be excluded from the UC schema/table iteration
    because ``classify_tables`` queries ``<catalog>.information_schema.tables``
    which — on a FOREIGN_CATALOG — is routed via JDBC to the remote source
    and fails on UC-only columns (``data_source_format``). The foreign
    catalog is still captured as governance metadata via
    ``list_foreign_catalogs`` in the same discovery run.
    """

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_foreign_catalog_skipped_from_schema_iteration(
        self,
        mock_explorer_cls,
        mock_tracker_cls,
        mock_auth_cls,
        mock_from_file,
    ):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(catalog_filter=[])

        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["managed_cat", "foreign_cat"]
        explorer.list_foreign_catalog_names.return_value = {"foreign_cat"}
        explorer.list_schemas.return_value = ["schema_a"]
        explorer.classify_tables.return_value = []
        explorer.list_functions.return_value = []
        explorer.list_volumes.return_value = []
        explorer.list_foreign_catalogs.return_value = [
            {"catalog_name": "foreign_cat", "connection_name": "sqlsrv_conn", "options": {}, "comment": None},
        ]

        inventory = run(dbutils, spark)

        # The managed catalog should be iterated; the foreign one must not.
        list_schemas_catalogs = [c.args[0] for c in explorer.list_schemas.call_args_list]
        assert list_schemas_catalogs == ["managed_cat"]
        classify_catalogs = [c.args[0] for c in explorer.classify_tables.call_args_list]
        assert "foreign_cat" not in classify_catalogs

        # The foreign catalog is still emitted as a governance row.
        foreign_rows = [r for r in inventory if r["object_type"] == "foreign_catalog"]
        assert len(foreign_rows) == 1
        assert foreign_rows[0]["object_name"] == "foreign_cat"

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_no_foreign_catalogs_is_noop(
        self,
        mock_explorer_cls,
        mock_tracker_cls,
        mock_auth_cls,
        mock_from_file,
    ):
        """Empty foreign-catalog set must not change the iteration — the
        exclusion branch is purely additive."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(catalog_filter=[])

        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["managed_a", "managed_b"]
        explorer.list_foreign_catalog_names.return_value = set()
        explorer.list_schemas.return_value = []
        explorer.list_foreign_catalogs.return_value = []

        run(dbutils, spark)

        list_schemas_catalogs = [c.args[0] for c in explorer.list_schemas.call_args_list]
        assert list_schemas_catalogs == ["managed_a", "managed_b"]


class TestToolOwnedCatalogs:
    """_tool_owned_catalogs ensures the migration tool never tries to
    migrate its own tracking / share-consumer state."""

    def test_includes_tracking_catalog(self):
        from discovery.discovery import _tool_owned_catalogs

        config = MagicMock()
        config.tracking_catalog = "migration_tracking"
        excluded = _tool_owned_catalogs(config)
        assert "migration_tracking" in excluded

    def test_includes_share_consumer(self):
        from discovery.discovery import _tool_owned_catalogs

        config = MagicMock()
        config.tracking_catalog = "migration_tracking"
        excluded = _tool_owned_catalogs(config)
        # share-consumer is named ``{cp_migration_share}_consumer`` — hardcoded.
        assert "cp_migration_share_consumer" in excluded

    def test_respects_custom_tracking_catalog_name(self):
        """If the operator configures a non-default tracking_catalog,
        excludes that one instead of the default."""
        from discovery.discovery import _tool_owned_catalogs

        config = MagicMock()
        config.tracking_catalog = "custom_tracking"
        excluded = _tool_owned_catalogs(config)
        assert "custom_tracking" in excluded


class TestWarnRlsCmTables:
    """_warn_rls_cm_tables prints a prominent banner listing tables with
    row filter or column mask. Operators see this in the discovery run log
    and decide whether to migrate RLS/CM governance to ABAC or accept skip."""

    def test_silent_when_no_rls_cm_rows(self, capsys):
        from discovery.discovery import _warn_rls_cm_tables

        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {"object_type": "managed_table", "object_name": "`c`.`s`.`t`"},
            {"object_type": "view", "object_name": "`c`.`s`.`v`"},
        ]
        _warn_rls_cm_tables(rows, config)
        captured = capsys.readouterr()
        assert "TABLES WITH ROW FILTER" not in captured.out
        assert "managed_sensitive" not in captured.out

    def test_fires_on_row_filter(self, capsys):
        from discovery.discovery import _warn_rls_cm_tables

        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {
                "object_type": "row_filter",
                "object_name": "`c`.`s`.`t`",
                "metadata_json": None,
            },
        ]
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        assert "TABLES WITH ROW FILTER" in out
        assert "`c`.`s`.`t`" in out
        assert "skipped_by_rls_cm_policy" in out

    def test_fires_on_column_mask_via_metadata(self, capsys):
        """column_mask rows have ``object_name`` = ``<fqn>.<col>`` so the
        clean table_fqn must be pulled from metadata_json."""
        import json as _json

        from discovery.discovery import _warn_rls_cm_tables

        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {
                "object_type": "column_mask",
                "object_name": "`c`.`s`.`t`.ssn",
                "metadata_json": _json.dumps({"table_fqn": "`c`.`s`.`t`"}),
            },
        ]
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        assert "TABLES WITH ROW FILTER / COLUMN MASK" in out
        assert "`c`.`s`.`t`" in out

    def test_deduplicates_table_across_rf_and_cm_rows(self, capsys):
        """Same table listed once even if it has both filter and mask."""
        import json as _json

        from discovery.discovery import _warn_rls_cm_tables

        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {"object_type": "row_filter", "object_name": "`c`.`s`.`t`"},
            {
                "object_type": "column_mask",
                "object_name": "`c`.`s`.`t`.ssn",
                "metadata_json": _json.dumps({"table_fqn": "`c`.`s`.`t`"}),
            },
        ]
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        # Table should appear exactly once in the listed bullets.
        assert out.count("- `c`.`s`.`t`") == 1

    def test_calls_out_drop_and_restore_when_flagged(self, capsys):
        """If operator set rls_cm_strategy=drop_and_restore, surface the
        ``NOT YET IMPLEMENTED`` warning so they know setup_sharing will fail."""
        from discovery.discovery import _warn_rls_cm_tables

        config = MagicMock()
        config.rls_cm_strategy = "drop_and_restore"
        rows = [{"object_type": "row_filter", "object_name": "`c`.`s`.`t`"}]
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        assert "NOT YET IMPLEMENTED" in out

    def test_tolerates_malformed_metadata_json(self, capsys):
        from discovery.discovery import _warn_rls_cm_tables

        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {
                "object_type": "column_mask",
                "object_name": "`c`.`s`.`t`.ssn",
                "metadata_json": "not-json",
            },
            {"object_type": "row_filter", "object_name": "`c`.`s`.`other`"},
        ]
        # Shouldn't raise, and still surfaces the row_filter row.
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        assert "`c`.`s`.`other`" in out
