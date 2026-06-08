"""Unit tests for src/migrate/hive_common.py — shared helpers for the
Hive-to-UC workers. ``rewrite_hive_namespace`` is the critical piece:
it rewrites hive_metastore.x.y references in CREATE VIEW / CREATE
TABLE bodies to ``<target_catalog>.x.y`` so migrated views resolve
on target. An incorrect rewrite here was the root cause of the
TABLE_OR_VIEW_NOT_FOUND fallout we hit mid-session earlier.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from migrate.hive_common import (
    HIVE_CATALOG,
    HIVE_TO_UC_PRIVILEGES,
    ensure_target_catalog_and_schema,
    rewrite_hive_fqn,
    rewrite_hive_namespace,
)


class TestRewriteHiveNamespace:
    def test_rewrites_backticked_hive_metastore_references(self):
        sql = "CREATE VIEW `c`.`s`.`v` AS SELECT * FROM `hive_metastore`.`x`.`y`"
        out = rewrite_hive_namespace(sql, "hive_upgraded")
        assert "`hive_metastore`.`x`.`y`" not in out
        assert "`hive_upgraded`.`x`.`y`" in out

    def test_rewrites_unbackticked_references(self):
        sql = "SELECT * FROM hive_metastore.schema.table"
        out = rewrite_hive_namespace(sql, "hive_upgraded")
        assert "hive_metastore.schema.table" not in out
        assert "hive_upgraded.schema.table" in out

    def test_rewrites_both_forms_in_same_sql(self):
        sql = "SELECT a.col, b.col FROM `hive_metastore`.`s`.`a` a JOIN hive_metastore.s.b b ON a.id = b.id"
        out = rewrite_hive_namespace(sql, "hive_upgraded")
        # Both occurrences rewritten.
        assert "`hive_metastore`" not in out
        assert "hive_metastore." not in out

    def test_does_not_rewrite_references_to_other_catalogs(self):
        """``hive_metastore_v2`` or ``my_hive_metastore`` are different
        catalog names — must not be touched by the rewrite."""
        sql = "SELECT * FROM `hive_metastore_v2`.`s`.`t`"
        out = rewrite_hive_namespace(sql, "hive_upgraded")
        assert "`hive_metastore_v2`.`s`.`t`" in out

    def test_handles_no_hive_references(self):
        sql = "SELECT * FROM `other_catalog`.`s`.`t`"
        out = rewrite_hive_namespace(sql, "hive_upgraded")
        assert out == sql


class TestRewriteHiveFqn:
    def test_backticked_fqn(self):
        assert rewrite_hive_fqn("`hive_metastore`.`s`.`t`", "hive_upgraded") == "`hive_upgraded`.`s`.`t`"

    def test_dotted_fqn(self):
        assert rewrite_hive_fqn("hive_metastore.s.t", "hive_upgraded") == "`hive_upgraded`.`s`.`t`"

    def test_non_hive_fqn_passes_through(self):
        """A non-hive FQN is returned unchanged — ensures the helper
        doesn't mangle UC references that happen to flow through."""
        fqn = "`other`.`s`.`t`"
        assert rewrite_hive_fqn(fqn, "hive_upgraded") == fqn

    def test_malformed_fqn_passes_through(self):
        """2-part or malformed input shouldn't crash."""
        assert rewrite_hive_fqn("just_table", "hive_upgraded") == "just_table"


class TestEnsureTargetCatalogAndSchema:
    def test_issues_create_catalog_and_create_schema(self):
        spark = MagicMock()
        ensure_target_catalog_and_schema(spark, "hive_upgraded", "sch")
        calls = [c.args[0] for c in spark.sql.call_args_list]
        assert any("CREATE CATALOG IF NOT EXISTS `hive_upgraded`" in s for s in calls)
        assert any("CREATE SCHEMA IF NOT EXISTS `hive_upgraded`.`sch`" in s for s in calls)


class TestHiveToUcPrivilegeMap:
    """Lock in the Hive→UC privilege translation. Expansion of this map
    is fine but silently changing a mapping would break grant migration
    for customers relying on the documented behavior."""

    def test_select_maps_to_select(self):
        assert HIVE_TO_UC_PRIVILEGES["SELECT"] == "SELECT"

    def test_read_metadata_maps_to_use_schema(self):
        """Hive's READ_METADATA is conceptually schema browsing — maps
        to UC's USE SCHEMA (not USE CATALOG)."""
        assert HIVE_TO_UC_PRIVILEGES["READ_METADATA"] == "USE SCHEMA"

    def test_usage_maps_to_use_catalog(self):
        assert HIVE_TO_UC_PRIVILEGES["USAGE"] == "USE CATALOG"

    def test_own_is_mapped_but_worker_skips_it(self):
        """OWN is present in the map (for completeness) but hive_grants_
        worker skips it — ownership is transferred via ALTER ... OWNER
        TO, not GRANT. Map entry exists so an operator reading the map
        sees what OWN *would* translate to."""
        assert HIVE_TO_UC_PRIVILEGES["OWN"] == "OWNER"


class TestHiveCatalogConstant:
    def test_hive_catalog_is_hive_metastore(self):
        """Hard-coded — catches a refactor that tries to make this
        configurable, since the rewrite helpers depend on the exact
        string ``hive_metastore``."""
        assert HIVE_CATALOG == "hive_metastore"


class TestConfigureAdlsAccountKey:
    """configure_adls_account_key sets the legacy fs.azure.account.key Hadoop
    conf for hive_metastore-on-ADLS access (serverless can't, classic can)."""

    def _spark_dbutils(self, secret="theKey"):
        from unittest.mock import MagicMock

        spark = MagicMock()
        dbutils = MagicMock()
        dbutils.secrets.get.return_value = secret
        return spark, dbutils

    def test_sets_account_key_for_abfss(self):
        from migrate.hive_common import configure_adls_account_key

        spark, dbutils = self._spark_dbutils()
        ok = configure_adls_account_key(
            spark, dbutils,
            "abfss://external-data@myacct.dfs.core.windows.net/path/t",
        )
        assert ok is True
        spark.conf.set.assert_called_once_with(
            "fs.azure.account.key.myacct.dfs.core.windows.net", "theKey"
        )

    def test_noop_for_non_abfss(self):
        from migrate.hive_common import configure_adls_account_key

        spark, dbutils = self._spark_dbutils()
        for loc in (None, "", "dbfs:/x", "s3://b/k"):
            assert configure_adls_account_key(spark, dbutils, loc) is False
        spark.conf.set.assert_not_called()
        dbutils.secrets.get.assert_not_called()

    def test_returns_false_when_secret_missing(self):
        from migrate.hive_common import configure_adls_account_key

        spark, dbutils = self._spark_dbutils()
        dbutils.secrets.get.side_effect = Exception("no such secret")
        ok = configure_adls_account_key(
            spark, dbutils, "abfss://c@acct.dfs.core.windows.net/t"
        )
        assert ok is False
        spark.conf.set.assert_not_called()

    def test_returns_false_when_conf_rejected(self):
        """Serverless rejects fs.azure.account.key — must not raise."""
        from migrate.hive_common import configure_adls_account_key

        spark, dbutils = self._spark_dbutils()
        spark.conf.set.side_effect = Exception("CONFIG_NOT_AVAILABLE")
        ok = configure_adls_account_key(
            spark, dbutils, "abfss://c@acct.dfs.core.windows.net/t"
        )
        assert ok is False
