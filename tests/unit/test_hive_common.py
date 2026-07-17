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
    ensure_target_database,
    rewrite_hive_fqn,
    rewrite_hive_namespace,
)


class TestRewriteHiveNamespaceIsIdentity:
    """Like-for-like: DDL is replayed into hive_metastore as-is, so the
    namespace rewrite must be a no-op (target namespace == source)."""

    def test_leaves_hive_metastore_references_untouched(self):
        sql = "CREATE VIEW `hive_metastore`.`s`.`v` AS SELECT * FROM `hive_metastore`.`x`.`y`"
        assert rewrite_hive_namespace(sql) == sql

    def test_unbackticked_references_untouched(self):
        sql = "SELECT * FROM hive_metastore.schema.table"
        assert rewrite_hive_namespace(sql) == sql

    def test_target_arg_ignored_when_passed(self):
        sql = "SELECT * FROM hive_metastore.s.t"
        assert rewrite_hive_namespace(sql, "anything") == sql


class TestRewriteHiveFqnIsIdentity:
    def test_backticked_fqn_unchanged(self):
        fqn = "`hive_metastore`.`s`.`t`"
        assert rewrite_hive_fqn(fqn) == fqn

    def test_dotted_fqn_unchanged(self):
        assert rewrite_hive_fqn("hive_metastore.s.t") == "hive_metastore.s.t"


class TestEnsureTargetDatabase:
    def test_issues_create_database_in_hive_metastore(self):
        spark = MagicMock()
        ensure_target_database(spark, "sch")
        calls = [c.args[0] for c in spark.sql.call_args_list]
        assert any("CREATE DATABASE IF NOT EXISTS `hive_metastore`.`sch`" in s for s in calls)
        assert not any("CREATE CATALOG" in s for s in calls)


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
