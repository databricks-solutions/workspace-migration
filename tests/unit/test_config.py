from __future__ import annotations

from pathlib import Path

import pytest

from common.config import MigrationConfig


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(body)
    return p


class TestMigrationConfig:
    def test_defaults(self):
        config = MigrationConfig(
            source_workspace_url="https://src.azuredatabricks.net",
            target_workspace_url="https://tgt.azuredatabricks.net",
            spn_client_id="client-id",
            spn_secret_scope="scope",
            spn_secret_key="key",
        )
        assert config.catalog_filter == []
        assert config.schema_filter == []
        assert config.tracking_catalog == "migration_tracking"
        assert config.tracking_schema == "cp_migration"
        assert config.dry_run is False
        assert config.batch_size == 50
        assert config.migrate_hive_dbfs_root is False
        assert config.hive_dbfs_staging_path == ""

    def test_from_workspace_file_minimal(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.source_workspace_url == "https://src.azuredatabricks.net"
        assert config.spn_client_id == "client-id"
        # Defaults applied for missing optional fields
        assert config.tracking_catalog == "migration_tracking"
        assert config.dry_run is False
        assert config.migrate_hive_dbfs_root is False

    def test_from_workspace_file_full(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
catalog_filter: "cat_a, cat_b"
schema_filter: ""
tracking_catalog: custom_tracking
tracking_schema: custom_schema
dry_run: true
batch_size: 25
migrate_hive_dbfs_root: true
hive_dbfs_staging_path: abfss://hive@acct.dfs.core.windows.net/upgraded/
iceberg_strategy: ddl_replay
rls_cm_strategy: staging_copy
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.catalog_filter == ["cat_a", "cat_b"]
        assert config.schema_filter == []
        assert config.dry_run is True
        assert config.batch_size == 25
        assert config.migrate_hive_dbfs_root is True
        assert config.hive_dbfs_staging_path.startswith("abfss://")
        assert config.iceberg_strategy == "ddl_replay"
        assert config.rls_cm_strategy == "staging_copy"

    def test_rls_cm_strategy_defaults_to_empty(self, tmp_path):
        """rls_cm_strategy defaults to "" (skip) when unset in config."""
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.rls_cm_strategy == ""

    def test_from_workspace_file_catalog_filter_as_list(self, tmp_path):
        """YAML list syntax for catalog_filter also works."""
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
catalog_filter:
  - cat_a
  - cat_b
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.catalog_filter == ["cat_a", "cat_b"]

    def test_from_workspace_file_raises_on_missing_required(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
# spn_client_id omitted
spn_secret_scope: migration
spn_secret_key: spn-secret
""",
        )
        with pytest.raises(ValueError, match="spn_client_id"):
            MigrationConfig.from_workspace_file(str(path))

    def test_from_workspace_file_raises_on_empty_required(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: ""
spn_secret_scope: migration
spn_secret_key: spn-secret
""",
        )
        with pytest.raises(ValueError, match="spn_client_id"):
            MigrationConfig.from_workspace_file(str(path))

    def test_from_workspace_file_raises_on_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            MigrationConfig.from_workspace_file(str(tmp_path / "does-not-exist.yaml"))

    def test_iceberg_strategy_ddl_replay_is_accepted(self, tmp_path):
        """Setting iceberg_strategy='ddl_replay' opts into Option A path —
        managed_table_worker checks this flag. An unset flag blocks Iceberg.
        """
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
iceberg_strategy: ddl_replay
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.iceberg_strategy == "ddl_replay"

    def test_rls_cm_strategy_staging_copy_is_accepted(self, tmp_path):
        """Path A: rls_cm_strategy='staging_copy' must round-trip so the
        dispatch check in setup_sharing / managed_table_worker can branch
        on it. Strategy validation lives in setup_sharing — config.py is a
        passthrough — but we pin the contract here."""
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
rls_cm_strategy: staging_copy
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.rls_cm_strategy == "staging_copy"

    def test_batch_size_coerced_to_int(self, tmp_path):
        """YAML may deserialise ``batch_size: 100`` as int or str depending
        on style; MigrationConfig must coerce to int so batching math works."""
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
batch_size: "100"
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.batch_size == 100
        assert isinstance(config.batch_size, int)



class TestCollisionPolicyConfig:
    """X.4: on_target_collision field — default, allowed values, error path."""

    def test_default_is_fail(self):
        """Unset on_target_collision defaults to 'fail' (safe)."""
        config = MigrationConfig(
            source_workspace_url="https://src.azuredatabricks.net",
            target_workspace_url="https://tgt.azuredatabricks.net",
            spn_client_id="c",
            spn_secret_scope="s",
            spn_secret_key="k",
        )
        assert config.on_target_collision == "fail"

    def test_from_yaml_skip(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: c
spn_secret_scope: s
spn_secret_key: k
on_target_collision: skip
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.on_target_collision == "skip"

    def test_from_yaml_fail_explicit(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: c
spn_secret_scope: s
spn_secret_key: k
on_target_collision: fail
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.on_target_collision == "fail"

    def test_from_yaml_empty_defaults_to_fail(self, tmp_path):
        """Empty-string (e.g. commented-out field left with value="") is
        treated as unset → fail."""
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: c
spn_secret_scope: s
spn_secret_key: k
on_target_collision: ""
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.on_target_collision == "fail"

    def test_from_yaml_unknown_value_raises(self, tmp_path):
        """Typo / unsupported value surfaces at pre_check-time, not
        silently becomes a different policy."""
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: c
spn_secret_scope: s
spn_secret_key: k
on_target_collision: overwrite
""",
        )
        with pytest.raises(ValueError, match="on_target_collision"):
            MigrationConfig.from_workspace_file(str(path))

    def test_from_yaml_case_insensitive(self, tmp_path):
        """Common YAML quirks: FAIL / Skip normalize to lowercase."""
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: c
spn_secret_scope: s
spn_secret_key: k
on_target_collision: SKIP
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.on_target_collision == "skip"


def test_lakebase_defaults_and_overrides(tmp_path):
    import yaml

    from common.config import MigrationConfig

    base = {
        "source_workspace_url": "https://s", "target_workspace_url": "https://t",
        "spn_client_id": "x", "spn_secret_scope": "sc", "spn_secret_key": "k",
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(base))
    cfg = MigrationConfig.from_workspace_file(str(p))
    assert cfg.lakebase_instance_name == "cp-migration-lakebase"
    assert cfg.lakebase_logical_database == "databricks_postgres"
    assert cfg.lakebase_capacity == "CU_1"

    p.write_text(yaml.safe_dump({**base, "lakebase_instance_name": "lb_x",
                                 "lakebase_logical_database": "ldb", "lakebase_capacity": "CU_2"}))
    cfg2 = MigrationConfig.from_workspace_file(str(p))
    assert cfg2.lakebase_instance_name == "lb_x"
    assert cfg2.lakebase_logical_database == "ldb"
    assert cfg2.lakebase_capacity == "CU_2"


def test_lfc_saas_cursor_columns_default_empty(tmp_path):
    import yaml

    base = {
        "source_workspace_url": "https://s", "target_workspace_url": "https://t",
        "spn_client_id": "x", "spn_secret_scope": "sc", "spn_secret_key": "k",
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(base))
    cfg = MigrationConfig.from_workspace_file(str(p))
    assert cfg.lfc_saas_cursor_columns == {}


def test_lfc_saas_cursor_columns_from_yaml_mapping(tmp_path):
    import yaml

    base = {
        "source_workspace_url": "https://s", "target_workspace_url": "https://t",
        "spn_client_id": "x", "spn_secret_scope": "sc", "spn_secret_key": "k",
        "lfc_saas_cursor_columns": {"bronze.sf.account": "SystemModstamp"},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(base))
    cfg = MigrationConfig.from_workspace_file(str(p))
    assert cfg.lfc_saas_cursor_columns == {"bronze.sf.account": "SystemModstamp"}


def test_lfc_saas_cursor_columns_from_json_string(tmp_path):
    # base_parameters / workspace-file values arrive as STRINGS — accept a JSON
    # string and parse it to a dict.
    import yaml

    base = {
        "source_workspace_url": "https://s", "target_workspace_url": "https://t",
        "spn_client_id": "x", "spn_secret_scope": "sc", "spn_secret_key": "k",
        "lfc_saas_cursor_columns": '{"bronze.sf.account": "SystemModstamp"}',
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(base))
    cfg = MigrationConfig.from_workspace_file(str(p))
    assert cfg.lfc_saas_cursor_columns == {"bronze.sf.account": "SystemModstamp"}


def test_lfc_saas_cursor_columns_empty_string_is_empty_dict():
    from common.config import _coerce_cursor_columns
    assert _coerce_cursor_columns("") == {}
    assert _coerce_cursor_columns(None) == {}
    assert _coerce_cursor_columns({}) == {}


def test_lfc_saas_cursor_columns_fail_loud_on_malformed_input():
    from common.config import _coerce_cursor_columns
    # malformed JSON string → fail loud
    with pytest.raises(ValueError, match="lfc_saas_cursor_columns"):
        _coerce_cursor_columns("{not valid json")
    # valid JSON but not an object (e.g. a list/scalar) → fail loud
    with pytest.raises(ValueError, match="lfc_saas_cursor_columns"):
        _coerce_cursor_columns('["a", "b"]')
    with pytest.raises(ValueError, match="lfc_saas_cursor_columns"):
        _coerce_cursor_columns(42)


class TestHiveStagingPathAlias:
    def test_default_staging_path_empty_and_no_target_catalog(self):
        config = MigrationConfig(
            source_workspace_url="https://src.azuredatabricks.net",
            target_workspace_url="https://tgt.azuredatabricks.net",
            spn_client_id="client-id",
            spn_secret_scope="scope",
            spn_secret_key="key",
        )
        assert config.hive_dbfs_staging_path == ""
        assert not hasattr(config, "hive_target_catalog")

    def test_new_key_parses(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
migrate_hive_dbfs_root: true
hive_dbfs_staging_path: abfss://stage@acct.dfs.core.windows.net/hive/
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.hive_dbfs_staging_path == "abfss://stage@acct.dfs.core.windows.net/hive/"

    def test_deprecated_alias_maps_with_warning(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
hive_dbfs_target_path: abfss://legacy@acct.dfs.core.windows.net/old/
""",
        )
        with pytest.warns(DeprecationWarning, match="hive_dbfs_target_path"):
            config = MigrationConfig.from_workspace_file(str(path))
        assert config.hive_dbfs_staging_path == "abfss://legacy@acct.dfs.core.windows.net/old/"

    def test_new_key_wins_over_deprecated_alias(self, tmp_path):
        path = _write(
            tmp_path,
            """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
hive_dbfs_staging_path: abfss://new@acct.dfs.core.windows.net/n/
hive_dbfs_target_path: abfss://old@acct.dfs.core.windows.net/o/
""",
        )
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.hive_dbfs_staging_path == "abfss://new@acct.dfs.core.windows.net/n/"
