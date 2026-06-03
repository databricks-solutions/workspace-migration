from unittest.mock import MagicMock

import pytest

from common.config import MigrationConfig


@pytest.fixture
def mock_config() -> MigrationConfig:
    """Default MigrationConfig used across unit tests."""
    return MigrationConfig(
        source_workspace_url="https://source.azuredatabricks.net",
        target_workspace_url="https://target.azuredatabricks.net",
        spn_client_id="test-client-id",
        spn_secret_scope="migration-scope",
        spn_secret_key="spn-secret",
        catalog_filter=["catalog_a", "catalog_b"],
        tracking_catalog="migration_tracking",
        tracking_schema="cp_migration",
    )


@pytest.fixture
def mock_dbutils():
    """Minimal dbutils mock. Config is now loaded from a workspace YAML file, so
    most tests mock MigrationConfig.from_workspace_file directly instead of
    relying on widget seeding here. Left for tests that only need secrets.get."""
    dbutils = MagicMock()
    dbutils.secrets.get.return_value = "fake-secret"
    return dbutils


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark


@pytest.fixture
def mock_workspace_client():
    client = MagicMock()
    client.current_user.me.return_value = MagicMock(user_name="test@databricks.com")
    return client
