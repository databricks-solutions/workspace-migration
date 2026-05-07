"""Tests for src/pre_check/pre_check_governance.py."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from pre_check.pre_check_governance import run


@contextmanager
def _patch_config(config):
    with patch("common.config.MigrationConfig.from_workspace_file", return_value=config):
        yield


def test_pre_check_governance_passes_when_discovery_inventory_populated():
    """When discovery_inventory has rows for governance object types, pass."""
    spark = MagicMock()
    config = MagicMock()
    config.tracking_catalog = "main"
    config.tracking_schema = "cp_migration"
    spark.sql.return_value.collect.return_value = [MagicMock(c=42)]

    with _patch_config(config):
        run(MagicMock(), spark)
    # No exception raised → pass


def test_pre_check_governance_fails_when_discovery_inventory_empty():
    """When discovery_inventory has no rows for governance types, raise loud."""
    spark = MagicMock()
    config = MagicMock()
    config.tracking_catalog = "main"
    config.tracking_schema = "cp_migration"
    spark.sql.return_value.collect.return_value = [MagicMock(c=0)]

    with _patch_config(config), pytest.raises(RuntimeError) as excinfo:
        run(MagicMock(), spark)
    assert "discovery" in str(excinfo.value).lower()


def test_pre_check_governance_queries_governance_object_types():
    """The COUNT query must filter to governance object_types."""
    spark = MagicMock()
    config = MagicMock()
    config.tracking_catalog = "main"
    config.tracking_schema = "cp_migration"
    spark.sql.return_value.collect.return_value = [MagicMock(c=10)]

    with _patch_config(config):
        run(MagicMock(), spark)

    sql = spark.sql.call_args.args[0]
    for ot in ("tag", "row_filter", "column_mask", "customer_share"):
        assert f"'{ot}'" in sql, f"missing object_type {ot} in COUNT filter SQL"
