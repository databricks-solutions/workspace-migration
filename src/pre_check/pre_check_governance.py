# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass

# COMMAND ----------
# pre_check for the standalone migrate_governance workflow.
#
# Trust-the-operator contract (per workflow_split_design Q1): we do NOT
# validate that target tables exist. The operator is responsible for
# running migrate_uc + migrate_hive first. This pre-check ONLY validates
# that discovery_inventory has been populated with governance rows —
# i.e. the discovery job ran on this environment at some point.
#
# Failure modes this catches:
#   - Operator forgot to run discovery → discovery_inventory empty for
#     governance types
#   - tracking_catalog/tracking_schema misconfigured → SQL errors
#   - Discovery ran but found no governance objects (no row filters,
#     tags, etc. — likely a real config issue worth surfacing)

import logging

from common.config import MigrationConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pre_check_governance")

GOVERNANCE_OBJECT_TYPES = (
    "tag",
    "comment",
    "row_filter",
    "column_mask",
    "customer_share",
    "policy",
    "monitor",
    "foreign_catalog",
)


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def run(dbutils, spark) -> None:  # noqa: ARG001
    config = MigrationConfig.from_workspace_file()

    types_in = ", ".join(f"'{t}'" for t in GOVERNANCE_OBJECT_TYPES)
    sql = (
        f"SELECT COUNT(*) AS c "
        f"FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE object_type IN ({types_in})"
    )
    rows = spark.sql(sql).collect()
    n = rows[0].c if rows else 0
    if n == 0:
        raise RuntimeError(
            "pre_check_governance: discovery_inventory has zero rows for "
            f"governance object types ({GOVERNANCE_OBJECT_TYPES}). The "
            "discovery job must run before migrate_governance. If you "
            "expect governance objects to exist on the source workspace, "
            "verify discovery completed successfully and the source has "
            "tags / comments / row filters / column masks / customer "
            "shares to migrate."
        )
    logger.info("pre_check_governance: %d governance rows in discovery_inventory.", n)


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
