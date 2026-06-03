# Databricks notebook source

# COMMAND ----------

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass  # not running under a Databricks notebook

# COMMAND ----------

# Seed a collision fixture (X.4).
#
# Creates a catalog on the TARGET metastore that matches a source-side
# catalog name but with a different shape (a schema + table that the
# source does NOT have). The pre_check's collision-detection step should
# flag this as a target_collision; on_target_collision=fail should
# refuse the migrate; on_target_collision=skip should WARN and seed
# skipped_target_exists rows.
#
# Ordinarily integration tests build seed data on the source workspace;
# this one is unusual in that it pokes the TARGET directly, bypassing
# the share, to simulate an operator-mistake scenario.
#
# Usage: invoked by the collision_integration_test workflow with a
# dbutils widget pointing at which catalog name to collide on. The
# teardown counterpart (teardown_collision.py) drops the rogue target
# objects after the assertion runs.

from common.auth import AuthManager
from common.config import MigrationConfig

# COMMAND ----------

dbutils.widgets.text("collision_catalog", "integration_test_src")  # noqa: F821
collision_catalog = dbutils.widgets.get("collision_catalog").strip()  # noqa: F821

config = MigrationConfig.from_workspace_file()
auth = AuthManager(config, dbutils)  # noqa: F821

# COMMAND ----------

# Create the rogue catalog on the TARGET workspace via SDK (we can't use
# spark.sql here because this notebook is pinned to the source cluster).
target = auth.target_client

try:
    target.catalogs.get(name=collision_catalog)
    print(f"[collision-seed] target catalog {collision_catalog!r} already exists; leaving in place.")
except Exception:  # noqa: BLE001
    target.catalogs.create(name=collision_catalog, comment="X.4 collision fixture — not from source")

try:
    target.schemas.get(full_name=f"{collision_catalog}.collision_only_schema")
except Exception:  # noqa: BLE001
    target.schemas.create(
        name="collision_only_schema",
        catalog_name=collision_catalog,
        comment="X.4 collision fixture — schema only exists on target",
    )

print(f"[collision-seed] Seeded rogue target catalog {collision_catalog!r} with collision_only_schema.")
