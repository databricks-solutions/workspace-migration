# Databricks notebook source

# COMMAND ----------

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

# Teardown for the X.4 collision fixture. Drops the rogue target catalog
# created by seed_collision_test_data.py so subsequent runs start clean.

from common.auth import AuthManager
from common.config import MigrationConfig

# COMMAND ----------

dbutils.widgets.text("collision_catalog", "integration_test_src")  # noqa: F821
collision_catalog = dbutils.widgets.get("collision_catalog").strip()  # noqa: F821

config = MigrationConfig.from_workspace_file()
auth = AuthManager(config, dbutils)  # noqa: F821
target = auth.target_client

# COMMAND ----------

# Drop the rogue schema first, then the catalog. CASCADE handles any
# objects the collision path accidentally created on target.
try:
    target.schemas.delete(full_name=f"{collision_catalog}.collision_only_schema")
except Exception as exc:  # noqa: BLE001
    print(f"[teardown-collision] schema delete failed (harmless if absent): {exc}")

try:
    target.catalogs.delete(name=collision_catalog, force=True)
    print(f"[teardown-collision] Dropped target catalog {collision_catalog!r}.")
except Exception as exc:  # noqa: BLE001
    print(f"[teardown-collision] catalog delete failed (harmless if absent): {exc}")
