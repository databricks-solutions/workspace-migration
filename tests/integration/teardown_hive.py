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

# Hive teardown: drop the source Hive database and the UC-target upgrade catalog.

spark.sql("DROP DATABASE IF EXISTS hive_metastore.integration_test_hive CASCADE")  # noqa: F821

from common.auth import AuthManager  # noqa: E402
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse  # noqa: E402

config = MigrationConfig.from_workspace_file()
spark.sql(f"DROP CATALOG IF EXISTS `{config.hive_target_catalog}` CASCADE")  # noqa: F821

# Phase 2 integration 2.11 seeded a parallel UC catalog referenced by
# mixed_ref_view. Drop on source; target drop happens in the target-side
# cleanup block below.
_HIVE_UCREF_CATALOG = "integration_test_hive_ucref"
try:
    spark.sql(f"DROP CATALOG IF EXISTS `{_HIVE_UCREF_CATALOG}` CASCADE")  # noqa: F821
    print(f"Dropped source UC ref catalog `{_HIVE_UCREF_CATALOG}`.")
except Exception as _exc:  # noqa: BLE001
    print(f"Source drop `{_HIVE_UCREF_CATALOG}` skipped: {_exc}")

# Clear Hive test-fixture rows from tracking tables — same reasoning as
# teardown_uc: stale ``validated`` rows make get_pending_objects return
# empty and Phase 2 workers produce no work on the next run.
try:
    spark.sql(  # noqa: F821
        """
        DELETE FROM migration_tracking.cp_migration.migration_status
        WHERE object_name LIKE '%integration_test_hive%'
           OR object_name LIKE '%hive_metastore.integration_test_hive%'
        """
    )
    spark.sql(  # noqa: F821
        """
        DELETE FROM migration_tracking.cp_migration.discovery_inventory
        WHERE source_type = 'hive'
           OR object_name LIKE '%integration_test_hive%'
        """
    )
    print("Cleared integration_test_hive fixture rows from tracking tables.")
except Exception as _exc:  # noqa: BLE001
    print(f"Tracking table cleanup skipped: {_exc}")

# Also drop hive_target_catalog on TARGET — hive migration creates it
# there too and a stale copy breaks the next run. Also drops the UC-ref
# catalog seeded for integration item 2.11.
try:
    auth = AuthManager(config, dbutils)  # noqa: F821
    wh_id = find_warehouse(auth)
    res = execute_and_poll(auth, wh_id, f"DROP CATALOG IF EXISTS `{config.hive_target_catalog}` CASCADE")
    print(f"Target drop `{config.hive_target_catalog}`: {res.get('state')}")
    res_ucref = execute_and_poll(auth, wh_id, f"DROP CATALOG IF EXISTS `{_HIVE_UCREF_CATALOG}` CASCADE")
    print(f"Target drop `{_HIVE_UCREF_CATALOG}`: {res_ucref.get('state')}")
except Exception as _exc:  # noqa: BLE001
    print(f"Target hive catalog cleanup skipped: {_exc}")

# COMMAND ----------

# Restore the pre-test config.yaml (setup_test_config saved a backup at
# the start of the workflow).

import os  # noqa: E402
import shutil  # noqa: E402

from common.config import _resolve_bundle_config_path  # type: ignore[import-not-found]  # noqa: E402

config_path = _resolve_bundle_config_path()
backup_path = config_path + ".pre-integration-test.bak"
if os.path.exists(backup_path):
    shutil.move(backup_path, config_path)
    print(f"Restored {config_path} from {backup_path}.")
else:
    print("No pre-integration-test backup found; config.yaml left as-is.")

print("Hive teardown complete.")
