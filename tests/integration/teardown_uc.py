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

from databricks.sdk import WorkspaceClient

# Drop UC test catalogs + tracking test schema on SOURCE
spark.sql("DROP CATALOG IF EXISTS integration_test_tgt CASCADE")  # noqa: F821
spark.sql("DROP CATALOG IF EXISTS integration_test_src CASCADE")  # noqa: F821
spark.sql("DROP CATALOG IF EXISTS integration_test_src_b CASCADE")  # noqa: F821
spark.sql("DROP SCHEMA IF EXISTS migration_tracking.cp_migration_test CASCADE")  # noqa: F821
print("Dropped UC test catalogs on source.")

# Clear test-fixture rows from migration_status / discovery_inventory so
# the next integration run starts from a clean slate. Without this,
# ``get_pending_objects`` treats stale ``validated`` rows as final,
# migrate workers get empty input, and Phase 3 workers fail trying to
# apply to target tables that this run never actually created.
# Scoped to integration_test_src / test_schema so we don't touch any
# real customer state that might share the tracking catalog.
try:
    spark.sql(  # noqa: F821
        """
        DELETE FROM migration_tracking.cp_migration.migration_status
        WHERE object_name LIKE '%integration_test_src%'
           OR object_name LIKE '%test_schema%'
           OR object_name LIKE '%extra_schema%'
        """
    )
    spark.sql(  # noqa: F821
        """
        DELETE FROM migration_tracking.cp_migration.discovery_inventory
        WHERE object_name LIKE '%integration_test_src%'
           OR object_name LIKE '%test_schema%'
           OR object_name LIKE '%extra_schema%'
           OR (catalog_name IN ('integration_test_src', 'integration_test_src_b'))
        """
    )
    print("Cleared integration_test fixture rows from tracking tables.")
except Exception as _exc:  # noqa: BLE001
    # First-ever run: tracking tables don't exist yet — safe to skip.
    print(f"Tracking table cleanup skipped: {_exc}")

# Also drop the migrated catalog on TARGET, otherwise the next run's
# migrate fails with TABLE_OR_VIEW_ALREADY_EXISTS. Use the target
# workspace's SQL warehouse via AuthManager.
from common.auth import AuthManager  # noqa: E402
from common.config import MigrationConfig  # noqa: E402
from common.sql_utils import execute_and_poll, find_warehouse  # noqa: E402

config = MigrationConfig.from_workspace_file()
auth = AuthManager(config, dbutils)  # noqa: F821
try:
    wh_id = find_warehouse(auth)
    for _sql in (
        "DROP CATALOG IF EXISTS integration_test_src CASCADE",
        "DROP CATALOG IF EXISTS integration_test_src_b CASCADE",
        "DROP CATALOG IF EXISTS cp_migration_share_consumer CASCADE",
    ):
        res = execute_and_poll(auth, wh_id, _sql)
        if res["state"] == "SUCCEEDED":
            print(f"Target: {_sql}")
        else:
            print(f"Target: {_sql} → {res.get('state')} ({res.get('error', '')})")
except Exception as _exc:  # noqa: BLE001
    print(f"Target cleanup skipped: {_exc}")

# COMMAND ----------

# Clean up Delta Share created during UC migration
w = WorkspaceClient()
# Tool-owned share + customer-defined share from 3.24 fixture.
for share_name in ("cp_migration_share", "integration_test_customer_share"):
    try:
        w.shares.delete(share_name)
        print(f"Deleted share '{share_name}'.")
    except Exception as e:  # noqa: BLE001
        print(f"Share '{share_name}' cleanup skipped: {e}")

try:
    for recipient in w.recipients.list():
        if recipient.name and (
            "cp_migration_recipient_" in recipient.name or recipient.name == "integration_test_recipient"
        ):
            try:
                w.recipients.delete(recipient.name)
                print(f"Deleted recipient '{recipient.name}'.")
            except Exception as e:  # noqa: BLE001
                print(f"Recipient '{recipient.name}' cleanup skipped: {e}")
except Exception as e:  # noqa: BLE001
    print(f"Recipient listing skipped: {e}")

# Drop the customer-defined share + recipient on TARGET too so the next
# run's sharing_worker can recreate them cleanly (create fails with
# "already exists" otherwise, which the worker tolerates but leaves
# stale cross-run state).
try:
    _auth_td = AuthManager(config, dbutils)  # noqa: F821
    for _share in ("integration_test_customer_share",):
        try:
            _auth_td.target_client.shares.delete(_share)
            print(f"Target: deleted share '{_share}'.")
        except Exception as _exc:  # noqa: BLE001
            print(f"Target: share '{_share}' cleanup skipped: {_exc}")
    for _rcpt in ("integration_test_recipient",):
        try:
            _auth_td.target_client.recipients.delete(_rcpt)
            print(f"Target: deleted recipient '{_rcpt}'.")
        except Exception as _exc:  # noqa: BLE001
            print(f"Target: recipient '{_rcpt}' cleanup skipped: {_exc}")
except Exception as _exc:  # noqa: BLE001
    print(f"Target share/recipient cleanup skipped: {_exc}")

# Registered model is nested inside integration_test_src which we
# already drop CASCADE above — source and target model rows disappear
# with the catalog drop. No explicit delete needed.

# COMMAND ----------

# Restore the pre-test config.yaml (setup_test_config saved a backup at
# the start of the workflow). Missing backup is harmless — means
# setup_test_config didn't run, so nothing to restore.

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

print("UC teardown complete.")
