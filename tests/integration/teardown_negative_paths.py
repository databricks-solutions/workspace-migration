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
    pass

# COMMAND ----------

# Negative-path integration teardown.
#
# The negative-path workflow's scenarios each rewrite ``config.yaml``
# with intentionally broken values. They DON'T seed fixtures on source or
# target (X.3.1 / X.3.2 fail pre_check before any side effect; X.3.3 is a
# proper no-op). So the teardown's only responsibility is to restore
# ``config.yaml`` from the backup the first ``setup_test_config`` run
# saved at the very start.
#
# Belt-and-braces: also best-effort drop any delta share / recipient that
# a mis-behaving setup_sharing somehow created — shouldn't happen given
# the scenarios fail before side effects, but harmless if nothing matches.

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

# COMMAND ----------

# Best-effort share / recipient cleanup. The injected scenarios should
# all fail BEFORE ``get_or_create_share`` runs, but if that ever regresses
# we don't want a stray ``cp_migration_share`` to poison subsequent runs.
try:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    for share_name in ("cp_migration_share",):
        try:
            w.shares.delete(share_name)
            print(f"Deleted stray share '{share_name}'.")
        except Exception as e:  # noqa: BLE001
            print(f"Share '{share_name}' cleanup skipped: {e}")
    try:
        for recipient in w.recipients.list():
            if recipient.name and "cp_migration_recipient_" in recipient.name:
                try:
                    w.recipients.delete(recipient.name)
                    print(f"Deleted stray recipient '{recipient.name}'.")
                except Exception as e:  # noqa: BLE001
                    print(f"Recipient '{recipient.name}' cleanup skipped: {e}")
    except Exception as e:  # noqa: BLE001
        print(f"Recipient listing skipped: {e}")
except Exception as exc:  # noqa: BLE001
    print(f"SDK cleanup skipped: {exc}")

print("Negative-path teardown complete.")
