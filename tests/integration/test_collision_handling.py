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

# X.4 collision-handling integration assertions.
#
# Runs AFTER seed_collision_test_data.py has seeded a rogue target
# catalog AND AFTER discovery has populated discovery_inventory with
# the source catalog of the same name.
#
# Scenarios, keyed by the ``scenario`` widget set by the workflow task:
#
#   fail  — on_target_collision=fail. pre_check should have recorded a
#           FAIL row on ``check_target_collisions`` and raised. Assert
#           the pre_check_results table reflects this. Then call
#           check_collision_gate directly and assert it raises
#           RuntimeError (the migrate orchestrator would abort here).
#
#   skip  — on_target_collision=skip. pre_check should have recorded a
#           WARN row and seeded skipped_target_exists rows in
#           migration_status. Assert both. Assert the target objects
#           seeded by the rogue catalog are still present (untouched).

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.orchestrator import check_collision_gate

# COMMAND ----------

dbutils.widgets.text("scenario", "fail")  # noqa: F821
dbutils.widgets.text("collision_catalog", "integration_test_src")  # noqa: F821

scenario = dbutils.widgets.get("scenario").strip()  # noqa: F821
collision_catalog = dbutils.widgets.get("collision_catalog").strip()  # noqa: F821

assert scenario in ("fail", "skip"), f"Unknown scenario {scenario!r}"

config = MigrationConfig.from_workspace_file()
auth = AuthManager(config, dbutils)  # noqa: F821
tracker = TrackingManager(spark, config)  # noqa: F821

errors: list[str] = []

# COMMAND ----------

# 1. Inspect the latest pre_check_results row for check_target_collisions.
latest = spark.sql(  # noqa: F821
    f"""
    SELECT status, message
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY check_name ORDER BY checked_at DESC) AS rn
        FROM {config.tracking_catalog}.{config.tracking_schema}.pre_check_results
    )
    WHERE rn = 1 AND check_name = 'check_target_collisions'
    """
).collect()

if not latest:
    errors.append("No check_target_collisions row in pre_check_results — pre_check didn't run the X.4 check.")
else:
    status = (latest[0].status or "").upper()
    print(f"[collision-assert] check_target_collisions status={status}, message={latest[0].message!r}")
    if scenario == "fail" and status != "FAIL":
        errors.append(f"Expected FAIL under on_target_collision=fail; got {status}.")
    elif scenario == "skip" and status != "WARN":
        errors.append(f"Expected WARN under on_target_collision=skip; got {status}.")

# COMMAND ----------

# 2. Under skip, assert migration_status has skipped_target_exists rows
# for the colliding source FQNs.
if scenario == "skip":
    rows = spark.sql(  # noqa: F821
        f"""
        SELECT object_name, object_type, status
        FROM {config.tracking_catalog}.{config.tracking_schema}.migration_status
        WHERE status = 'skipped_target_exists'
        """
    ).collect()
    print(f"[collision-assert] skipped_target_exists rows: {len(rows)}")
    if not rows:
        errors.append("Expected at least one skipped_target_exists row under skip policy; got 0.")

# COMMAND ----------

# 3. Assert orchestrator's collision gate behaves correctly for the
# given scenario.
try:
    check_collision_gate(spark, config)  # noqa: F821
    gate_raised = False
except RuntimeError as exc:
    gate_raised = True
    gate_exc = exc

if scenario == "fail":
    if not gate_raised:
        errors.append("Expected check_collision_gate to raise under fail policy; it didn't.")
    else:
        print(f"[collision-assert] check_collision_gate raised as expected: {gate_exc}")
elif scenario == "skip" and gate_raised:
    errors.append(f"check_collision_gate raised unexpectedly under skip policy: {gate_exc}")

# COMMAND ----------

# 4. The rogue target catalog MUST still exist (we don't touch it in
# either fail or skip policy). Verify via SDK.
try:
    auth.target_client.catalogs.get(name=collision_catalog)
    print(f"[collision-assert] Target catalog {collision_catalog!r} still present, as expected.")
except Exception as exc:  # noqa: BLE001
    errors.append(
        f"Target catalog {collision_catalog!r} was modified or deleted — collision handling "
        f"must not touch pre-existing target objects. Error: {exc}"
    )

# COMMAND ----------

if errors:
    msg = "\n".join(f"  - {e}" for e in errors)
    raise AssertionError(f"X.4 collision integration assertions failed:\n{msg}")

print("[collision-assert] All X.4 collision integration assertions passed.")
