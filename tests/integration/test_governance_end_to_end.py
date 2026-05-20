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

# End-to-end integration assertions for the migrate_governance workflow.
#
# Split out of test_uc_end_to_end.py per WS-17 (workflow split). Phase 3
# items 3.15 (catalog/schema/volume tags), 3.17 (column + volume comments),
# and 3.24 (customer-defined Delta share) live here. UC items (P.1-P.4
# staging-copy, 2.5.x Iceberg, 3.19 registered model) stay in
# test_uc_end_to_end.py. 3.21 (connection) and 3.22 (foreign catalog)
# don't have test_uc assertion rows yet (backlog item 13); when they
# land, they belong here too.

# COMMAND ----------
# NOT tested in this standalone governance test:
# - Phase 3 T29 (row filter on target external_customers)
# - Phase 3 T30 (column mask on target external_customers)
# - DDL sanitizer E2E (row_filter + column_mask reapply on external_customers)
# - 3.24 customer share (pre-existing F.1 intermittent flagged 2026-04-23)
#
# T29 / T30 / DDL-sanitizer test the strip-then-reapply chain that
# requires migrate_uc to have run first (so target has the cloned
# managed/external tables with stripped RLS/CM ready for the governance
# reapply leg). The standalone governance test pre-seeds target tables
# as empty shapes (D3) but cannot replicate UC's clone-then-strip state.
#
# These assertions belong in an end-to-end test that runs migrate_uc
# AND migrate_governance back-to-back. Follow-up: extend
# uc_integration_test_workflow.yml to also invoke migrate_governance,
# and add the end-to-end assertions there.

from common.config import MigrationConfig
from common.tracking import TrackingManager

config = MigrationConfig.from_workspace_file()
tracker = TrackingManager(spark, config)  # noqa: F821

error_messages: list[str] = []

# Pull the latest migration_status snapshot once, the same way the UC
# governance assertions inside test_uc_end_to_end did. The governance
# blocks below filter this DataFrame for tag / comment / share / recipient
# rows.
full_status = tracker.get_latest_migration_status()

# COMMAND ----------
# --- 3.15: Catalog / schema / volume tags ---
# Tags applied to non-table securables (CATALOG, SCHEMA, VOLUME) must
# discover and replay on target through tags_worker's generic branch.
# We check that each securable_type carries at least one validated tag row.
_has_non_table_tags = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_non_table_tags", debugValue="false"
)
if str(_has_non_table_tags).lower() == "true":
    _validated_tag_rows = full_status.filter("object_type = 'tag' AND status = 'validated'").collect()
    # C6: object_name aligns with discovery's per-tag key:
    #   f"{securable_fqn}:{column_name}:{tag_name}".rstrip(":")
    # CATALOG/SCHEMA/VOLUME tags have empty column, so their keys look
    # like ``<fqn>::<tag_name>`` (double colon, table/securable type
    # encoded in the FQN itself).
    _tag_names = [r["object_name"] for r in _validated_tag_rows]
    _cat_tag_hit = any("integration_test_src" in n and "::" in n for n in _tag_names if n.count(".") <= 1)
    _sch_tag_hit = any("test_schema" in n and "::" in n for n in _tag_names)
    _vol_tag_hit = any("test_volume" in n and "::" in n for n in _tag_names)
    if not _cat_tag_hit:
        error_messages.append("3.15 tags: no validated CATALOG tag row (expected `integration_test_src`::<tag>).")
    if not _sch_tag_hit:
        error_messages.append("3.15 tags: no validated SCHEMA tag row (expected ...test_schema::<tag>).")
    if not _vol_tag_hit:
        error_messages.append("3.15 tags: no validated VOLUME tag row (expected ...test_volume::<tag>).")
    if _cat_tag_hit and _sch_tag_hit and _vol_tag_hit:
        print("3.15 tags validated: CATALOG / SCHEMA / VOLUME all replayed on target.")
else:
    print("3.15 tags: non-table tags not seeded; skipping.")

# COMMAND ----------
# --- 3.17: Column comment + volume comment ---
# Column comments require ``ALTER TABLE ... ALTER COLUMN ... COMMENT`` on
# target (COMMENT ON COLUMN is not valid Databricks SQL). Volume comments
# go through the generic ``COMMENT ON VOLUME`` path. Both come from the
# extended comments_worker.run() iteration added alongside this fixture.

_has_col_cmt = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_column_comment", debugValue="false"
)
if str(_has_col_cmt).lower() == "true":
    _col_cmt_rows = full_status.filter(
        "object_type = 'comment' AND status = 'validated' AND object_name LIKE 'COMMENT_COLUMN_%'"
    ).collect()
    if not _col_cmt_rows:
        error_messages.append(
            "3.17 column comment: no validated COMMENT_COLUMN_* row — "
            "comments_worker did not replay the column comment via ALTER TABLE ... ALTER COLUMN."
        )
    else:
        _amount_hit = any("managed_orders" in r["object_name"] and ".amount" in r["object_name"] for r in _col_cmt_rows)
        if not _amount_hit:
            error_messages.append(
                f"3.17 column comment: COMMENT_COLUMN row(s) present but none for "
                f"managed_orders.amount. Got: {[r['object_name'] for r in _col_cmt_rows]}"
            )
        else:
            # Verify on target: DESCRIBE TABLE should show the comment on the column.
            try:
                _desc = spark.sql(  # noqa: F821
                    "DESCRIBE TABLE integration_test_src.test_schema.managed_orders"
                ).collect()
                _amount_row = next((r for r in _desc if r.col_name == "amount"), None)
                _amount_comment = getattr(_amount_row, "comment", None) if _amount_row else None
                if not _amount_comment or "GBP" not in _amount_comment:
                    error_messages.append(
                        f"3.17 column comment: target managed_orders.amount comment is "
                        f"{_amount_comment!r}, expected to contain 'GBP'."
                    )
                else:
                    print(f"3.17 column comment validated: target carries comment {_amount_comment!r}.")
            except Exception as _exc:  # noqa: BLE001
                error_messages.append(f"3.17 column comment: target DESCRIBE failed: {_exc}")
else:
    print("3.17 column comment: not seeded; skipping.")

_has_vol_cmt = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_volume_comment", debugValue="false"
)
if str(_has_vol_cmt).lower() == "true":
    _vol_cmt_rows = full_status.filter(
        "object_type = 'comment' AND status = 'validated' AND object_name LIKE 'COMMENT_VOLUME_%'"
    ).collect()
    _vol_hit = any("test_volume" in r["object_name"] for r in _vol_cmt_rows)
    if not _vol_hit:
        error_messages.append(
            "3.17 volume comment: no validated COMMENT_VOLUME_*test_volume* row — "
            "comments_worker did not replay the volume comment."
        )
    else:
        print(f"3.17 volume comment validated: {len(_vol_cmt_rows)} volume comment row(s) replayed.")
else:
    print("3.17 volume comment: not seeded; skipping.")

# COMMAND ----------
# --- Phase 3 T28: tag rows in migration_status ---
# Moved from test_uc_end_to_end.py per WS-17 cleanup. tags_worker runs in
# migrate_governance, so this validation belongs here.

# Task 28 — tags: expect at least one 'tag' row with status='validated'
has_tag = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_tag", debugValue="false"
)
if str(has_tag).lower() == "true":
    tag_rows = full_status.filter("object_type = 'tag' AND status = 'validated'").collect()
    if not tag_rows:
        error_messages.append("Phase 3 T28: no validated tag rows in migration_status.")
    else:
        print(f"Phase 3 T28 validated: {len(tag_rows)} tag row(s) on target.")
else:
    print("Phase 3 T28: tag fixture not seeded; skipping.")

# COMMAND ----------
# --- Phase 3 T32: comments replayed on target ---
# Moved from test_uc_end_to_end.py per WS-17 cleanup. comments_worker runs
# in migrate_governance.

# Task 32 — comments: expect at least CATALOG + SCHEMA + TABLE comment rows
comment_rows = full_status.filter("object_type = 'comment' AND status = 'validated'").collect()
if len(comment_rows) < 2:
    # 2 minimum since TABLE comments on Delta may skip via DEEP CLONE path
    error_messages.append(f"Phase 3 T32: expected >= 2 comment rows (catalog + schema), got {len(comment_rows)}.")
else:
    print(f"Phase 3 T32 validated: {len(comment_rows)} comment row(s) replayed.")

# COMMAND ----------

if error_messages:
    raise AssertionError(
        f"governance integration test failed with {len(error_messages)} error(s):\n" + "\n".join(error_messages)
    )

print("governance integration test: all assertions passed.")
