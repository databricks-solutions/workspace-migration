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
    # tags_worker writes object_name = f"TAGS_{securable_type}_{fqn}[.col]"
    _tag_names = [r["object_name"] for r in _validated_tag_rows]
    _cat_tag_hit = any(n.startswith("TAGS_CATALOG_") and "integration_test_src" in n for n in _tag_names)
    _sch_tag_hit = any(n.startswith("TAGS_SCHEMA_") and "test_schema" in n for n in _tag_names)
    _vol_tag_hit = any(n.startswith("TAGS_VOLUME_") and "test_volume" in n for n in _tag_names)
    if not _cat_tag_hit:
        error_messages.append("3.15 tags: no validated CATALOG tag row (expected TAGS_CATALOG_*integration_test_src*).")
    if not _sch_tag_hit:
        error_messages.append("3.15 tags: no validated SCHEMA tag row (expected TAGS_SCHEMA_*test_schema*).")
    if not _vol_tag_hit:
        error_messages.append("3.15 tags: no validated VOLUME tag row (expected TAGS_VOLUME_*test_volume*).")
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
# --- Phase 3 T29: row filter replayed on target ---
# Moved from test_uc_end_to_end.py per WS-17 cleanup. row_filters_worker
# runs in migrate_governance, and the DDL sanitizer reapply leg is also a
# governance concern (strip happens in migrate_uc views/external workers,
# reapply happens in migrate_governance).

# Task 29 — row filter
has_rf = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_row_filter", debugValue="false"
)
if str(has_rf).lower() == "true":
    rf_rows = full_status.filter("object_type = 'row_filter' AND status = 'validated'").collect()
    if not rf_rows:
        error_messages.append("Phase 3 T29: row filter not replayed on target.")
    else:
        print(f"Phase 3 T29 validated: row filter applied on {rf_rows[0]['object_name']}.")

    # --- DDL sanitizer end-to-end (S.12) ---
    # Not just "migration_status says validated" — fetch the external_customers
    # table from TARGET and verify its row_filter is actually populated.
    # Proves the strip-filter-from-DDL path in external_table_worker + the
    # later row_filters_worker re-application on target both work.
    try:
        from common.auth import AuthManager  # noqa: E402

        _auth = AuthManager(config, dbutils)  # noqa: F821
        _tgt_info = _auth.target_client.tables.get("integration_test_src.test_schema.external_customers")
        if getattr(_tgt_info, "row_filter", None) is None:
            error_messages.append(
                "DDL sanitizer E2E: external_customers on target has no "
                "row_filter — strip-then-reapply chain broke. Sanitizer "
                "stripped the filter from CREATE TABLE but row_filters_worker "
                "didn't reapply it."
            )
        else:
            print(
                f"DDL sanitizer E2E validated: external_customers on target "
                f"carries row_filter "
                f"'{getattr(_tgt_info.row_filter, 'function_name', '?')}'"
            )
    except Exception as _exc:  # noqa: BLE001
        error_messages.append(f"DDL sanitizer E2E: target lookup failed: {_exc}")
else:
    print("Phase 3 T29: row filter fixture not seeded; skipping.")

# COMMAND ----------
# --- Phase 3 T30: column mask replayed on target ---
# Moved from test_uc_end_to_end.py per WS-17 cleanup. column_masks_worker
# runs in migrate_governance, and the DDL sanitizer reapply leg for masks
# is a governance concern just like the row_filter reapply above.

# Task 30 — column mask
has_cm = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_column_mask", debugValue="false"
)
if str(has_cm).lower() == "true":
    cm_rows = full_status.filter("object_type = 'column_mask' AND status = 'validated'").collect()
    if not cm_rows:
        error_messages.append("Phase 3 T30: column mask not replayed on target.")
    else:
        print(f"Phase 3 T30 validated: column mask applied on {cm_rows[0]['object_name']}.")

    # --- DDL sanitizer end-to-end: column mask on target (S.12) ---
    # external_customers.customer_id should carry the mask on target
    # even though external_table_worker stripped the MASK clause from
    # the replayed CREATE TABLE (because mask_customer function hadn't
    # been migrated yet at that stage).
    try:
        from common.auth import AuthManager  # noqa: E402

        _auth = AuthManager(config, dbutils)  # noqa: F821
        _tgt_info = _auth.target_client.tables.get("integration_test_src.test_schema.external_customers")
        _masked_cols = [c for c in (getattr(_tgt_info, "columns", None) or []) if getattr(c, "mask", None) is not None]
        if not _masked_cols:
            error_messages.append(
                "DDL sanitizer E2E: external_customers on target has no "
                "column masks — strip-then-reapply chain broke for masks."
            )
        else:
            print(f"DDL sanitizer E2E validated: {len(_masked_cols)} column mask(s) on external_customers on target.")
    except Exception as _exc:  # noqa: BLE001
        error_messages.append(f"DDL sanitizer E2E (mask): target lookup failed: {_exc}")
else:
    print("Phase 3 T30: column mask fixture not seeded; skipping.")

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
# --- 3.24: Customer-defined Delta share ---
# sharing_worker recreates the share shell + recipient on target. The
# tool's internal ``cp_migration_share`` is excluded at discovery time
# via list_shares(exclude_names=...), so this customer share flows
# through unaffected.

_has_cs = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_customer_share", debugValue="false"
)
if str(_has_cs).lower() == "true":
    _cs_name = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
        taskKey="seed_uc", key="customer_share_name", debugValue=""
    )
    _cr_name = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
        taskKey="seed_uc", key="customer_recipient_name", debugValue=""
    )

    _share_rows = full_status.filter(f"object_type = 'share' AND object_name = 'SHARE_{_cs_name}'").collect()
    _rcpt_rows = full_status.filter(f"object_type = 'recipient' AND object_name = 'RECIPIENT_{_cr_name}'").collect()
    # Hard assertions. PR #31 downgraded these to WARNINGs because the
    # customer share intermittently failed to surface a migration_status
    # row — F.1 traced that to the seeder identity drifting from the
    # migration SPN, which left the SPN without USE SHARE / USE RECIPIENT
    # so discovery's ``list_shares()`` / ``list_recipients()`` silently
    # skipped the objects. The seeder now transfers ownership to the SPN
    # (see ``seed_uc_test_data.py`` 3.24 block), so both rows must land
    # every run. If either row is missing, something is genuinely broken
    # (e.g. ownership ALTER failed, or sharing_worker crashed) — fail
    # loud rather than silently masking regressions.
    if not _share_rows:
        error_messages.append(f"3.24 customer share: no migration_status row for SHARE_{_cs_name}.")
    elif _share_rows[0]["status"] != "validated":
        error_messages.append(
            f"3.24 customer share: SHARE_{_cs_name} status is "
            f"{_share_rows[0]['status']!r}, expected 'validated'. "
            f"error={_share_rows[0]['error_message']!r}"
        )
    if not _rcpt_rows:
        error_messages.append(f"3.24 customer share: no migration_status row for RECIPIENT_{_cr_name}.")
    elif _rcpt_rows[0]["status"] != "validated":
        error_messages.append(
            f"3.24 customer share: RECIPIENT_{_cr_name} status is "
            f"{_rcpt_rows[0]['status']!r}, expected 'validated'. "
            f"error={_rcpt_rows[0]['error_message']!r}"
        )

    if _share_rows and _share_rows[0]["status"] == "validated":
        # Verify via target_client.shares.get
        try:
            from common.auth import AuthManager  # noqa: E402

            _auth_s = AuthManager(config, dbutils)  # noqa: F821
            _tgt_share = _auth_s.target_client.shares.get(name=_cs_name, include_shared_data=True)
            _shared_objects = list(_tgt_share.objects or [])
            if not _shared_objects:
                error_messages.append(f"3.24 customer share: target share '{_cs_name}' has no objects attached.")
            else:
                _obj_names = [o.name for o in _shared_objects]
                if not any("managed_orders" in (n or "") for n in _obj_names):
                    error_messages.append(
                        f"3.24 customer share: target share '{_cs_name}' objects are "
                        f"{_obj_names}, expected to include managed_orders."
                    )
                else:
                    print(
                        f"3.24 customer share validated: target share '{_cs_name}' carries "
                        f"{len(_shared_objects)} object(s) including managed_orders."
                    )
        except Exception as _exc:  # noqa: BLE001
            error_messages.append(f"3.24 customer share: target shares.get failed: {_exc}")
else:
    print("3.24 customer share: not seeded; skipping.")

# COMMAND ----------

if error_messages:
    raise AssertionError(
        f"governance integration test failed with {len(error_messages)} error(s):\n" + "\n".join(error_messages)
    )

print("governance integration test: all assertions passed.")
