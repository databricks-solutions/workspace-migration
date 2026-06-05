# Databricks notebook source

# COMMAND ----------

import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _files_root = "/Workspace" + _nb.split("/files/")[0] + "/files"
    # /files/src for ``common.*`` resolution; /files for ``tests.integration.*``
    # shared helpers (matches setup_test_config bootstrap).
    for _p in (f"{_files_root}/src", _files_root):
        if _p not in sys.path:
            sys.path.insert(0, _p)
except NameError:
    pass

# COMMAND ----------

# Hive end-to-end assertion: verify all seeded Hive objects migrated to UC
# target under {hive_target_catalog}.integration_test_hive.

from common.config import MigrationConfig
from common.tracking import TrackingManager
from tests.integration._assertion_helpers import expect_validated  # type: ignore[import-not-found]

config = MigrationConfig.from_workspace_file()
tracker = TrackingManager(spark, config)  # noqa: F821

status_df = tracker.get_latest_migration_status()
hive_types = (
    "hive_view",
    "hive_function",
    "hive_managed_dbfs_root",
    "hive_managed_nondbfs",
    "hive_external",
    "hive_grant",
)
status_df = status_df.filter(status_df.object_type.isin(list(hive_types)))

total = status_df.count()
print(f"Total Hive migrated objects: {total}")
assert total > 0, "No Hive migration status records found."

error_messages: list[str] = []


def _expect_validated(row, label: str) -> bool:  # type: ignore[no-untyped-def]
    """Thin wrapper binding the shared helper to this notebook's
    ``error_messages`` accumulator. See
    ``tests/integration/_assertion_helpers.py``. Closes review H11."""
    return expect_validated(row, label, error_messages)


counts = {
    row["status"]: row["n"] for row in status_df.groupBy("status").count().withColumnRenamed("count", "n").collect()
}
print(f"Hive status breakdown: {counts}")

# Expected object types from seed_hive_test_data
expected_types = ["hive_view", "hive_function"]
if config.migrate_hive_dbfs_root:
    expected_types.append("hive_managed_dbfs_root")

for htype in expected_types:
    rows = status_df.filter(f"object_type = '{htype}'").collect()
    if not rows:
        error_messages.append(f"No {htype} records in migration_status.")
        continue
    accepted = ("validated",)
    if htype == "hive_managed_dbfs_root" and not config.migrate_hive_dbfs_root:
        accepted = ("validated", "skipped_by_config")
    failed = [r for r in rows if r["status"] not in accepted]
    if failed:
        for row in failed:
            error_messages.append(f"Hive {htype}: {row['object_name']} [{row['status']}]: {row['error_message']}")
    else:
        print(f"Hive {htype}: {len(rows)} object(s) OK")

# Data-level row-count check: rely on migration_status (worker records source
# row count + target row count after DEEP CLONE / data copy) — we can't query
# the target catalog directly from this source-side notebook since the target
# metastore isn't visible here.
if config.migrate_hive_dbfs_root:
    dbfs_rows = status_df.filter("object_type = 'hive_managed_dbfs_root' AND status = 'validated'").collect()
    if not dbfs_rows:
        error_messages.append(
            "No validated hive_managed_dbfs_root records (DBFS-root migration did not run or did not validate)."
        )
    else:
        for row in dbfs_rows:
            if row["source_row_count"] != row["target_row_count"]:
                error_messages.append(
                    f"DBFS-root row mismatch for {row['object_name']}: "
                    f"src={row['source_row_count']} tgt={row['target_row_count']}"
                )
            else:
                print(f"{row['object_name']}: src/tgt rows match ({row['source_row_count']})")

# COMMAND ----------
# --- Hive external + managed non-DBFS assertions ---
# Both workers run on every Hive migration but had zero input before the
# corresponding seed was added. Now we assert each seeded category is
# validated and its row count matches across source and target.

has_hive_external = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_hive_external", debugValue="false"
)
if str(has_hive_external).lower() == "true":
    ext_rows = status_df.filter("object_type = 'hive_external' AND object_name LIKE '%external_invoices%'").collect()
    if not ext_rows:
        error_messages.append("hive_external: no migration_status row for external_invoices.")
    else:
        row = ext_rows[0]
        if not _expect_validated(row, "hive_external external_invoices"):
            pass  # error already appended
        elif row["source_row_count"] != row["target_row_count"]:
            error_messages.append(
                f"hive_external: external_invoices row mismatch "
                f"(src={row['source_row_count']}, tgt={row['target_row_count']})"
            )
        else:
            print(f"hive_external validated: external_invoices rows match ({row['source_row_count']})")
else:
    print(
        "hive_external: fixture not seeded (requires migrate_hive_dbfs_root + "
        "hive_dbfs_target_path); skipping assertion."
    )

has_hive_nondbfs = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_hive_nondbfs", debugValue="false"
)
if str(has_hive_nondbfs).lower() == "true":
    nd_rows = status_df.filter("object_type = 'hive_managed_nondbfs' AND object_name LIKE '%nondbfs_sales%'").collect()
    if not nd_rows:
        error_messages.append("hive_managed_nondbfs: no migration_status row for nondbfs_sales.")
    else:
        row = nd_rows[0]
        if not _expect_validated(row, "hive_managed_nondbfs nondbfs_sales"):
            pass  # error already appended
        elif row["source_row_count"] != row["target_row_count"]:
            error_messages.append(
                f"hive_managed_nondbfs: nondbfs_sales row mismatch "
                f"(src={row['source_row_count']}, tgt={row['target_row_count']})"
            )
        else:
            print(f"hive_managed_nondbfs validated: nondbfs_sales rows match ({row['source_row_count']})")
else:
    print(
        "hive_managed_nondbfs: fixture not seeded (requires migrate_hive_dbfs_root "
        "+ hive_dbfs_target_path); skipping assertion."
    )

# COMMAND ----------
# --- Hive grants assertion ---
# Seed grants SELECT on managed_orders to ``account users``. Verify
# hive_grants_worker migrated that grant.

has_hive_grant = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_hive_grant", debugValue="false"
)
if str(has_hive_grant).lower() == "true":
    full_status_hive = tracker.get_latest_migration_status()
    hg_rows = full_status_hive.filter(
        "object_type = 'hive_grant' AND status = 'validated' AND object_name LIKE '%account users%'"
    ).collect()
    if not hg_rows:
        error_messages.append(
            "hive_grants: no validated hive_grant row for `account users` — "
            "SELECT on Hive schema did not migrate to target."
        )
    else:
        print(f"hive_grants validated: {len(hg_rows)} hive_grant row(s) for 'account users' replayed.")
else:
    print("hive_grants: grant fixture not seeded; skipping assertion.")

# COMMAND ----------
# --- Phase 2 integration extras (items 2.10 – 2.15) ---
# Each block gates on the seed's task value so partial fixtures don't
# mask unrelated regressions.


def _validated_hive_rows(object_type: str, name_like: str) -> list:
    return status_df.filter(
        f"object_type = '{object_type}' AND object_name LIKE '%{name_like}%'"
    ).collect()


# --- 2.10 multi-upstream view ---
_has_multi_upstream = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_multi_upstream_view", debugValue="false"
)
if str(_has_multi_upstream).lower() == "true":
    rows_mu = _validated_hive_rows("hive_view", "product_orders")
    if not rows_mu:
        error_messages.append("2.10: product_orders view row missing from migration_status.")
    elif _expect_validated(rows_mu[0], "2.10 product_orders view"):
        print("2.10 OK: multi-upstream view product_orders validated.")
    rows_mp = _validated_hive_rows("hive_managed_dbfs_root", "managed_products")
    if not rows_mp:
        error_messages.append("2.10: managed_products upstream table missing from migration_status.")
    else:
        _expect_validated(rows_mp[0], "2.10 managed_products upstream")
else:
    print("2.10 skipped: seed did not create multi-upstream view.")

# 2.11 (cross-catalog view referencing a UC catalog) was removed: it was the
# only fixture that created a UC catalog on the Hive side, which forced the
# fragile catalog_filter="<nonexistent>" hack to keep UC discovery a no-op.
# That hack now hard-errors under the H8 "raise on empty filter" fix, so the
# fixture + the filter were dropped together (niche scenario; unblocks the rest
# of the hive suite). See the workflow YAML (no catalog_filter) for context.

# --- 2.12 partitioned DBFS-root ---
_has_part_dbfs = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_partitioned_dbfs", debugValue="false"
)
if str(_has_part_dbfs).lower() == "true" and config.migrate_hive_dbfs_root:
    rows_pd = _validated_hive_rows("hive_managed_dbfs_root", "partitioned_orders")
    if not rows_pd:
        error_messages.append("2.12: partitioned_orders row missing from migration_status.")
    else:
        row = rows_pd[0]
        if not _expect_validated(row, "2.12 partitioned_orders"):
            pass  # error already appended
        elif row["source_row_count"] != row["target_row_count"]:
            error_messages.append(
                f"2.12: partitioned_orders row mismatch "
                f"(src={row['source_row_count']}, tgt={row['target_row_count']})"
            )
        else:
            print(f"2.12 OK: partitioned_orders rows match ({row['source_row_count']}).")
elif str(_has_part_dbfs).lower() == "true":
    print("2.12 skipped: migrate_hive_dbfs_root=false.")
else:
    print("2.12 skipped: seed did not create partitioned DBFS-root table.")

# --- 2.13 partitioned external ---
_has_part_ext = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_partitioned_external", debugValue="false"
)
if str(_has_part_ext).lower() == "true":
    rows_pe = _validated_hive_rows("hive_external", "external_partitioned_sales")
    if not rows_pe:
        error_messages.append("2.13: external_partitioned_sales row missing from migration_status.")
    else:
        row = rows_pe[0]
        if not _expect_validated(row, "2.13 external_partitioned_sales"):
            pass  # error already appended
        elif row["source_row_count"] != row["target_row_count"]:
            error_messages.append(
                f"2.13: external_partitioned_sales row mismatch "
                f"(src={row['source_row_count']}, tgt={row['target_row_count']})"
            )
        else:
            print(f"2.13 OK: external_partitioned_sales rows match ({row['source_row_count']}).")
else:
    print("2.13 skipped: seed did not create partitioned external table.")

# --- 2.14 orchestrator batch sizing ---
_batch_tables_seeded = int(  # noqa: B008
    dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
        taskKey="seed_hive", key="batch_tables_seeded", debugValue="0"
    )
    or "0"
)
if _batch_tables_seeded > 0 and config.migrate_hive_dbfs_root:
    batch_rows = [
        r
        for r in status_df.filter(
            "object_type = 'hive_managed_dbfs_root' AND object_name LIKE '%batch_tbl_%'"
        ).collect()
    ]
    # H11: "validated" means status=validated AND empty error_message.
    # A worker recording ``validated`` with a warning in error_message
    # would otherwise count here as a pass but is really a partial
    # failure that needs operator attention.
    clean_validated = [r for r in batch_rows if r["status"] == "validated" and not r["error_message"]]
    if len(clean_validated) != _batch_tables_seeded:
        missing = _batch_tables_seeded - len(clean_validated)
        failures = [
            f"{r['object_name']}[{r['status']}]: {r['error_message']}"
            for r in batch_rows
            if not (r["status"] == "validated" and not r["error_message"])
        ]
        error_messages.append(
            f"2.14: expected {_batch_tables_seeded} batch_tbl_* clean-validated; "
            f"got {len(clean_validated)} (missing or warning-tainted: {missing}). "
            f"Non-clean rows: {failures}"
        )
    else:
        print(f"2.14 OK: all {_batch_tables_seeded} batch_tbl_* tables validated.")
elif _batch_tables_seeded > 0:
    print("2.14 skipped: migrate_hive_dbfs_root=false.")
else:
    print("2.14 skipped: seed created no batch tables.")

# --- 2.15 view dependency chain ---
_has_chain = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_view_chain", debugValue="false"
)
if str(_has_chain).lower() == "true":
    chain_names = ("view_level1", "view_level2", "view_level3")
    missing = []
    chain_ok = True
    for nm in chain_names:
        rows_cn = _validated_hive_rows("hive_view", nm)
        if not rows_cn:
            missing.append(nm)
            chain_ok = False
        elif not _expect_validated(rows_cn[0], f"2.15 view chain {nm}"):
            # _expect_validated has already appended to error_messages with
            # the chain-step label so a future failure points at the
            # specific view that didn't migrate.
            chain_ok = False
    if missing:
        error_messages.append(f"2.15: missing view chain rows: {missing}")
    if not missing and chain_ok:
        print("2.15 OK: view_level1 → view_level2 → view_level3 all validated.")
else:
    print("2.15 skipped: seed did not create view chain.")

# COMMAND ----------
# --- Coverage guard: every hive-owned in-scope type must be EXERCISED ---
# Red-if-untested (same mechanism as the UC + governance suites). The hive
# suite (migrate_hive) owns these object types. A type is "exercised" when
# migration_status has a row in its expected terminal state (validated).
# Anything in scope that isn't exercised — and isn't explicitly
# exempted-with-reason — fails the run, naming the type, so a silently
# untested type shows RED instead of a false green.
#
# Exemptions are surfaced loudly (an EXEMPT line, never a silent green), and
# only apply to types whose *source fixture* genuinely can't be created in
# this environment — NOT to a seeded type whose migration failed (that stays
# hard RED). Two environmental gates:
#  1. The whole DBFS-root group only runs when migrate_hive_dbfs_root is on.
#  2. hive_external / hive_managed_nondbfs are seeded by writing hive_metastore
#     tables to a RAW abfss LOCATION, which needs a direct storage account key.
#     The serverless test compute reaches ADLS only via UC external locations,
#     so it can't create those source tables (managed_dbfs_root has no explicit
#     LOCATION, so it seeds fine). The seed reports has_hive_external /
#     has_hive_nondbfs=false when it couldn't; we exempt-with-reason on that.
_HIVE_EXPECTED = {
    "hive_view": {"validated"},
    "hive_function": {"validated"},
    "hive_grant": {"validated"},
    "hive_managed_dbfs_root": {"validated"},
    "hive_managed_nondbfs": {"validated"},
    "hive_external": {"validated"},
}
_HIVE_COVERAGE_EXEMPT: dict[str, str] = {}
if not config.migrate_hive_dbfs_root:
    _dbfs_reason = "requires migrate_hive_dbfs_root=true (DBFS-root migration disabled this run)"
    for _t in ("hive_managed_dbfs_root", "hive_managed_nondbfs", "hive_external"):
        _HIVE_COVERAGE_EXEMPT[_t] = _dbfs_reason
else:
    # Gate the raw-abfss-LOCATION fixtures on whether the seed could create
    # them (serverless seed compute lacks a direct storage account key).
    _seed_storage_reason = (
        "source fixture not seeded — serverless seed compute can't write a "
        "hive_metastore table to a raw abfss LOCATION (no direct storage "
        "account key; needs a classic cluster with storage creds). Migration "
        "path itself is unaffected."
    )
    _has_ext = str(
        dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
            taskKey="seed_hive", key="has_hive_external", debugValue="false"
        )
    ).lower()
    _has_nd = str(
        dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
            taskKey="seed_hive", key="has_hive_nondbfs", debugValue="false"
        )
    ).lower()
    if _has_ext != "true":
        _HIVE_COVERAGE_EXEMPT["hive_external"] = _seed_storage_reason
    if _has_nd != "true":
        _HIVE_COVERAGE_EXEMPT["hive_managed_nondbfs"] = _seed_storage_reason

_hive_cov = tracker.get_latest_migration_status()
_hive_by_type: dict[str, set] = {}
for _r in _hive_cov.select("object_type", "status").distinct().collect():
    _hive_by_type.setdefault(_r["object_type"], set()).add(_r["status"])

for _t, _ok in _HIVE_EXPECTED.items():
    _got = _hive_by_type.get(_t, set())
    if _got & _ok:
        print(f"COVERAGE OK: '{_t}' exercised ({sorted(_got)})")
    elif _t in _HIVE_COVERAGE_EXEMPT:
        print(f"COVERAGE EXEMPT '{_t}': {_HIVE_COVERAGE_EXEMPT[_t]}")
    elif _got:
        # Attempted but never reached a validated state — surface the failure
        # messages so the red names WHY, not just THAT it failed.
        _fails = (
            _hive_cov.filter(f"object_type = '{_t}' AND status != 'validated'")
            .select("object_name", "status", "error_message")
            .limit(5)
            .collect()
        )
        _detail = "; ".join(f"{_r['object_name']}: {_r['error_message']}" for _r in _fails)
        error_messages.append(
            f"COVERAGE: in-scope hive type '{_t}' was NOT validated — found {sorted(_got)}. "
            f"Failures: {_detail}"
        )
    else:
        error_messages.append(
            f"COVERAGE: in-scope hive type '{_t}' was NOT exercised — expected {sorted(_ok)}, "
            f"found NONE. Type is untested."
        )

# COMMAND ----------

if error_messages:
    raise AssertionError(
        f"Hive integration test failed with {len(error_messages)} error(s):\n" + "\n".join(error_messages)
    )
print("Hive integration tests passed.")
