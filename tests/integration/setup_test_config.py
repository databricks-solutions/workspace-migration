# Databricks notebook source

# COMMAND ----------

# Setup test config.
#
# Overrides the workspace copy of config.yaml with values appropriate for
# the invoking integration workflow (UC vs Hive). Back up the file first
# so ``teardown`` can restore it — we don't want a failed integration
# run to leave config.yaml in a "test toggles on" state that an operator
# would then trip over during a real migration.
#
# Each integration workflow passes its desired toggles as task
# parameters — that way the repo ships ``config.yaml`` with neutral
# placeholder defaults (placeholders for URLs + SPN + paths), and the
# workflows themselves carry the test-specific behavior flags.
#
# Parameters consumed (all strings; "true"/"false" for booleans):
#   iceberg_strategy         — "" or "ddl_replay"
#   rls_cm_strategy          — "" (skip) or "staging_copy".
#   migrate_hive_dbfs_root   — "true" / "false"
#   hive_dbfs_target_path    — ADLS URL for Hive DBFS-root migration; may
#                              be empty when ``migrate_hive_dbfs_root`` is
#                              false. Typically provided by an operator-
#                              set BUNDLE_VAR or left at the workspace
#                              config.yaml's value for subsequent reads.
#   batch_size               — integer ≥ 1 overriding batch_size in
#                              config.yaml. Empty → leave existing value.
#                              Hive integration passes "10" so its 12-
#                              table fixture exercises > 1 batch.
#   catalog_filter           — comma-separated allow-list of UC catalogs
#                              to discover. Empty → unchanged. Hive
#                              integration passes "integration_test_src"
#                              so UC discovery ignores the parallel
#                              ``integration_test_hive_ucref`` fixture
#                              seeded for the cross-catalog view test.
#
# --- Negative-path injections (integration X.3) ---
# These intentionally corrupt the config so a downstream task fails loud
# and safe. All default to "false" so the normal UC / Hive integration
# workflows are unaffected.
#
#   inject_bad_spn_id            — when "true", overwrite spn_client_id
#                                  with a syntactically-valid-but-wrong
#                                  UUID so auth fails at pre_check.
#   inject_unreachable_target    — when "true", overwrite
#                                  target_workspace_url with a
#                                  non-resolving hostname so pre_check's
#                                  target auth/metastore checks fail.

import shutil

# COMMAND ----------
# Bootstrap so we can reuse MigrationConfig's resolver for the config
# path (keeps "where does config.yaml live" in one place).
import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _files_root = "/Workspace" + _nb.split("/files/")[0] + "/files"
    _src = f"{_files_root}/src"
    # /files/src first (common.config resolution) then /files (tests.integration.*).
    for _p in (_src, _files_root):
        if _p not in sys.path:
            sys.path.insert(0, _p)
except NameError:
    pass

# COMMAND ----------

import yaml  # noqa: E402

from common.config import _resolve_bundle_config_path  # type: ignore[import-not-found]  # noqa: E402

config_path = _resolve_bundle_config_path()
backup_path = config_path + ".pre-integration-test.bak"

# Back up the current config exactly once. If the backup already exists
# (e.g. a previous run crashed before teardown, or an earlier task in
# the same negative-paths chain already took the backup), RESTORE
# from it to start this invocation from the pristine pre-test
# baseline — see the else branch below. Authoritative-not-additive
# semantics (S.14): each invocation's overrides apply on top of the
# real config, NEVER on top of a previous invocation's result.
import os  # noqa: E402

if not os.path.exists(backup_path):
    shutil.copy2(config_path, backup_path)
    print(f"Backed up {config_path} -> {backup_path}")
else:
    # Backup exists → restore config.yaml from it BEFORE applying this
    # invocation's overrides. Without this, successive runs (UC → Hive,
    # or chained negative-paths scenarios within a single workflow)
    # would start from the previous invocation's post-override config
    # and silently inherit UC-only keys (e.g. ``catalog_filter``,
    # ``batch_size``) that the current workflow never opts into.
    # Authoritative-not-additive: each invocation writes config.yaml
    # from the pristine baseline + its own overrides.
    shutil.copy2(backup_path, config_path)
    print(f"Backup already exists at {backup_path}; restored {config_path} from it to start clean.")

# COMMAND ----------

dbutils.widgets.text("iceberg_strategy", "")  # noqa: F821
dbutils.widgets.text("rls_cm_strategy", "")  # noqa: F821
dbutils.widgets.text("migrate_hive_dbfs_root", "false")  # noqa: F821
dbutils.widgets.text("hive_dbfs_target_path", "")  # noqa: F821
dbutils.widgets.text("batch_size", "")  # noqa: F821
dbutils.widgets.text("catalog_filter", "")  # noqa: F821
# Negative-path injection widgets (integration X.3). Default "false" so
# normal UC / Hive integration runs are unaffected.
dbutils.widgets.text("inject_bad_spn_id", "false")  # noqa: F821
dbutils.widgets.text("inject_unreachable_target", "false")  # noqa: F821


def _get_bool(key: str, default: str) -> bool:
    return str(dbutils.widgets.get(key) or default).strip().lower() == "true"  # type: ignore[name-defined]  # noqa: F821


def _get_str(key: str, default: str = "") -> str:
    return str(dbutils.widgets.get(key) or default).strip()  # type: ignore[name-defined]  # noqa: F821


iceberg_strategy = _get_str("iceberg_strategy", "")
rls_cm_strategy = _get_str("rls_cm_strategy", "")
migrate_hive_dbfs_root = _get_bool("migrate_hive_dbfs_root", "false")
hive_dbfs_target_path = _get_str("hive_dbfs_target_path", "")
batch_size_raw = _get_str("batch_size", "")
catalog_filter_raw = _get_str("catalog_filter", "")

# Negative-path injection toggles (integration X.3).
inject_bad_spn_id = _get_bool("inject_bad_spn_id", "false")
inject_unreachable_target = _get_bool("inject_unreachable_target", "false")

# Delegate the override transformation to a pure helper so the logic is
# unit-testable without dbutils / yaml I/O (S.14).

# COMMAND ----------

# Load the pristine baseline (the restore-from-backup step above
# guarantees config.yaml is the pre-test baseline here), transform it
# via the authoritative helper, and write the result back.
from tests.integration._config_override import apply_integration_overrides  # type: ignore[import-not-found]  # noqa: E402, I001

with open(config_path) as f:
    baseline_cfg = yaml.safe_load(f) or {}

cfg = apply_integration_overrides(
    baseline_cfg,
    iceberg_strategy=iceberg_strategy,
    rls_cm_strategy=rls_cm_strategy,
    migrate_hive_dbfs_root=migrate_hive_dbfs_root,
    hive_dbfs_target_path=hive_dbfs_target_path,
    batch_size_raw=batch_size_raw,
    catalog_filter_raw=catalog_filter_raw,
    inject_bad_spn_id=inject_bad_spn_id,
    inject_unreachable_target=inject_unreachable_target,
)

# rls_cm_strategy may have been rewritten inside the helper — pull the
# resolved value back for the summary print below.
rls_cm_strategy = cfg.get("rls_cm_strategy", rls_cm_strategy)

with open(config_path, "w") as f:
    yaml.safe_dump(cfg, f, sort_keys=False)

print(
    f"Overrode {config_path} for this integration test run:\n"
    f"  iceberg_strategy         = {iceberg_strategy!r}\n"
    f"  rls_cm_strategy          = {rls_cm_strategy!r}\n"
    f"  migrate_hive_dbfs_root   = {migrate_hive_dbfs_root}\n"
    f"  hive_dbfs_target_path    = {cfg.get('hive_dbfs_target_path', '')!r}\n"
    f"  batch_size               = {cfg.get('batch_size', '(unchanged)')}\n"
    f"  catalog_filter           = {cfg.get('catalog_filter', '(unchanged)')}\n"
    f"  [inject] bad_spn_id      = {inject_bad_spn_id}\n"
    f"  [inject] unreach_target  = {inject_unreachable_target}\n"
)
