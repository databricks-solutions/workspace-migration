"""Pure config-override helper extracted from ``setup_test_config.py``.

``setup_test_config.py`` is a Databricks notebook that rewrites the
workspace copy of ``config.yaml`` with per-workflow toggles. The
transformation itself is pure (dict in, dict out) — we keep it here so
it is unit-testable without a workspace / dbutils / yaml file I/O.

**Authoritative-not-additive contract.**
``apply_integration_overrides`` takes a **fresh baseline** config and
returns the override applied on top. Callers (the notebook) must
supply the pre-test baseline each invocation — NOT the previous run's
contaminated config. Specifically:

- UC integration sets ``rls_cm_strategy=staging_copy`` +
  ``iceberg_strategy=ddl_replay``.
- Hive integration sets ``batch_size=10`` + ``catalog_filter=integration_test_src``.
- Negative-paths runs chain scenarios that each inject a different
  corruption (``inject_bad_spn_id`` / ``inject_unreachable_target``).

If a UC run's post-override config were (wrongly) fed into Hive's
override, the Hive run would silently inherit a UC-specific
``catalog_filter`` or similar cross-workflow contamination. The
notebook guards against this by restoring from
``.pre-integration-test.bak`` before every invocation, so this helper
always sees the pristine baseline.
"""

from __future__ import annotations

import copy
from typing import Any


def apply_integration_overrides(
    baseline_cfg: dict,
    *,
    iceberg_strategy: str,
    rls_cm_strategy: str,
    migrate_hive_dbfs_root: bool,
    hive_dbfs_target_path: str,
    batch_size_raw: str,
    catalog_filter_raw: str,
    lfc_target_connection_name_raw: str = "",
    lfc_saas_cursor_columns_raw: str = "",
    inject_bad_spn_id: bool = False,
    inject_unreachable_target: bool = False,
) -> dict[str, Any]:
    """Apply per-workflow overrides to a pristine ``config.yaml`` dict.

    Returns a new dict — ``baseline_cfg`` is deep-copied so callers can
    reuse the same baseline across multiple scenarios without
    cross-contamination.
    """
    cfg = copy.deepcopy(baseline_cfg) if baseline_cfg else {}

    cfg["iceberg_strategy"] = iceberg_strategy
    cfg["rls_cm_strategy"] = rls_cm_strategy
    cfg["migrate_hive_dbfs_root"] = migrate_hive_dbfs_root
    if hive_dbfs_target_path:
        cfg["hive_dbfs_target_path"] = hive_dbfs_target_path
    # If hive_dbfs_target_path is not provided, leave the existing
    # operator-configured value in place.
    if batch_size_raw:
        try:
            cfg["batch_size"] = max(1, int(batch_size_raw))
        except ValueError as _exc:
            raise ValueError(
                f"batch_size must be an integer, got {batch_size_raw!r}"
            ) from _exc
    if catalog_filter_raw:
        cfg["catalog_filter"] = [
            x.strip() for x in catalog_filter_raw.split(",") if x.strip()
        ]
    if lfc_target_connection_name_raw:
        cfg["lfc_target_connection_name"] = lfc_target_connection_name_raw
    # JSON string ``{dest_fqn: cursor}`` — stored as-is; MigrationConfig's
    # _coerce_cursor_columns parses a JSON string (or a YAML mapping) to a dict.
    if lfc_saas_cursor_columns_raw:
        cfg["lfc_saas_cursor_columns"] = lfc_saas_cursor_columns_raw

    # --- Negative-path injections (integration X.3) ---
    # Applied AFTER the scope overrides so we are corrupting the
    # post-scope config, not the original file.
    if inject_bad_spn_id:
        cfg["spn_client_id"] = "00000000-0000-0000-0000-000000000000"
    if inject_unreachable_target:
        cfg["target_workspace_url"] = "https://adb-0000000000000000.0.azuredatabricks.net"

    return cfg
