from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path

REQUIRED_FIELDS: tuple[str, ...] = (
    "source_workspace_url",
    "target_workspace_url",
    "spn_client_id",
    "spn_secret_scope",
    "spn_secret_key",
)


def _coerce_list(raw: object) -> list[str]:
    """Accept list, comma-separated string, or None; return list[str]."""
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, str):
        return [p.strip() for p in raw.split(",") if p.strip()]
    return []


def _coerce_cursor_columns(raw: object) -> dict[str, str]:
    """Parse the ``lfc_saas_cursor_columns`` field to a ``{dest_fqn: cursor}`` dict.

    Accepts a dict (the config.yaml mapping path) OR a JSON string (the
    workspace-file / job ``base_parameters`` path — base_parameters values are
    always strings). Empty string / None / empty mapping → ``{}``. A non-empty
    string that isn't a JSON object, or any other type, raises ``ValueError`` so
    a typo surfaces at config-load time rather than silently dropping cursors."""
    if raw is None or raw == "" or raw == {}:
        return {}
    if isinstance(raw, str):
        import json

        try:
            raw = json.loads(raw)
        except (TypeError, ValueError) as exc:
            msg = f"lfc_saas_cursor_columns must be a JSON object string, got {raw!r}"
            raise ValueError(msg) from exc
    if not isinstance(raw, dict):
        msg = f"lfc_saas_cursor_columns must be a mapping of dest_fqn -> cursor, got {raw!r}"
        raise ValueError(msg)
    return {str(k): str(v) for k, v in raw.items()}


def _coerce_bool(raw: object) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    return str(raw).strip().lower() in ("true", "1", "yes")


def _coerce_hive_staging_path(raw: dict) -> str:
    """Resolve the shared abfss staging path for the DBFS-root two-hop copy.

    Prefers the current ``hive_dbfs_staging_path`` key. Falls back to the
    deprecated ``hive_dbfs_target_path`` (the old UC-rehome key) with a
    DeprecationWarning so existing config.yaml files keep working through the
    like-for-like rename. Returns "" when neither is set.
    """
    new = raw.get("hive_dbfs_staging_path")
    if new:
        return str(new)
    old = raw.get("hive_dbfs_target_path")
    if old:
        warnings.warn(
            "hive_dbfs_target_path is deprecated; rename it to "
            "hive_dbfs_staging_path (shared abfss staging for the DBFS-root "
            "two-hop copy). The old key is still honoured for now.",
            DeprecationWarning,
            stacklevel=2,
        )
        return str(old)
    return ""


_COLLISION_POLICIES = ("fail", "skip")


def _coerce_collision_policy(raw: object) -> str:
    """Validate the on_target_collision field against the allow-list.

    Defaults to ``fail`` when unset or empty so operators who don't know
    about collision handling get the safe behaviour automatically.
    Unknown values raise a ``ValueError`` with the allow-list so typos
    surface at pre_check-time rather than silently becoming "skip".
    """
    if raw is None or raw == "":
        return "fail"
    val = str(raw).strip().lower()
    if val not in _COLLISION_POLICIES:
        msg = (
            f"on_target_collision must be one of {_COLLISION_POLICIES}, "
            f"got {raw!r}"
        )
        raise ValueError(msg)
    return val


def _coerce_test_kill_after(raw: object) -> int | None:
    """Parse the TEST-ONLY ``test_kill_after`` field.

    Accepts ``None`` / empty / 0 (all return None no-op) or a positive int.
    Negative values raise a ``ValueError`` so a typo surfaces at config-
    load time rather than silently becoming "off". Real validation /
    enforcement (refuse unless a test profile) happens at worker runtime
    in :mod:`migrate.reconciliation.maybe_kill`.
    """
    if raw is None or raw == "":
        return None
    try:
        val = int(raw)
    except (TypeError, ValueError) as exc:
        msg = f"test_kill_after must be a non-negative integer, got {raw!r}"
        raise ValueError(msg) from exc
    if val < 0:
        msg = f"test_kill_after must be non-negative, got {val}"
        raise ValueError(msg)
    if val == 0:
        return None
    return val


def _resolve_bundle_config_path() -> str:
    """Resolve config.yaml path from the running notebook's own workspace path.

    config.yaml is synced by DAB into the bundle's ``files/`` directory on
    every deploy, so it sits at ``${workspace.file_path}/config.yaml``. We
    derive that path by splitting the running notebook's own workspace path
    on ``/files/`` (the same trick the sys.path bootstrap at the top of each
    notebook uses) and re-joining under the ``/Workspace`` FUSE mount.

    Raises:
        RuntimeError: when not running under a Databricks notebook context
            (no ``dbutils`` available). Callers should pass an explicit path
            to :meth:`MigrationConfig.from_workspace_file` in that case.
    """
    dbutils = None
    try:
        import IPython  # type: ignore[import-not-found]

        ip = IPython.get_ipython()
        if ip is not None:
            dbutils = ip.user_ns.get("dbutils")
    except Exception:  # noqa: BLE001
        dbutils = None

    if dbutils is None:
        msg = (
            "Cannot auto-resolve config.yaml path: no dbutils available. "
            "Pass an explicit path to MigrationConfig.from_workspace_file()."
        )
        raise RuntimeError(msg)

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    nb_path = ctx.notebookPath().get()
    # nb_path looks like /Shared/cp_migration/<target>/files/<...>/notebook.
    # The config lives at <bundle_root>/files/config.yaml on the FUSE mount.
    bundle_root = nb_path.split("/files/")[0]
    return f"/Workspace{bundle_root}/files/config.yaml"


@dataclass
class MigrationConfig:
    """Configuration for a Unity Catalog migration run."""

    source_workspace_url: str
    target_workspace_url: str
    spn_client_id: str
    spn_secret_scope: str
    spn_secret_key: str
    catalog_filter: list[str] = field(default_factory=list)
    schema_filter: list[str] = field(default_factory=list)
    tracking_catalog: str = "migration_tracking"
    tracking_schema: str = "cp_migration"
    dry_run: bool = False
    batch_size: int = 50
    # Overwrite protection (review finding #2). When False (default), workers
    # that issue destructive CREATE OR REPLACE / DEEP CLONE skip re-creating a
    # target object that already exists and validate it instead — protecting a
    # post-cutover target from being clobbered on a resume / re-trigger. Set to
    # True only for a deliberate re-migration that should replace target data.
    overwrite_existing: bool = False
    # Ownership transfer (review finding #5). When True (default), the grants
    # workers transfer each migrated securable's ownership to its original
    # owner via ALTER ... OWNER TO (applied after the other grants so the
    # migration SPN keeps MANAGE while granting). Set False to leave every
    # migrated object owned by the migration SPN.
    transfer_ownership: bool = True
    # Iceberg (Phase 2.5) — set to "ddl_replay" to opt into Option A for
    # UC-managed Iceberg tables (DDL replay + re-ingest via cp_migration_share).
    # Leaving this empty blocks Iceberg migration so customers have to
    # explicitly acknowledge the trade-off (lost snapshot history / time
    # travel / branches + tags).
    iceberg_strategy: str = ""
    # Row filter / column mask on managed tables — Delta Sharing refuses to
    # share tables with legacy RLS/CM (``ALTER TABLE ... SET ROW FILTER`` /
    # ``SET MASK``). Supported values:
    # DEPRECATED (scope decision, findings #21/#16). Tables protected by a
    # row filter, column mask, OR an ABAC policy are NOT migrated — copying
    # them risks silent data loss (the copy reads through the policy) and
    # there is no safe source-untouched way to read the raw data. Discovery
    # now EXCLUDES all such tables and surfaces them in the dashboard for
    # manual handling. The old ``staging_copy`` strategy is removed; only the
    # default ("" = exclude + report) is meaningful. Field retained for
    # backward compatibility until the staging_copy code path is fully torn out.
    rls_cm_strategy: str = ""
    # Lakeflow Connect (Phase 3) — name of the pre-existing UC connection on
    # target that the recreated query-based pipeline will use. Required only
    # when any query-based LFC pipelines are included in the migration; left
    # empty otherwise and the pre-check is a no-op for this field.
    lfc_target_connection_name: str = ""
    # Lakeflow Connect Tier-1 SaaS (Salesforce/GA4/ServiceNow) — MANDATORY,
    # operator-supplied incremental cursor per migrated SaaS table. Maps the
    # destination-table FQN (``cat.schema.table``) -> cursor column name. SaaS
    # connectors do NOT echo their auto-selected cursor in the pipeline spec, so
    # the operator must declare it (run discovery first; it prints the candidate
    # timestamp/numeric columns per table). A SaaS table absent from this map
    # full-loads (no row_filter); the tool does NOT guess. Accepts a YAML mapping
    # or, on the base_parameters path, a JSON string.
    lfc_saas_cursor_columns: dict[str, str] = field(default_factory=dict)
    # Hive (Phase 2) — unused in Phase 1 notebooks but fields exist so the
    # dataclass matches the full config file schema.
    migrate_hive_dbfs_root: bool = False
    hive_dbfs_staging_path: str = ""
    # Online Tables → Lakebase synced table migration (migrate_online_tables).
    # The job creates this Lakebase database instance if it does not exist.
    lakebase_instance_name: str = "cp-migration-lakebase"
    lakebase_logical_database: str = "databricks_postgres"
    lakebase_capacity: str = "CU_1"
    # Target pre-existing state / collision handling (X.4).
    #
    # When a source object has the same FQN as an object on target AND no
    # migration_status row says we created it, pre_check emits a
    # ``target_collision`` pre_check_results row. The policy decides what
    # happens:
    #
    #   ``fail``  (default) — status FAIL, migrate refuses to start.
    #              Operator must rename, drop, or move the colliding object
    #              off the target before re-running.
    #   ``skip``  — status WARN, pre_check writes a ``skipped_target_exists``
    #              migration_status row so workers skip the object on the
    #              next migrate run. Target object is left untouched.
    #
    # Overwrite is intentionally not offered in v1 — destroying customer
    # data from a migration tool by default is almost never what anyone
    # wants.
    on_target_collision: str = "fail"
    # X.1 kill-injection (TEST ONLY). When set to a positive int, workers
    # raise ``SystemExit`` after processing that many objects in the current
    # batch so integration tests can simulate a cluster-death mid-migrate
    # and then exercise the reconciliation pass on the follow-up run.
    #
    # Refused at runtime unless the environment signals a test profile
    # (``WSM_TEST_MODE=1`` or ``DATABRICKS_ENVIRONMENT`` starts with
    # ``test``). See :mod:`migrate.reconciliation` for the check. NEVER
    # set this in a production config.yaml.
    test_kill_after: int | None = None

    @classmethod
    def from_workspace_file(cls, path: str | None = None) -> MigrationConfig:
        """Load migration config from a YAML file at a workspace path.

        When ``path`` is ``None`` (the default for workflow notebooks), resolve
        the config.yaml shipped alongside the current bundle deployment at
        ``${workspace.file_path}/config.yaml``. The path is discovered from
        the running notebook's own workspace location — the same trick the
        sys.path bootstrap at the top of each notebook uses — so the config
        file travels with the bundle and every ``databricks bundle deploy``
        re-syncs it from the local checkout.

        Tests and ad-hoc callers can pass an explicit ``path``.

        On Databricks, ``/Workspace/...`` paths resolve to local FUSE-mounted
        files that are readable with ``open()``. Required fields are validated;
        missing ones raise a clear error so pre-check can surface them.
        """
        import yaml

        resolved = path if path is not None else _resolve_bundle_config_path()
        content = Path(resolved).read_text()
        raw = yaml.safe_load(content) or {}
        if not isinstance(raw, dict):
            msg = f"Config file at {resolved} must be a YAML mapping, got {type(raw).__name__}"
            raise ValueError(msg)

        missing = [k for k in REQUIRED_FIELDS if not raw.get(k)]
        if missing:
            msg = f"Required config fields missing or empty in {resolved}: {missing}. Edit the file and re-run."
            raise ValueError(msg)

        return cls(
            source_workspace_url=str(raw["source_workspace_url"]),
            target_workspace_url=str(raw["target_workspace_url"]),
            spn_client_id=str(raw["spn_client_id"]),
            spn_secret_scope=str(raw["spn_secret_scope"]),
            spn_secret_key=str(raw["spn_secret_key"]),
            catalog_filter=_coerce_list(raw.get("catalog_filter")),
            schema_filter=_coerce_list(raw.get("schema_filter")),
            tracking_catalog=str(raw.get("tracking_catalog", "migration_tracking")),
            tracking_schema=str(raw.get("tracking_schema", "cp_migration")),
            dry_run=_coerce_bool(raw.get("dry_run")),
            overwrite_existing=_coerce_bool(raw.get("overwrite_existing")),
            transfer_ownership=_coerce_bool(raw.get("transfer_ownership", True)),
            batch_size=int(raw.get("batch_size", 50)),
            iceberg_strategy=str(raw.get("iceberg_strategy", "")),
            rls_cm_strategy=str(raw.get("rls_cm_strategy", "")),
            lfc_target_connection_name=str(raw.get("lfc_target_connection_name", "")),
            lfc_saas_cursor_columns=_coerce_cursor_columns(raw.get("lfc_saas_cursor_columns")),
            migrate_hive_dbfs_root=_coerce_bool(raw.get("migrate_hive_dbfs_root")),
            hive_dbfs_staging_path=_coerce_hive_staging_path(raw),
            lakebase_instance_name=str(raw.get("lakebase_instance_name", "cp-migration-lakebase")),
            lakebase_logical_database=str(raw.get("lakebase_logical_database", "databricks_postgres")),
            lakebase_capacity=str(raw.get("lakebase_capacity", "CU_1")),
            on_target_collision=_coerce_collision_policy(raw.get("on_target_collision", "fail")),
            test_kill_after=_coerce_test_kill_after(raw.get("test_kill_after")),
        )
