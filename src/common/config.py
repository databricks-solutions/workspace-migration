from __future__ import annotations

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


def _coerce_bool(raw: object) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    return str(raw).strip().lower() in ("true", "1", "yes")


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
    # Iceberg (Phase 2.5) — set to "ddl_replay" to opt into Option A for
    # UC-managed Iceberg tables (DDL replay + re-ingest via cp_migration_share).
    # Leaving this empty blocks Iceberg migration so customers have to
    # explicitly acknowledge the trade-off (lost snapshot history / time
    # travel / branches + tags).
    iceberg_strategy: str = ""
    # Row filter / column mask on managed tables — Delta Sharing refuses to
    # share tables with legacy RLS/CM (``ALTER TABLE ... SET ROW FILTER`` /
    # ``SET MASK``). Supported values:
    #   ""              — default; affected tables are recorded in
    #                     migration_status with ``skipped_by_rls_cm_policy``
    #                     and their data does not move to target.
    #   "staging_copy"  — clone the table to an unprotected staging table on
    #                     source (in the migration tracking schema), share
    #                     the staging copy, CTAS into the target, then re-
    #                     apply the original RLS/CM on the target. Source
    #                     row filter / column mask are never altered. See
    #                     README and docs/RLS_CM_STAGING_COPY.md.
    rls_cm_strategy: str = ""
    # Hive (Phase 2) — unused in Phase 1 notebooks but fields exist so the
    # dataclass matches the full config file schema.
    migrate_hive_dbfs_root: bool = False
    hive_dbfs_target_path: str = ""
    hive_target_catalog: str = "hive_upgraded"
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
            batch_size=int(raw.get("batch_size", 50)),
            iceberg_strategy=str(raw.get("iceberg_strategy", "")),
            rls_cm_strategy=str(raw.get("rls_cm_strategy", "")),
            migrate_hive_dbfs_root=_coerce_bool(raw.get("migrate_hive_dbfs_root")),
            hive_dbfs_target_path=str(raw.get("hive_dbfs_target_path", "")),
            hive_target_catalog=str(raw.get("hive_target_catalog", "hive_upgraded")),
            lakebase_instance_name=str(raw.get("lakebase_instance_name", "cp-migration-lakebase")),
            lakebase_logical_database=str(raw.get("lakebase_logical_database", "databricks_postgres")),
            lakebase_capacity=str(raw.get("lakebase_capacity", "CU_1")),
            on_target_collision=_coerce_collision_policy(raw.get("on_target_collision", "fail")),
            test_kill_after=_coerce_test_kill_after(raw.get("test_kill_after")),
        )
