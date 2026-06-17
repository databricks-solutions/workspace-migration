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
    pass  # not running under a Databricks notebook (e.g. pytest)

# COMMAND ----------

# Discovery: unified entry point for UC and Hive discovery.
#
# Both domains always run and write to a single `discovery_inventory`
# table; rows are distinguished by the `source_type` column ('uc' or
# 'hive'). An empty Hive metastore (e.g., a workspace without legacy
# Hive tables) yields zero hive rows and is a normal no-op.

import contextlib
from collections import Counter
from datetime import datetime, timezone

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import find_warehouse, write_discovery_inventory_via_warehouse
from common.stateful_utils import CAPABILITY, StatefulExplorer
from common.tracking import TrackingManager, discovery_row, discovery_schema
from migrate.lfc_utils import (
    candidate_cursor_columns,
    classify_pipeline,
    extract_table_configs,
    resolve_saas_cursor,
)
from migrate.reconciliation import resolve_current_job_run_id

# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------


_MIGRATION_SHARE = "cp_migration_share"
_MIGRATION_RECIPIENT_PREFIX = "cp_migration_recipient_"


def _tool_owned_catalogs(config) -> set[str]:
    """Catalogs the tool itself owns — excluded from discovery so the tool
    doesn't try to migrate its own tracking/consumer state."""
    return {
        config.tracking_catalog,  # discovery_inventory, migration_status, pre_check_results
        f"{_MIGRATION_SHARE}_consumer",  # target-side share-consumer catalog (on source this won't exist, harmless)
    }


def _discover_uc(config, explorer, now) -> tuple[list[dict], int]:
    """Discover UC objects. Returns (rows, dlt_count)."""
    rows: list[dict] = []
    dlt_count = 0
    all_table_fqns: list[str] = []  # for workspace-level monitor enumeration
    # ABAC policies hang off catalog / schema / table securables (no
    # workspace-level list endpoint). Collect every securable we traverse
    # so policy enumeration covers all three levels after the main loop.
    all_securables: list[tuple[str, str]] = []

    catalogs = explorer.list_catalogs(filter_list=config.catalog_filter or None)
    tool_catalogs = _tool_owned_catalogs(config)
    catalogs = [c for c in catalogs if c not in tool_catalogs]
    # Foreign catalogs are captured separately as governance metadata via
    # list_foreign_catalogs(); iterating their information_schema here would
    # route through JDBC to the remote source and fail on UC-only columns.
    foreign_catalog_names = explorer.list_foreign_catalog_names()
    excluded_foreign: list[str] = []
    if foreign_catalog_names:
        excluded_foreign = [c for c in catalogs if c in foreign_catalog_names]
        catalogs = [c for c in catalogs if c not in foreign_catalog_names]
        if excluded_foreign:
            print(f"[uc] Excluding foreign catalog(s) from schema/table discovery: {sorted(excluded_foreign)}")
    if config.catalog_filter and not catalogs:
        # Non-empty filter that resolves to nothing iterable is almost always
        # operator misconfiguration (named a foreign catalog, named a
        # tool-owned catalog, or named a catalog that doesn't exist).
        # Empty filter is fine here — that's the "discover everything"
        # contract and an all-foreign workspace is a legitimate no-op.
        raise ValueError(
            f"catalog_filter={config.catalog_filter} resolved to zero catalogs after "
            f"excluding foreign={sorted(excluded_foreign)} and tool-owned={sorted(tool_catalogs)}. "
            "Check the filter against the workspace's managed catalogs."
        )
    print(f"[uc] Discovered {len(catalogs)} catalog(s) (excluding tool-owned {sorted(tool_catalogs)}): {catalogs}")

    for catalog in catalogs:
        all_securables.append(("CATALOG", catalog))
        schemas = explorer.list_schemas(catalog)
        if config.schema_filter:
            schemas = [s for s in schemas if s in config.schema_filter]
        print(f"  [uc] Catalog '{catalog}': {len(schemas)} schema(s)")

        for schema in schemas:
            all_securables.append(("SCHEMA", f"{catalog}.{schema}"))
            # --- Tables and views ---
            tables = explorer.classify_tables(catalog, schema)
            for tbl in tables:
                fqn = tbl["fqn"]
                obj_type = tbl["object_type"]

                if obj_type == "view":
                    is_dlt, pipeline_id = False, None
                else:
                    is_dlt, pipeline_id = explorer.detect_dlt_managed(fqn)
                    # MVs / STs always have a pipeline_id (auto-provisioned or
                    # DLT-defined). Only flag as DLT-managed if the underlying
                    # pipeline is user-owned (non-empty spec.libraries). That
                    # distinction is deferred to mv_st_worker.
                    if obj_type in ("mv", "st"):
                        is_dlt = False  # distinguish later via pipelines.get()
                    if is_dlt:
                        dlt_count += 1

                row_count = 0
                size_bytes = 0
                create_stmt = ""
                table_format: str | None = None

                # MV / ST row counts via SELECT COUNT(*) can be expensive or
                # block on auto-refresh; skip them and let the target do its
                # own validation after REFRESH.
                if obj_type not in ("view", "mv", "st"):
                    with contextlib.suppress(Exception):
                        row_count = explorer.get_table_row_count(fqn)
                    with contextlib.suppress(Exception):
                        size_bytes = explorer.get_table_size_bytes(fqn)
                    with contextlib.suppress(Exception):
                        table_format = explorer.get_table_format(fqn)

                ddl_failure: str | None = None
                try:
                    create_stmt = explorer.get_create_statement(fqn)
                except Exception as exc:  # noqa: BLE001
                    # Iceberg managed tables and some UC object types don't support
                    # SHOW CREATE TABLE — record the reason in metadata_json so
                    # workers can decide how to handle it, and keep discovery
                    # going.
                    ddl_failure = f"get_create_statement failed: {type(exc).__name__}: {exc}"
                    print(f"    [uc][warn] {fqn}: {ddl_failure}")

                rows.append(
                    discovery_row(
                        source_type="uc",
                        object_type=obj_type,
                        object_name=fqn,
                        catalog_name=catalog,
                        schema_name=schema,
                        discovered_at=now,
                        row_count=row_count,
                        size_bytes=size_bytes,
                        is_dlt_managed=is_dlt,
                        pipeline_id=pipeline_id,
                        create_statement=create_stmt,
                        format=table_format,
                        metadata={"ddl_failure": ddl_failure} if ddl_failure else None,
                    )
                )
                if obj_type in ("managed_table", "external_table"):
                    all_table_fqns.append(fqn)
                # ABAC policies can attach to views too, so enumerate every
                # table/view as a TABLE-type securable (UC uses TABLE for
                # both in the policies API).
                if obj_type in ("managed_table", "external_table", "view", "mv", "st"):
                    all_securables.append(("TABLE", fqn.strip("`").replace("`.`", ".")))

            # --- Functions ---
            for func_fqn in explorer.list_functions(catalog, schema):
                ddl = ""
                with contextlib.suppress(Exception):
                    ddl = explorer.get_function_ddl(func_fqn)

                rows.append(
                    discovery_row(
                        source_type="uc",
                        object_type="function",
                        object_name=func_fqn,
                        catalog_name=catalog,
                        schema_name=schema,
                        discovered_at=now,
                        create_statement=ddl,
                    )
                )

            # --- Volumes ---
            for vol in explorer.list_volumes(catalog, schema):
                rows.append(
                    discovery_row(
                        source_type="uc",
                        object_type="volume",
                        object_name=vol["fqn"],
                        catalog_name=catalog,
                        schema_name=schema,
                        discovered_at=now,
                        table_type=vol.get("volume_type"),  # MANAGED or EXTERNAL
                        storage_location=vol.get("storage_location"),
                    )
                )

            # --- Phase 3 governance: per-schema objects ---
            for tag in explorer.list_tags(catalog, schema):
                rows.append(
                    discovery_row(
                        source_type="uc",
                        object_type="tag",
                        object_name=(f"{tag['securable_fqn']}:{tag.get('column_name', '')}:{tag['tag_name']}").rstrip(
                            ":"
                        ),
                        catalog_name=catalog,
                        schema_name=schema,
                        discovered_at=now,
                        metadata=tag,
                    )
                )

            for rf in explorer.list_row_filters(catalog, schema):
                rows.append(
                    discovery_row(
                        source_type="uc",
                        object_type="row_filter",
                        object_name=rf["table_fqn"],
                        catalog_name=catalog,
                        schema_name=schema,
                        discovered_at=now,
                        metadata=rf,
                    )
                )

            for cm in explorer.list_column_masks(catalog, schema):
                rows.append(
                    discovery_row(
                        source_type="uc",
                        object_type="column_mask",
                        object_name=f"{cm['table_fqn']}.{cm['column_name']}",
                        catalog_name=catalog,
                        schema_name=schema,
                        discovered_at=now,
                        metadata=cm,
                    )
                )

            for m in explorer.list_registered_models(catalog, schema):
                rows.append(
                    discovery_row(
                        source_type="uc",
                        object_type="registered_model",
                        object_name=m["model_fqn"],
                        catalog_name=catalog,
                        schema_name=schema,
                        discovered_at=now,
                        storage_location=m.get("storage_location"),
                        metadata=m,
                    )
                )

    # --- Phase 3 governance: workspace-level objects ---
    # Monitors are per-table; enumerate over every discovered table.
    for mon in explorer.list_monitors(all_table_fqns):
        rows.append(
            discovery_row(
                source_type="uc",
                object_type="monitor",
                object_name=mon["table_fqn"],
                catalog_name=None,
                schema_name=None,
                discovered_at=now,
                metadata=mon,
            )
        )

    for p in explorer.list_policies(all_securables):
        securable_fqn = p.get("securable_fqn", "?")
        # Make object_name unique across securables even when two different
        # securables have a same-named policy: scope with the securable FQN.
        policy_name = p["policy_name"] or "unnamed"
        rows.append(
            discovery_row(
                source_type="uc",
                object_type="policy",
                object_name=f"{securable_fqn}::{policy_name}",
                catalog_name=None,
                schema_name=None,
                discovered_at=now,
                metadata=p,
            )
        )

    for c in explorer.list_connections():
        rows.append(
            discovery_row(
                source_type="uc",
                object_type="connection",
                object_name=c["connection_name"],
                catalog_name=None,
                schema_name=None,
                discovered_at=now,
                metadata=c,
            )
        )

    for fc in explorer.list_foreign_catalogs():
        rows.append(
            discovery_row(
                source_type="uc",
                object_type="foreign_catalog",
                object_name=fc["catalog_name"],
                catalog_name=fc["catalog_name"],
                schema_name=None,
                discovered_at=now,
                metadata=fc,
            )
        )

    for ot in explorer.list_online_tables():
        ot_meta = dict(ot)
        ot_meta["capability"] = CAPABILITY["online_table"]
        rows.append(
            discovery_row(
                source_type="stateful",
                object_type="online_table",
                object_name=ot["online_table_fqn"],
                catalog_name=None,
                schema_name=None,
                discovered_at=now,
                metadata=ot_meta,
            )
        )

    exclude_shares = frozenset({_MIGRATION_SHARE})
    for s in explorer.list_shares(exclude_names=exclude_shares):
        rows.append(
            discovery_row(
                source_type="uc",
                object_type="share",
                object_name=s["share_name"],
                catalog_name=None,
                schema_name=None,
                discovered_at=now,
                metadata=s,
            )
        )

    for r in explorer.list_recipients(exclude_prefix=_MIGRATION_RECIPIENT_PREFIX):
        rows.append(
            discovery_row(
                source_type="uc",
                object_type="recipient",
                object_name=r["recipient_name"],
                catalog_name=None,
                schema_name=None,
                discovered_at=now,
                metadata=r,
            )
        )

    for p in explorer.list_providers():
        rows.append(
            discovery_row(
                source_type="uc",
                object_type="provider",
                object_name=p["provider_name"],
                catalog_name=None,
                schema_name=None,
                discovered_at=now,
                metadata=p,
            )
        )

    _warn_rls_cm_tables(rows, config)

    return rows, dlt_count


def _warn_rls_cm_tables(rows: list[dict], config) -> None:
    """Surface a prominent warning listing tables with row filter / column
    mask — Delta Sharing refuses to share these, and the tool skips them by
    default. Operators can opt into the staging_copy path via
    ``config.rls_cm_strategy``.
    """
    import json as _json

    rls_cm_tables: set[str] = set()
    for r in rows:
        ot = r.get("object_type")
        if ot == "row_filter" and r.get("object_name"):
            rls_cm_tables.add(r["object_name"])
        elif ot == "column_mask" and r.get("metadata_json"):
            try:
                meta = _json.loads(r["metadata_json"])
            except _json.JSONDecodeError:
                continue
            tbl = meta.get("table_fqn")
            if tbl:
                rls_cm_tables.add(tbl)

    if not rls_cm_tables:
        return

    strategy = (getattr(config, "rls_cm_strategy", "") or "").strip().lower()
    print()
    print("=" * 78)
    print("!! TABLES WITH ROW FILTER / COLUMN MASK DETECTED")
    print("=" * 78)
    print(f"Discovery found {len(rls_cm_tables)} managed table(s) protected by a row filter or column mask:")
    for fqn in sorted(rls_cm_tables):
        print(f"  - {fqn}")
    print()
    print("Delta Sharing providers cannot share tables with legacy RLS/CM (ALTER TABLE ... SET ROW FILTER / SET MASK).")
    print()
    if strategy == "staging_copy":
        print(
            "config.rls_cm_strategy = 'staging_copy' — setup_sharing will CTAS "
            "each affected table into the migration tracking catalog's "
            "cp_migration_staging schema, share the staging copy, and the "
            "migrate workers will DEEP CLONE that copy. Source RLS/CM "
            "untouched."
        )
    else:
        print(
            "Default behavior: these tables WILL BE SKIPPED during migration. "
            "Their data will NOT move to target. migration_status will record "
            "status 'skipped_by_rls_cm_policy'."
        )
        print()
        print("Options to migrate their data (see README.md for details):")
        print(
            "  1. Rewrite their governance as ABAC policies before migrating — "
            "Delta Sharing supports sharing tables protected by ABAC."
        )
        print(
            "  2. Set rls_cm_strategy='staging_copy' — clones each affected "
            "table into a staging schema and shares the copy. Source RLS/CM "
            "untouched. Requires the migration SPN to be a workspace admin."
        )
    print("=" * 78)
    print()


def _discover_hive(config, explorer, now) -> list[dict]:
    """Discover Hive objects. Returns rows list."""
    rows: list[dict] = []
    databases = explorer.list_hive_databases()
    print(f"[hive] Discovered {len(databases)} database(s): {databases}")

    for database in databases:
        # --- Tables and views ---
        for tbl in explorer.classify_hive_tables(database):
            row_count = 0
            size_bytes = 0
            if tbl["object_type"] == "hive_table":
                with contextlib.suppress(Exception):
                    row_count = explorer.get_table_row_count(tbl["fqn"])
                with contextlib.suppress(Exception):
                    size_bytes = explorer.get_table_size_bytes(tbl["fqn"])

            rows.append(
                discovery_row(
                    source_type="hive",
                    object_type=tbl["object_type"],
                    object_name=tbl["fqn"],
                    catalog_name="hive_metastore",
                    schema_name=database,
                    discovered_at=now,
                    row_count=row_count,
                    size_bytes=size_bytes,
                    data_category=tbl["data_category"],
                    table_type=tbl["table_type"],
                    provider=tbl["provider"],
                    storage_location=tbl["storage_location"],
                )
            )

        # --- Functions ---
        for func_fqn in explorer.list_hive_functions(database):
            rows.append(
                discovery_row(
                    source_type="hive",
                    object_type="hive_function",
                    object_name=func_fqn,
                    catalog_name="hive_metastore",
                    schema_name=database,
                    discovered_at=now,
                    data_category="hive_function",
                    table_type="",
                    provider="",
                    storage_location="",
                )
            )

    return rows


# COMMAND ----------


def _annotate_saas_cursor_candidates(meta: dict, describe_columns) -> None:
    """For a Tier-1 SaaS lfc_pipeline meta dict, DESCRIBE each landed destination
    table on the SOURCE and record the plausible cursor columns (timestamp/date/
    numeric) under ``metadata['candidate_cursor_columns']`` (keyed by dest FQN),
    plus a doc-recommended suggestion. Prints one operator-facing summary line
    per table so the operator can populate ``lfc_saas_cursor_columns`` before
    migration. Tables that don't exist yet describe to an empty list (no crash).
    No-op for non-Tier-1-SaaS pipelines (no cursor handle to set)."""
    definition = (meta or {}).get("definition") or {}
    if classify_pipeline(definition) != ("saas", "tier1"):
        return
    source_type = str((((definition.get("spec") or {}).get("ingestion_definition")) or {})
                       .get("source_type") or "").upper()
    candidates: dict[str, list[str]] = {}
    for c in extract_table_configs(definition):
        dest_fqn = f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}"
        try:
            cols = candidate_cursor_columns(describe_columns(dest_fqn))
        except Exception as exc:  # noqa: BLE001 — table may not exist yet; skip gracefully
            print(f"[stateful][lfc] {dest_fqn}: cannot DESCRIBE yet ({type(exc).__name__}); "
                  "no cursor candidates")
            cols = []
        candidates[dest_fqn] = cols
        suggested = resolve_saas_cursor(source_type, cols)
        hint = f" (suggested: {suggested})" if suggested else ""
        print(f"[stateful][lfc] {source_type} {dest_fqn}: candidate cursor columns = "
              f"{cols or '[]'}{hint} — set in lfc_saas_cursor_columns before migrate_lfc")
    meta["candidate_cursor_columns"] = candidates


def _discover_stateful(config, stateful, now, *, describe_columns=None) -> list[dict]:
    """Discover stateful-service objects (source_type='stateful').

    Each surface's list_* returns dicts with a name key + a "definition" raw
    spec. We tag every row source_type='stateful' and stash the capability
    subtype alongside the raw spec in metadata_json. Dependency analysis is a
    later step that reads these rows; discovery only enumerates + persists.

    When ``describe_columns`` (a ``dest_fqn -> list[describe_rows]`` callable) is
    supplied, Tier-1 SaaS lfc_pipeline rows are additionally annotated with the
    candidate cursor columns of each landed destination table — a discovery aid
    so the operator can populate the MANDATORY ``lfc_saas_cursor_columns`` map.
    """
    # `config` is accepted for symmetry with _discover_uc/_discover_hive and
    # reserved for a future stateful catalog/scope filter; unused today.
    # (object_type, list_fn, name_key)
    surfaces = [
        ("vector_search_index", stateful.list_vector_search_indexes, "index_name"),
        ("app", stateful.list_apps, "app_name"),
        ("database_instance", stateful.list_database_instances, "instance_name"),
        ("synced_table", stateful.list_synced_tables, "synced_table_name"),
        ("model_serving_endpoint", stateful.list_model_serving_endpoints, "endpoint_name"),
        ("lfc_pipeline", stateful.list_lfc_pipelines, "pipeline_name"),
    ]
    rows: list[dict] = []
    for obj_type, list_fn, name_key in surfaces:
        for item in list_fn():
            meta = dict(item)
            meta["capability"] = CAPABILITY[obj_type]
            if obj_type == "lfc_pipeline" and describe_columns is not None:
                _annotate_saas_cursor_candidates(meta, describe_columns)
            rows.append(
                discovery_row(
                    source_type="stateful",
                    object_type=obj_type,
                    object_name=item[name_key],
                    catalog_name=None,
                    schema_name=None,
                    discovered_at=now,
                    metadata=meta,
                )
            )
    return rows


# COMMAND ----------


def _read_scope(dbutils) -> str:
    """Discovery scope, from the ``discovery_scope`` widget. Values:

    * ``all``  (default) — UC + hive + stateful on one (serverless) compute,
      writing inventory via spark. Back-compat for non-hive-on-ADLS suites.
    * ``uc``   — UC + stateful only (skip hive). Use for the serverless pass
      when hive lives on ADLS and is discovered separately on classic compute.
    * ``hive`` — hive only, reading source on the (classic, NON-UC) cluster's
      spark and writing inventory through the SQL warehouse. hive_metastore
      tables on ADLS can't be read on serverless/UC compute (their Delta log
      lives on ADLS), so this scope runs on a classic No-Isolation cluster.
    """
    try:
        dbutils.widgets.text("discovery_scope", "all")
        v = (dbutils.widgets.get("discovery_scope") or "all").strip().lower()
        return v if v in ("all", "uc", "hive") else "all"
    except Exception:  # noqa: BLE001 — not in a notebook / no widget
        return "all"


def run(dbutils, spark):  # noqa: D103
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    scope = _read_scope(dbutils)
    job_run_id = resolve_current_job_run_id(dbutils)
    explorer = CatalogExplorer(spark, auth)
    now = datetime.now(tz=timezone.utc)
    print(f"[discovery] scope = {scope}")

    # --- hive-only scope: classic compute, warehouse-routed inventory write ---
    # No UC spark access here (NON-UC cluster), so we DON'T init tracking tables
    # (the uc/all pass already created them) and DON'T write via spark.
    if scope == "hive":
        print("[hive] Scanning hive_metastore (classic compute)...")
        hive_rows = _discover_hive(config, explorer, now)
        print(f"\nTotal hive objects discovered: {len(hive_rows)}")
        # Tracking catalog is on the SOURCE metastore — use a source warehouse.
        wh_id = find_warehouse(auth, use_source=True)
        tracking_fqn = f"{config.tracking_catalog}.{config.tracking_schema}"
        write_discovery_inventory_via_warehouse(auth, wh_id, tracking_fqn, hive_rows, source_type="hive")
        print("Hive discovery inventory written via warehouse.")
        return hive_rows

    tracker = TrackingManager(spark, config)
    tracker.job_run_id = job_run_id
    tracker.init_tracking_tables()

    inventory: list[dict] = []
    dlt_count = 0

    print("[uc] Scanning UC catalogs...")
    uc_rows, dlt_count = _discover_uc(config, explorer, now)
    inventory.extend(uc_rows)

    # scope 'uc' skips hive (it's discovered separately on classic compute);
    # scope 'all' keeps the legacy single-compute behaviour.
    if scope == "all":
        print("[hive] Scanning hive_metastore...")
        inventory.extend(_discover_hive(config, explorer, now))

    print("[stateful] Scanning stateful-service surfaces...")
    stateful_explorer = StatefulExplorer(auth)

    def _describe_columns(fqn: str) -> list:
        """Raw DESCRIBE TABLE rows of a landed table on the SOURCE warehouse.
        Used to surface candidate SaaS cursor columns; raises on a missing table
        so the annotator can skip it gracefully."""
        from common.sql_utils import execute_and_fetch

        src_wh_id = find_warehouse(auth, use_source=True)
        bt = ".".join(f"`{p}`" for p in fqn.split("."))
        res = execute_and_fetch(auth, src_wh_id, f"DESCRIBE TABLE {bt}", use_source=True)
        if res["state"] != "SUCCEEDED":
            raise RuntimeError(f"DESCRIBE failed for {fqn}: {res.get('error', res['state'])}")
        return res.get("rows") or []

    inventory.extend(_discover_stateful(config, stateful_explorer, now,
                                        describe_columns=_describe_columns))

    print(f"\nTotal objects discovered: {len(inventory)}")

    if inventory:
        df = spark.createDataFrame(inventory, schema=discovery_schema())
        tracker.write_discovery_inventory(df)
        print("Discovery inventory written to tracking table.")
    else:
        print("WARNING: No objects discovered. Check catalog/schema/scope filters.")

    # Summary by (source_type, object_type)
    type_counts = Counter((obj["source_type"], obj["object_type"]) for obj in inventory)
    print(f"\n{'Source':<8} {'Object Type':<20} {'Count':>8}")
    print("-" * 40)
    for (src, obj_type), count in sorted(type_counts.items()):
        print(f"{src:<8} {obj_type:<20} {count:>8}")
    print("-" * 40)
    print(f"{'TOTAL':<28} {len(inventory):>8}")

    if dlt_count > 0:
        print(f"\n** {dlt_count} DLT-managed table(s) detected. These require special handling during migration. **")

    return inventory


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
