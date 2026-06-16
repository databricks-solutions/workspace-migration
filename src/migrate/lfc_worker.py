# Databricks notebook source
# COMMAND ----------
from __future__ import annotations  # noqa: E402

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
import json
import logging
import time

from migrate.lfc_utils import (
    build_query_based_create_spec,
    build_unified_view_sql,
    classify_pipeline,
    extract_table_configs,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lfc_worker")
# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def _bt(fqn: str) -> str:
    """Backtick a dotted FQN: a.b.c -> `a`.`b`.`c`."""
    return ".".join(f"`{p}`" for p in fqn.split("."))


def migrate_pipeline(row: dict, *, deps, target_connection_name: str) -> list[dict]:
    """Migrate one query-based lfc_pipeline row. Returns status dicts.

    Non-query-based rows return [] (left for a later stage). Per-pipeline
    isolation is the caller's job (it wraps this in try/except per row).
    """
    obj_name = row["object_name"]
    definition = (json.loads(row.get("metadata_json") or "{}") or {}).get("definition") or {}
    kind, _tier = classify_pipeline(definition)
    if kind != "query_based":
        logger.info("[lfc] %s is %s — deferred to a later stage.", obj_name, kind)
        return []

    cfgs = extract_table_configs(definition)
    canon = [f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}" for c in cfgs]
    if canon and all(deps.target_view_exists(_bt(f)) for f in canon):
        return [{"object_name": obj_name, "object_type": "lfc_pipeline",
                 "status": "skipped_target_pipeline_exists", "error_message": None}]

    results: list[dict] = []
    boundaries: dict[str, str] = {}
    start = time.time()

    for c in cfgs:
        if not c.get("cursor_column"):
            continue  # batch table: no clone, recreated pipeline full-loads it
        src_table_fqn = f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}"
        boundaries[c["source_table"]] = deps.compute_boundary(src_table_fqn, c["cursor_column"])
        clone = deps.clone_history(c)
        results.append({"object_name": f"{src_table_fqn}_history", "object_type": "lfc_table",
                        "status": clone["status"], "error_message": clone.get("error_message")})

    spec = build_query_based_create_spec(
        definition, target_connection_name=target_connection_name,
        boundaries=boundaries, name=f"{obj_name}_migrated",
    )
    deps.create_pipeline(spec)
    results.append({"object_name": obj_name, "object_type": "lfc_pipeline",
                    "status": "lfc_pipeline_created_incremental", "error_message": None,
                    "duration_seconds": time.time() - start})

    for c in cfgs:
        if not c.get("cursor_column"):
            continue
        base = f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}"
        sql = build_unified_view_sql(
            canonical=_bt(base), history=_bt(f"{base}_history"), incr=_bt(f"{base}_incr"),
            scd_type=c["scd_type"], primary_keys=c["primary_keys"], cursor_column=c["cursor_column"],
        )
        deps.create_view(sql)
        results.append({"object_name": base, "object_type": "lfc_view",
                        "status": "lfc_view_created", "error_message": None})
    return results


# COMMAND ----------


def run(dbutils, spark) -> None:  # noqa: ARG001 — spark unused; kept for worker-signature uniformity
    """Entry point when running as a Databricks notebook.

    Wires real deps over live Databricks clients and loops all pending
    lfc_pipeline rows published by the orchestrator under 'lfc_pipeline_list'.
    """
    import types

    from common.auth import AuthManager
    from common.catalog_utils import CatalogExplorer
    from common.config import MigrationConfig
    from common.sql_utils import execute_and_fetch, execute_and_poll, find_warehouse
    from common.tracking import TrackingManager
    from common.validation import Validator
    from migrate.managed_table_worker import clone_table
    from migrate.reconciliation import resolve_current_job_run_id
    from migrate.rls_cm import make_staging_table_fqn
    from migrate.setup_sharing import (
        SHARE_NAME,
        add_tables_to_share,
        ensure_share_consumer_catalog,
        ensure_target_catalogs_and_schemas,
        get_or_create_share,
    )

    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

    source_explorer = CatalogExplorer(spark, auth)
    target_explorer = CatalogExplorer(spark, auth)
    validator = Validator(source_explorer, target_explorer)

    run_id = tracker.job_run_id or "unknown"
    src_wh_id = find_warehouse(auth, use_source=True)
    tgt_wh_id = find_warehouse(auth, use_source=False)

    rows_json = dbutils.jobs.taskValues.get(  # type: ignore[union-attr]
        taskKey="orchestrator", key="lfc_pipeline_list", debugValue="[]"
    )
    rows = json.loads(rows_json) if rows_json else []
    logger.info("[lfc_worker] %d pipeline(s) to migrate", len(rows))

    # ---------------------------------------------------------------------------
    # Build the real deps seam
    # ---------------------------------------------------------------------------

    def _compute_boundary(table_fqn: str, cursor_column: str) -> str:
        """MAX(cursor) on the SOURCE warehouse over the current landed table."""
        sql = f"SELECT CAST(MAX(`{cursor_column}`) AS STRING) FROM {_bt(table_fqn)}"
        res = execute_and_fetch(auth, src_wh_id, sql, use_source=True)
        if res["state"] != "SUCCEEDED":
            raise RuntimeError(
                f"compute_boundary query failed for {table_fqn}: {res.get('error', res['state'])}"
            )
        rows_data = res.get("rows") or []
        if not rows_data or not rows_data[0]:
            raise RuntimeError(f"compute_boundary returned no rows for {table_fqn}")
        return str(rows_data[0][0])

    def _clone_history(table_cfg: dict) -> dict:
        """Path-A staging clone of the landed Streaming Table into a _history table on target.

        Steps:
        1. Derive original FQN (dest_catalog.dest_schema.dest_table) = the landed ST.
        2. CTAS the ST into a staging table in tracking_catalog.cp_migration_staging.
        3. Ensure share / consumer catalog / target catalog+schema exist.
        4. call clone_table to DEEP CLONE the staging copy onto target as <table>_history.

        CONCERN: setup_sharing helpers (ensure_target_catalogs_and_schemas,
        add_tables_to_share, ensure_share_consumer_catalog) expect a list[dict]
        of objects with keys 'object_name', 'catalog_name', 'schema_name'. We
        synthesize that shape below from table_cfg. Verified against
        setup_sharing.py source. clone_table is called with the staging share
        entry so the consumer path resolves correctly.
        """
        dest_cat = table_cfg["destination_catalog"]
        dest_sch = table_cfg["destination_schema"]
        dest_tbl = table_cfg["destination_table"]
        original_fqn = _bt(f"{dest_cat}.{dest_sch}.{dest_tbl}")
        target_history_fqn = _bt(f"{dest_cat}.{dest_sch}.{dest_tbl}_history")

        # Step 2: CTAS into staging on source
        staging_fqn = make_staging_table_fqn(original_fqn, run_id, config.tracking_catalog)
        ctas_sql = f"CREATE OR REPLACE TABLE {staging_fqn} AS SELECT * FROM {original_fqn}"
        res = execute_and_poll(auth, src_wh_id, ctas_sql, use_source=True)
        if res["state"] != "SUCCEEDED":
            return {
                "status": "failed",
                "error_message": f"staging CTAS failed for {original_fqn}: {res.get('error', res['state'])}",
            }
        tracker.record_staging_created(original_fqn=original_fqn, staging_fqn=staging_fqn)

        # Step 3: ensure share infrastructure
        # ensure_target_catalogs_and_schemas expects list[dict] with catalog_name / schema_name keys
        target_tbl_list = [
            {"object_name": target_history_fqn, "catalog_name": dest_cat, "schema_name": dest_sch}
        ]
        get_or_create_share(auth, SHARE_NAME)
        # staging_fqn shape: `<tcat>`.`cp_migration_staging`.`stg_<hash>`
        # build the share entry for the staging table
        staging_clean = staging_fqn.strip("`").replace("`.`", ".")
        stg_parts = staging_clean.split(".")
        staging_share_entry = {
            "object_name": staging_fqn,
            "catalog_name": stg_parts[0] if len(stg_parts) == 3 else config.tracking_catalog,
            "schema_name": "cp_migration_staging",
        }
        add_tables_to_share(auth, SHARE_NAME, [staging_share_entry])
        ensure_target_catalogs_and_schemas(auth, target_tbl_list)
        ensure_share_consumer_catalog(auth, SHARE_NAME, dry_run=False)

        # Step 4: deep clone via clone_table
        # staging_share_entry mimics a managed_table row so clone_table can derive the consumer path
        return clone_table(
            {"object_name": staging_fqn, "format": "delta"},
            config=config,
            auth=auth,
            tracker=tracker,
            validator=validator,
            wh_id=tgt_wh_id,
            share_name=SHARE_NAME,
            target_fqn=target_history_fqn,
            object_type="lfc_table",
        )

    def _target_view_exists(canonical_fqn: str) -> bool:
        """True iff the canonical table/view exists on the target workspace."""
        dotted = canonical_fqn.replace("`", "").replace("`.`", ".")
        try:
            auth.target_client.tables.get(dotted)
            return True
        except Exception:  # noqa: BLE001
            return False

    def _create_pipeline(spec: dict) -> None:
        """Create the recreated LFC pipeline on the TARGET workspace.

        # LIVE-VALIDATION: confirm pipelines.create accepts this spec shape
        # against the installed SDK (plan Task 8). If the typed API rejects
        # a raw dict, use CreatePipeline.from_dict(spec) or equivalent.
        """
        try:
            auth.target_client.pipelines.create(**spec)
        except TypeError:
            # Fallback: SDK may require a typed object
            from databricks.sdk.service.pipelines import CreatePipeline  # type: ignore[attr-defined]
            auth.target_client.pipelines.create(CreatePipeline.from_dict(spec))

    def _create_view(sql: str) -> None:
        """Execute a CREATE OR REPLACE VIEW DDL on the TARGET warehouse."""
        res = execute_and_poll(auth, tgt_wh_id, sql, use_source=False)
        if res["state"] != "SUCCEEDED":
            raise RuntimeError(f"create_view failed: {res.get('error', res['state'])}\nSQL: {sql}")

    deps = types.SimpleNamespace(
        compute_boundary=_compute_boundary,
        clone_history=_clone_history,
        target_view_exists=_target_view_exists,
        create_pipeline=_create_pipeline,
        create_view=_create_view,
    )

    # Read target_connection_name from config
    target_connection_name: str = getattr(config, "lfc_target_connection_name", "") or ""
    if not target_connection_name:
        raise ValueError(
            "config.lfc_target_connection_name is required for lfc_worker. "
            "Set it in migration_config.json."
        )

    # ---------------------------------------------------------------------------
    # Process each pipeline row with per-row isolation
    # ---------------------------------------------------------------------------
    all_results: list[dict] = []
    for row in rows:
        try:
            row_results = migrate_pipeline(row, deps=deps, target_connection_name=target_connection_name)
            all_results.extend(row_results)
        except Exception as exc:  # noqa: BLE001
            logger.error("[lfc_worker] pipeline %s failed: %s", row.get("object_name"), exc, exc_info=True)
            all_results.append({
                "object_name": row.get("object_name", "unknown"),
                "object_type": "lfc_pipeline",
                "status": "failed",
                "error_message": str(exc),
            })

    if all_results:
        tracker.append_migration_status(all_results)
    for r in all_results:
        logger.info("  %s [%s]: %s", r["object_name"], r["object_type"], r["status"])


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
