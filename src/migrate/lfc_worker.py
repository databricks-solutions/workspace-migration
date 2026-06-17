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
    cloned_ok: dict[str, bool] = {}  # source_table -> history clone landed on target?
    start = time.time()

    for c in cfgs:
        if not c.get("cursor_column"):
            continue  # batch table: no clone, recreated pipeline full-loads it
        src_table_fqn = f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}"
        boundaries[c["source_table"]] = deps.compute_boundary(src_table_fqn, c["cursor_column"])
        clone = deps.clone_history(c)
        results.append({"object_name": f"{src_table_fqn}_history", "object_type": "lfc_table",
                        "status": clone["status"], "error_message": clone.get("error_message")})
        # history exists on target iff the clone validated (or was already there)
        cloned_ok[c["source_table"]] = clone["status"] in ("validated", "skipped_target_exists")

    spec = build_query_based_create_spec(
        definition, target_connection_name=target_connection_name,
        boundaries=boundaries, name=f"{obj_name}_migrated",
    )
    pipeline_id = deps.create_pipeline(spec)
    results.append({"object_name": obj_name, "object_type": "lfc_pipeline",
                    "status": "lfc_pipeline_created_incremental", "error_message": None,
                    "duration_seconds": time.time() - start})

    # Trigger the recreated pipeline's first (incremental) run so <t>_incr is
    # materialised, then build the unified view below. The first run is cheap —
    # row_filter scopes it to post-cutover rows, NOT the cloned history. Bounded
    # wait with graceful fallback: if <t>_incr doesn't appear in time (e.g. the
    # target can't reach the source, or its update fails), the view loop defers it.
    incr_targets = [_bt(f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}_incr")
                    for c in cfgs if c.get("cursor_column") and cloned_ok.get(c["source_table"])]
    if pipeline_id and incr_targets:
        deps.run_pipeline_and_await(pipeline_id, incr_targets)

    for c in cfgs:
        if not c.get("cursor_column"):
            continue
        if not cloned_ok.get(c["source_table"]):
            continue  # history clone failed — can't union a missing _history table; clone row already records the error
        base = f"{c['destination_catalog']}.{c['destination_schema']}.{c['destination_table']}"
        # The unified view unions <base>_history (cloned) + <base>_incr (produced by the
        # recreated pipeline's forward ingestion). If the pipeline hasn't run yet — e.g. it
        # can't reach the source without target-side network egress — <base>_incr won't
        # exist; defer the view rather than fail. It's created once forward ingestion runs.
        if not deps.target_table_exists(_bt(f"{base}_incr")):
            results.append({"object_name": base, "object_type": "lfc_view",
                            "status": "lfc_view_pending_forward_ingest",
                            "error_message": f"{base}_incr not present — recreated pipeline forward ingestion has "
                                             "not run yet; unified view deferred"})
            continue
        sql = build_unified_view_sql(
            canonical=_bt(base), history=_bt(f"{base}_history"), incr=_bt(f"{base}_incr"),
            scd_type=c["scd_type"], primary_keys=c["primary_keys"], cursor_column=c["cursor_column"],
        )
        # A view failure must NOT fail the (already-created) pipeline/table: the
        # recreated pipeline + cloned history are durable migration artefacts.
        # Defer the view (it can be built once forward ingestion has run/refreshed)
        # rather than propagate and discard the good rows.
        try:
            deps.create_view(sql)
        except Exception as exc:  # noqa: BLE001
            results.append({"object_name": base, "object_type": "lfc_view",
                            "status": "lfc_view_pending_forward_ingest",
                            "error_message": f"unified view deferred — create failed: {exc}"})
            continue
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

    from databricks.sdk.service.sharing import PermissionsChange, Privilege

    from common.auth import AuthManager
    from common.catalog_utils import CatalogExplorer
    from common.config import MigrationConfig
    from common.sql_utils import execute_and_fetch, execute_and_poll, find_warehouse
    from common.tracking import TrackingManager
    from common.validation import Validator
    from migrate.clone_lib import clone_table
    from migrate.reconciliation import resolve_current_job_run_id
    from migrate.rls_cm import make_staging_table_fqn
    from migrate.sharing_lib import (
        SHARE_NAME,
        add_tables_to_share,
        ensure_share_consumer_catalog,
        ensure_target_catalogs_and_schemas,
        get_or_create_recipient,
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

    def _count_rows(wh_id: str, fqn_bt: str, *, use_source: bool) -> int | None:
        """COUNT(*) over a backticked FQN on the given warehouse. Returns None on error."""
        res = execute_and_fetch(auth, wh_id, f"SELECT COUNT(*) FROM {fqn_bt}", use_source=use_source)
        if res["state"] != "SUCCEEDED":
            return None
        rd = res.get("rows") or []
        return int(rd[0][0]) if rd and rd[0] and rd[0][0] is not None else None

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
        tracker.record_staging_created(original_fqn=original_fqn, staging_fqn=staging_fqn, run_id=run_id)

        # Step 3: ensure share infrastructure
        # ensure_target_catalogs_and_schemas expects list[dict] with catalog_name / schema_name keys
        target_tbl_list = [
            {"object_name": target_history_fqn, "catalog_name": dest_cat, "schema_name": dest_sch}
        ]
        get_or_create_share(auth, SHARE_NAME)
        # Establish the cross-metastore D2D recipient on source keyed to the TARGET
        # metastore — this is what creates the target-side provider that
        # ensure_share_consumer_catalog needs. Mirrors setup_sharing.run().
        target_metastore_id = auth.target_client.metastores.summary().global_metastore_id
        recipient_name = get_or_create_recipient(auth, target_metastore_id)
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
        # GRANT SELECT on the share to the recipient — WITHOUT this the recipient
        # sees an empty share and the consumer catalog exposes no tables, so the
        # DEEP CLONE fails with SCHEMA/TABLE_NOT_FOUND. Mirrors setup_sharing.run()
        # step 6. Idempotent (share-level; covers tables added later this run).
        auth.source_client.shares.update_permissions(
            name=SHARE_NAME,
            changes=[PermissionsChange(principal=recipient_name, add=[Privilege.SELECT.value])],
        )
        ensure_target_catalogs_and_schemas(auth, target_tbl_list)
        ensure_share_consumer_catalog(auth, SHARE_NAME, dry_run=False)

        # Step 4: DEEP CLONE the staging copy onto target as <table>_history, with a
        # retry on cross-region (NE->WE) Delta-Sharing propagation lag — the staging
        # table can be invisible in the consumer catalog for a short window after
        # ALTER SHARE ADD (see reference_wsm_target_copy_share_race). Refresh the
        # consumer catalog + retry on NOT_FOUND-shaped errors only.
        last_res: dict = {}
        for attempt in range(6):
            last_res = clone_table(
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
            st = last_res["status"]
            if st in ("validated", "skipped_target_exists"):
                return last_res
            if st == "validation_failed":
                # The DEEP CLONE DDL succeeded but clone_table's validate_row_count
                # counts the TARGET table on the SOURCE spark session (known H10 flaw)
                # and can't find it. Re-validate authoritatively via the warehouses:
                # source staging count vs target _history count.
                src_cnt = _count_rows(src_wh_id, staging_fqn, use_source=True)
                tgt_cnt = _count_rows(tgt_wh_id, target_history_fqn, use_source=False)
                if src_cnt is not None and src_cnt == tgt_cnt:
                    return {"object_name": last_res.get("object_name", staging_fqn),
                            "object_type": "lfc_table", "status": "validated", "error_message": None,
                            "source_row_count": src_cnt, "target_row_count": tgt_cnt}
                return {"object_name": last_res.get("object_name", staging_fqn),
                        "object_type": "lfc_table", "status": "validation_failed",
                        "error_message": f"warehouse row-count check: source={src_cnt} target={tgt_cnt}"}
            # st == "failed": retry ONLY on a consumer-side NOT_FOUND (share propagation lag)
            err = (last_res.get("error_message") or "").upper()
            if "NOT_FOUND" not in err and "CANNOT BE FOUND" not in err:
                return last_res  # genuine clone failure — surface immediately
            logger.info(
                "[lfc] staging not visible in consumer catalog yet (attempt %d/6) — refreshing share + retrying",
                attempt + 1,
            )
            time.sleep(15)
            ensure_share_consumer_catalog(auth, SHARE_NAME, dry_run=False)
        return last_res

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

        Live-validated shape (databricks-sdk 0.102): pipelines.create takes the
        ingestion_definition as a typed IngestionPipelineDefinition, plus the
        REQUIRED top-level catalog/schema (direct publishing mode). There is no
        CreatePipeline class in this SDK.
        """
        from databricks.sdk.service.pipelines import IngestionPipelineDefinition

        created = auth.target_client.pipelines.create(
            name=spec["name"],
            catalog=spec.get("catalog"),
            schema=spec.get("schema"),
            channel=spec.get("channel", "PREVIEW"),
            ingestion_definition=IngestionPipelineDefinition.from_dict(spec["ingestion_definition"]),
        )
        return created.pipeline_id

    def _run_pipeline_and_await(pipeline_id: str, incr_fqns: list[str],
                                *, max_attempts: int = 60, sleep_seconds: float = 10.0) -> None:
        """Trigger the recreated pipeline's first run and wait (bounded) for the
        UPDATE to COMPLETE. The first run is cheap (row_filter scopes it to
        post-cutover rows). We wait for COMPLETED rather than mere <t>_incr
        existence because LFC <t>_incr is a STREAMING TABLE: the table object
        appears as soon as the graph is resolved, but is NOT queryable until the
        update finishes its first refresh (else SELECT raises
        STREAMING_TABLE_NEEDS_REFRESH). Returns when the update COMPLETES, when it
        FAILS/CANCELS, or after the budget — never raises; the caller's view loop
        defers the view if <t>_incr still isn't queryable."""
        try:
            auth.target_client.pipelines.start_update(pipeline_id)
        except Exception as exc:  # noqa: BLE001 — surface via deferral, don't abort the pipeline row
            logger.warning("[lfc] start_update(%s) failed: %s — view will be deferred", pipeline_id, exc)
            return
        for _ in range(max_attempts):
            time.sleep(sleep_seconds)
            try:
                ups = auth.target_client.pipelines.get(pipeline_id).latest_updates or []
                state = str(ups[0].state).upper() if ups else ""
            except Exception:  # noqa: BLE001 — transient; keep polling
                continue
            if "COMPLETED" in state:
                return
            if "FAILED" in state or "CANCELED" in state or "CANCELLED" in state:
                logger.warning("[lfc] recreated pipeline %s update %s — view will be deferred", pipeline_id, state)
                return

    def _create_view(sql: str) -> None:
        """Execute a CREATE OR REPLACE VIEW DDL on the TARGET warehouse."""
        res = execute_and_poll(auth, tgt_wh_id, sql, use_source=False)
        if res["state"] != "SUCCEEDED":
            raise RuntimeError(f"create_view failed: {res.get('error', res['state'])}\nSQL: {sql}")

    deps = types.SimpleNamespace(
        compute_boundary=_compute_boundary,
        clone_history=_clone_history,
        target_view_exists=_target_view_exists,
        target_table_exists=_target_view_exists,  # same impl (tables.get) — checks <t>_incr presence
        create_pipeline=_create_pipeline,
        run_pipeline_and_await=_run_pipeline_and_await,
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
