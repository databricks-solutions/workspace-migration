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
# Comments & Table Properties Worker (Phase 3 Task 32).
#
# Delta DEEP CLONE preserves comments + TBLPROPERTIES automatically, so this
# worker runs only for non-Delta managed tables and all external tables. It
# also sets COMMENT ON CATALOG / COMMENT ON SCHEMA which DEEP CLONE doesn't
# reach.
#
# Reads directly from discovery_inventory (no new object_type needed for
# comments themselves — we re-read from source's information_schema at
# migrate time to pick up any updates between discovery and migrate).

import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("comments_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def _escape(value: str) -> str:
    """Escape a comment string for inline SQL.

    Backslash-double MUST happen before quote-double, otherwise the escape-
    doubled `\\'` gets re-broken. Semicolons are dropped because they would
    terminate the COMMENT ON ... IS '...' statement mid-string.
    """
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("'", "''")
        .replace(";", "")
    )


def _emit_comment(
    securable_type: str,
    fqn: str,
    comment: str,
    *,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
    column_name: str | None = None,
) -> dict:
    # Column comments need ALTER TABLE ... ALTER COLUMN ... COMMENT '...';
    # COMMENT ON COLUMN is not supported by Databricks SQL. Table/Schema/
    # Catalog/Volume all take the generic COMMENT ON <type> fqn IS '...' form.
    if securable_type == "COLUMN":
        if not column_name:
            raise ValueError("column_name is required for COLUMN comments")
        sql = f"ALTER TABLE {fqn} ALTER COLUMN `{column_name}` COMMENT '{_escape(comment)}'"
        obj_key = f"COMMENT_COLUMN_{fqn}.{column_name}"
    else:
        sql = f"COMMENT ON {securable_type} {fqn} IS '{_escape(comment)}'"
        obj_key = f"COMMENT_{securable_type}_{fqn}"
    start = time.time()
    if dry_run:
        logger.info("[DRY RUN] %s", sql)
        return {
            "object_name": obj_key,
            "object_type": "comment",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }
    result = execute_and_poll(auth, wh_id, sql)
    duration = time.time() - start
    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_key,
            "object_type": "comment",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }
    return {
        "object_name": obj_key,
        "object_type": "comment",
        "status": "validated",
        "error_message": None,
        "duration_seconds": duration,
    }


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    wh_id = find_warehouse(auth)

    # Re-read from source information_schema at migrate time.
    # Comments on catalogs + schemas: always.
    # Comments on non-Delta tables: only those need explicit replay.
    #
    # Batches information_schema queries by (catalog, schema) instead of
    # per-object — at 100k tables this is ~100x fewer queries (one per
    # schema for each info_schema view, vs one per table). See review M7.
    results: list[dict] = []
    inv_fqn = f"{config.tracking_catalog}.{config.tracking_schema}.discovery_inventory"

    # ---- Catalog comments (one query per catalog; N is small) ----
    cat_rows = spark.sql(
        f"SELECT DISTINCT catalog_name FROM {inv_fqn} "
        f"WHERE source_type = 'uc' AND catalog_name IS NOT NULL"
    ).collect()

    for row in cat_rows:
        with _SuppressLog(results, row.catalog_name, "CATALOG"):
            comment_rows = spark.sql(
                f"SELECT comment FROM system.information_schema.catalogs "
                f"WHERE catalog_name = '{_escape(row.catalog_name)}'"
            ).collect()
            if comment_rows and comment_rows[0].comment:
                results.append(
                    _emit_comment(
                        "CATALOG",
                        f"`{row.catalog_name}`",
                        comment_rows[0].comment,
                        auth=auth,
                        wh_id=wh_id,
                        dry_run=config.dry_run,
                    )
                )

    # ---- Build per-(catalog, schema) lookups from discovery_inventory ----
    non_delta_set: set[tuple[str, str, str]] = set()
    for row in spark.sql(
        f"SELECT object_name, format FROM {inv_fqn} "
        f"WHERE source_type = 'uc' "
        f"AND object_type IN ('external_table','managed_table') "
        f"AND (format IS NULL OR lower(format) <> 'delta')"
    ).collect():
        parts = row.object_name.strip("`").split("`.`")
        if len(parts) == 3:
            non_delta_set.add((parts[0], parts[1], parts[2]))

    all_tables_set: set[tuple[str, str, str]] = set()
    for row in spark.sql(
        f"SELECT object_name FROM {inv_fqn} "
        f"WHERE source_type = 'uc' "
        f"AND object_type IN ('external_table','managed_table')"
    ).collect():
        parts = row.object_name.strip("`").split("`.`")
        if len(parts) == 3:
            all_tables_set.add((parts[0], parts[1], parts[2]))

    volume_set: set[tuple[str, str, str]] = set()
    for row in spark.sql(
        f"SELECT object_name FROM {inv_fqn} "
        f"WHERE source_type = 'uc' AND object_type = 'volume'"
    ).collect():
        parts = row.object_name.strip("`").split("`.`")
        if len(parts) == 3:
            volume_set.add((parts[0], parts[1], parts[2]))

    # ---- One info_schema query per (catalog, schema) for each comment kind ----
    sch_pairs = spark.sql(
        f"SELECT DISTINCT catalog_name, schema_name FROM {inv_fqn} "
        f"WHERE source_type = 'uc' "
        f"AND catalog_name IS NOT NULL AND schema_name IS NOT NULL"
    ).collect()

    for pair in sch_pairs:
        cat, sch = pair.catalog_name, pair.schema_name

        # 1) Schema comment
        with _SuppressLog(results, f"{cat}.{sch}", "SCHEMA"):
            sch_meta = spark.sql(
                f"SELECT comment FROM `{cat}`.information_schema.schemata "
                f"WHERE schema_name = '{_escape(sch)}'"
            ).collect()
            if sch_meta and sch_meta[0].comment:
                results.append(
                    _emit_comment(
                        "SCHEMA",
                        f"`{cat}`.`{sch}`",
                        sch_meta[0].comment,
                        auth=auth,
                        wh_id=wh_id,
                        dry_run=config.dry_run,
                    )
                )

        # 2) Table comments — non-Delta only (DEEP CLONE handles Delta).
        with _SuppressLog(results, f"{cat}.{sch}", "TABLES"):
            tbl_rows = spark.sql(
                f"SELECT table_name, comment FROM `{cat}`.information_schema.tables "
                f"WHERE table_schema = '{_escape(sch)}' AND comment IS NOT NULL"
            ).collect()
            for tr in tbl_rows:
                if (cat, sch, tr.table_name) in non_delta_set and tr.comment:
                    results.append(
                        _emit_comment(
                            "TABLE",
                            f"`{cat}`.`{sch}`.`{tr.table_name}`",
                            tr.comment,
                            auth=auth,
                            wh_id=wh_id,
                            dry_run=config.dry_run,
                        )
                    )

        # 3) Column comments — all UC tables (ALTER TABLE ALTER COLUMN
        #    is idempotent for Delta managed tables that already carry
        #    the comment from DEEP CLONE).
        with _SuppressLog(results, f"{cat}.{sch}", "COLUMNS"):
            col_rows = spark.sql(
                f"SELECT table_name, column_name, comment "
                f"FROM `{cat}`.information_schema.columns "
                f"WHERE table_schema = '{_escape(sch)}' AND comment IS NOT NULL"
            ).collect()
            for cr in col_rows:
                if (cat, sch, cr.table_name) in all_tables_set and cr.comment:
                    results.append(
                        _emit_comment(
                            "COLUMN",
                            f"`{cat}`.`{sch}`.`{cr.table_name}`",
                            cr.comment,
                            auth=auth,
                            wh_id=wh_id,
                            dry_run=config.dry_run,
                            column_name=cr.column_name,
                        )
                    )

        # 4) Volume comments
        with _SuppressLog(results, f"{cat}.{sch}", "VOLUMES"):
            vol_rows = spark.sql(
                f"SELECT volume_name, comment "
                f"FROM `{cat}`.information_schema.volumes "
                f"WHERE volume_schema = '{_escape(sch)}' AND comment IS NOT NULL"
            ).collect()
            for vr in vol_rows:
                if (cat, sch, vr.volume_name) in volume_set and vr.comment:
                    results.append(
                        _emit_comment(
                            "VOLUME",
                            f"`{cat}`.`{sch}`.`{vr.volume_name}`",
                            vr.comment,
                            auth=auth,
                            wh_id=wh_id,
                            dry_run=config.dry_run,
                        )
                    )

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Comments worker complete. %d validated, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


class _SuppressLog:
    """Record a failed comment replay as a tracking row instead of raising."""

    def __init__(self, results, obj_name, securable_type):
        self.results = results
        self.obj_name = obj_name
        self.securable_type = securable_type

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is None:
            return False
        self.results.append(
            {
                "object_name": f"COMMENT_{self.securable_type}_{self.obj_name}",
                "object_type": "comment",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        )
        return True  # swallow


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
