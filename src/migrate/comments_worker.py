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
    return str(value).replace("'", "''")


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
    results: list[dict] = []

    cat_rows = spark.sql(
        f"SELECT DISTINCT catalog_name "
        f"FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE source_type = 'uc' AND catalog_name IS NOT NULL"
    ).collect()

    for row in cat_rows:
        with _SuppressLog(results, row.catalog_name, "CATALOG"):
            comment_rows = spark.sql(
                f"SELECT comment FROM system.information_schema.catalogs WHERE catalog_name = '{row.catalog_name}'"
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

    sch_rows = spark.sql(
        f"SELECT DISTINCT catalog_name, schema_name "
        f"FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE source_type = 'uc' AND catalog_name IS NOT NULL AND schema_name IS NOT NULL"
    ).collect()

    for row in sch_rows:
        with _SuppressLog(results, f"{row.catalog_name}.{row.schema_name}", "SCHEMA"):
            comment_rows = spark.sql(
                f"SELECT comment FROM `{row.catalog_name}`.information_schema.schemata "
                f"WHERE schema_name = '{row.schema_name}'"
            ).collect()
            if comment_rows and comment_rows[0].comment:
                results.append(
                    _emit_comment(
                        "SCHEMA",
                        f"`{row.catalog_name}`.`{row.schema_name}`",
                        comment_rows[0].comment,
                        auth=auth,
                        wh_id=wh_id,
                        dry_run=config.dry_run,
                    )
                )

    # Non-Delta tables — TBLPROPERTIES + COMMENT ON TABLE
    non_delta = spark.sql(
        f"SELECT object_name, format FROM "
        f"{config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE source_type = 'uc' AND object_type IN ('external_table','managed_table') "
        f"AND (format IS NULL OR lower(format) <> 'delta')"
    ).collect()
    for row in non_delta:
        with _SuppressLog(results, row.object_name, "TABLE"):
            # COMMENT ON TABLE is replayed from DESCRIBE TABLE EXTENDED;
            # information_schema.tables.comment covers this too.
            parts = row.object_name.strip("`").split("`.`")
            if len(parts) == 3:
                catalog, schema, name = parts
                tbl_meta = spark.sql(
                    f"SELECT comment FROM `{catalog}`.information_schema.tables "
                    f"WHERE table_schema='{schema}' AND table_name='{name}'"
                ).collect()
                if tbl_meta and tbl_meta[0].comment:
                    results.append(
                        _emit_comment(
                            "TABLE",
                            row.object_name,
                            tbl_meta[0].comment,
                            auth=auth,
                            wh_id=wh_id,
                            dry_run=config.dry_run,
                        )
                    )

    # Column comments — Delta DEEP CLONE preserves column metadata for
    # managed Delta tables, but external tables and non-Delta tables need
    # explicit replay. We iterate every UC table we discovered and replay
    # any column with a non-null comment. Delta managed-table columns will
    # usually already carry the comment on target; re-issuing the ALTER
    # TABLE is idempotent.
    all_tables = spark.sql(
        f"SELECT object_name FROM "
        f"{config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE source_type = 'uc' AND object_type IN ('external_table','managed_table')"
    ).collect()
    for row in all_tables:
        parts = row.object_name.strip("`").split("`.`")
        if len(parts) != 3:
            continue
        catalog, schema, name = parts
        with _SuppressLog(results, row.object_name, "COLUMN"):
            col_meta = spark.sql(
                f"SELECT column_name, comment FROM `{catalog}`.information_schema.columns "
                f"WHERE table_schema='{schema}' AND table_name='{name}' AND comment IS NOT NULL"
            ).collect()
            for c in col_meta:
                if c.comment:
                    results.append(
                        _emit_comment(
                            "COLUMN",
                            row.object_name,
                            c.comment,
                            auth=auth,
                            wh_id=wh_id,
                            dry_run=config.dry_run,
                            column_name=c.column_name,
                        )
                    )

    # Volume comments — the volume_worker creates the target volume shell
    # but does not copy the ``COMMENT`` clause, so replay here via
    # information_schema.volumes.
    vol_rows = spark.sql(
        f"SELECT object_name FROM "
        f"{config.tracking_catalog}.{config.tracking_schema}.discovery_inventory "
        f"WHERE source_type = 'uc' AND object_type = 'volume'"
    ).collect()
    for row in vol_rows:
        parts = row.object_name.strip("`").split("`.`")
        if len(parts) != 3:
            continue
        catalog, schema, name = parts
        with _SuppressLog(results, row.object_name, "VOLUME"):
            vol_meta = spark.sql(
                f"SELECT comment FROM `{catalog}`.information_schema.volumes "
                f"WHERE volume_schema='{schema}' AND volume_name='{name}'"
            ).collect()
            if vol_meta and vol_meta[0].comment:
                results.append(
                    _emit_comment(
                        "VOLUME",
                        row.object_name,
                        vol_meta[0].comment,
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
