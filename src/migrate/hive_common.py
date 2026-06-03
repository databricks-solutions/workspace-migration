"""Shared helpers for Hive-to-UC migration workers (Phase 2)."""

from __future__ import annotations

import re

HIVE_CATALOG = "hive_metastore"


def rewrite_hive_namespace(sql: str, target_catalog: str) -> str:
    """Replace every ``hive_metastore.`` reference in a SQL string with
    ``<target_catalog>.``. Handles both backticked and un-backticked forms.
    """
    # `hive_metastore`. -> `target_catalog`.
    sql = re.sub(r"`hive_metastore`\.", f"`{target_catalog}`.", sql)
    # hive_metastore. (no backticks)
    sql = re.sub(r"\bhive_metastore\.", f"{target_catalog}.", sql)
    return sql


def rewrite_hive_fqn(fqn: str, target_catalog: str) -> str:
    """Rewrite a fully-qualified table name from hive_metastore.x.y to
    ``<target_catalog>.x.y``.  Accepts backticked or plain form.
    """
    stripped = fqn.strip("`").split("`.`")
    if len(stripped) == 3 and stripped[0] == HIVE_CATALOG:
        _, schema, name = stripped
        return f"`{target_catalog}`.`{schema}`.`{name}`"
    # Fallback — treat as dotted
    parts = fqn.split(".")
    if len(parts) == 3 and parts[0] == HIVE_CATALOG:
        return f"`{target_catalog}`.`{parts[1]}`.`{parts[2]}`"
    return fqn


def ensure_target_catalog_and_schema(spark, target_catalog: str, schema: str) -> None:
    """Idempotently create the UC target catalog and schema."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{target_catalog}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{schema}`")


# Hive → UC privilege mapping for hive_grants_worker.
# Keys are Hive legacy ACL action types; values are UC privilege strings.
# Anything not in this map is dropped with a warning.
HIVE_TO_UC_PRIVILEGES = {
    "SELECT": "SELECT",
    "MODIFY": "MODIFY",
    "READ_METADATA": "USE SCHEMA",
    "CREATE": "CREATE SCHEMA",
    "CREATE_NAMED_FUNCTION": "CREATE FUNCTION",
    "USAGE": "USE CATALOG",
    "OWN": "OWNER",  # Ownership is transferred, not granted; worker skips this case.
    "ALL PRIVILEGES": "ALL PRIVILEGES",
}
