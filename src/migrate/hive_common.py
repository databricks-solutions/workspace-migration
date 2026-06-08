"""Shared helpers for Hive-to-UC migration workers (Phase 2)."""

from __future__ import annotations

import logging
import re

HIVE_CATALOG = "hive_metastore"

logger = logging.getLogger("hive_common")

# Secret holding the ADLS storage account key for hive_metastore tables on
# ADLS. hive_metastore LOCATION access does NOT route through UC credential
# vending — it uses the legacy fs.azure.account.key Hadoop conf, which can only
# be set on CLASSIC compute (serverless rejects it). Workers/seed that touch
# ADLS-backed hive_metastore tables must therefore run on a classic cluster
# (job_cluster_key=hive_adls_classic) and call configure_adls_account_key().
ADLS_KEY_SECRET_SCOPE = "migration"
ADLS_KEY_SECRET_KEY = "adls-account-key"


def configure_adls_account_key(
    spark,
    dbutils,
    storage_location: str | None,
    *,
    scope: str = ADLS_KEY_SECRET_SCOPE,
    key: str = ADLS_KEY_SECRET_KEY,
) -> bool:
    """Set fs.azure.account.key for the storage account in *storage_location*.

    hive_metastore EXTERNAL / managed-non-DBFS tables live on ADLS, and that
    access bypasses UC credential vending — it needs the legacy account-key
    Hadoop conf, settable only on classic compute. Parses the account host from
    an ``abfss://container@<account>.dfs.core.windows.net/...`` location and
    sets the per-session key from the secret. Best-effort and idempotent:

    * no-op for non-abfss locations,
    * no-op if the secret isn't configured (returns False — caller stays on UC
      vending / serverless behaviour),
    * no-op if the runtime rejects the conf (serverless), logged not raised.

    Returns True only when the key was successfully applied.
    """
    if not storage_location or not storage_location.startswith("abfss://"):
        return False
    try:
        host = storage_location.split("@", 1)[1].split("/", 1)[0]  # <account>.dfs.core.windows.net
    except (IndexError, AttributeError):
        return False
    if not host:
        return False
    try:
        secret = dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:  # noqa: BLE001 — secret not configured
        logger.info("ADLS account key secret %s/%s unavailable (%s); skipping.", scope, key, exc)
        return False
    try:
        spark.conf.set(f"fs.azure.account.key.{host}", secret)
        logger.info("Configured fs.azure.account.key for %s.", host)
        return True
    except Exception as exc:  # noqa: BLE001 — serverless blocks this conf
        logger.warning("Could not set fs.azure.account.key for %s (%s).", host, exc)
        return False


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
    "OWN": "OWNER",  # Ownership is transferred via ALTER ... OWNER TO (hive_grants_worker), not granted.
    "ALL PRIVILEGES": "ALL PRIVILEGES",
}
