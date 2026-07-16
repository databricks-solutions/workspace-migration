"""Shared helpers for Hive-to-UC migration workers (Phase 2)."""

from __future__ import annotations

import logging

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


def rewrite_hive_namespace(sql: str, target_catalog: str = HIVE_CATALOG) -> str:
    """Like-for-like migration replays DDL into hive_metastore unchanged, so
    this is now an identity function. Kept as a call-site seam (and for the
    ``target_catalog`` signature) while callers are migrated off the rewrite.
    """
    return sql


def rewrite_hive_fqn(fqn: str, target_catalog: str = HIVE_CATALOG) -> str:
    """Identity: the target FQN equals the source FQN (both in hive_metastore)."""
    return fqn


def ensure_target_database(spark, schema: str) -> None:
    """Idempotently create the target database in hive_metastore (like-for-like)."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{HIVE_CATALOG}`.`{schema}`")


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
