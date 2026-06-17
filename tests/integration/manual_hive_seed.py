# Databricks notebook source
# MANUAL Hive seed + storage-key setup — for interactive debugging of the
# hive_metastore-on-ADLS path on a classic cluster.
#
# HOW TO USE:
#   1. Create a classic cluster (data_security_mode NONE / "No Isolation
#      Shared", single node is fine) on the SOURCE workspace.
#   2. Attach this notebook and Run All. Each cell prints what it did so you
#      can see exactly where (if anywhere) abfss access stalls.
#
# It (a) sets the ADLS account key on the running session, (b) probes abfss
# reachability, then (c) creates the same hive fixtures the integration seed
# does — a DBFS-root managed table plus the two ADLS tables (external +
# managed-non-DBFS) that the migrate_hive external/nondbfs workers consume.

# COMMAND ----------
# --- Parameters (edit if your lab differs) ---
STORAGE_ACCOUNT = "stextsourcemig36cd38"            # ADLS Gen2 account
CONTAINER       = "external-data"                   # external location container
BASE_PATH       = "hive_dbfs_migration"             # subpath under the container
SECRET_SCOPE    = "migration"
SECRET_KEY      = "adls-account-key"
DB             = "hive_metastore.integration_test_hive"

_host = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
_base = f"abfss://{CONTAINER}@{_host}/{BASE_PATH}"
print("storage host :", _host)
print("abfss base   :", _base)

# COMMAND ----------
# --- 1. Set the ADLS account key on this session ---
# On a classic (No Isolation) cluster this is honored by the ABFS driver.
# (On UC access-mode / serverless compute it is ignored — that's the whole
# reason the migrate_hive ADLS workers need a classic cluster.)
_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)  # noqa: F821
spark.conf.set(f"fs.azure.account.key.{_host}", _key)  # noqa: F821
print(f"Set fs.azure.account.key.{_host} (len={len(_key)})")

# COMMAND ----------
# --- 2. Probe abfss reachability (fast, so you see a clear result) ---
# Raw TCP first (interruptible), then an actual abfss listing.
import socket

try:
    _c = socket.create_connection((_host, 443), timeout=20)
    _c.close()
    print(f"TCP {_host}:443  -> OK")
except Exception as e:
    print(f"TCP {_host}:443  -> FAIL: {type(e).__name__}: {e}")

try:
    print("abfss ls       ->", dbutils.fs.ls(f"abfss://{CONTAINER}@{_host}/"))  # noqa: F821
    print("abfss ls       -> OK")
except Exception as e:
    print(f"abfss ls       -> FAIL/HANG-IF-NO-RETURN: {type(e).__name__}: {e}")

# COMMAND ----------
# --- 3. Clean any stale fixtures (managed-table files orphaned by prior runs) ---
spark.sql(f"DROP DATABASE IF EXISTS {DB} CASCADE")  # noqa: F821
try:
    dbutils.fs.rm("dbfs:/user/hive/warehouse/integration_test_hive.db", True)  # noqa: F821
except Exception as e:
    print("dbfs rm skipped:", e)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")  # noqa: F821
print("database ready:", DB)

# COMMAND ----------
# --- 4a. DBFS-root managed table (no explicit LOCATION) ---
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DB}.managed_orders (order_id INT, amount DOUBLE) USING DELTA
""")  # noqa: F821
spark.sql(f"INSERT OVERWRITE TABLE {DB}.managed_orders VALUES (1, 10.0), (2, 20.0)")  # noqa: F821
print("managed_orders rows:", spark.table(f"{DB}.managed_orders").count())  # noqa: F821

# COMMAND ----------
# --- 4b. EXTERNAL table on ADLS (this is the abfss write that hangs in the job) ---
_ext_loc = f"{_base}/hive_external_invoices"
print("creating external_invoices at", _ext_loc)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DB}.external_invoices (invoice_id INT, amount DOUBLE)
    USING DELTA LOCATION '{_ext_loc}'
""")  # noqa: F821
spark.sql(f"INSERT OVERWRITE TABLE {DB}.external_invoices VALUES (101, 100.0), (102, 200.0)")  # noqa: F821
print("external_invoices rows:", spark.table(f"{DB}.external_invoices").count())  # noqa: F821

# COMMAND ----------
# --- 4c. MANAGED non-DBFS table on ADLS (explicit LOCATION off DBFS root) ---
_nd_loc = f"{_base}/hive_nondbfs_sales"
print("creating nondbfs_sales at", _nd_loc)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DB}.nondbfs_sales (sale_id INT, amount DOUBLE)
    USING DELTA LOCATION '{_nd_loc}'
""")  # noqa: F821
spark.sql(f"INSERT OVERWRITE TABLE {DB}.nondbfs_sales VALUES (201, 50.0), (202, 75.0), (203, 90.0)")  # noqa: F821
print("nondbfs_sales rows:", spark.table(f"{DB}.nondbfs_sales").count())  # noqa: F821

# COMMAND ----------
print("DONE — fixtures created:")
display(spark.sql(f"SHOW TABLES IN {DB}"))  # noqa: F821
