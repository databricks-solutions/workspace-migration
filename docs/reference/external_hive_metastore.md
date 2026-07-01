# External Hive Metastore Migration Guide

## Context

Some customers run an **external Hive metastore** — typically a MySQL or Azure SQL database — and point their Databricks clusters at it via cluster init scripts or Spark configuration. In that setup the metastore database lives outside Databricks entirely, so no data migration is required: the new workspace can be configured to point at the **same** external metastore and will see all existing databases and tables.

This document describes what the customer needs to reconfigure on the target workspace. The tool does not migrate external Hive metastore configuration for you — it only **discovers** which clusters, jobs, and init scripts reference an external metastore so the customer has a checklist.

## Discovery report

Run the `discovery` workflow with `scope.include_hive: true` after deploying the migration tool. A new section in the migration dashboard ("External Hive Metastore" page — not yet wired into the Lakeview JSON; query the tracking tables directly for now) lists:

- Clusters whose `spark_conf` contains `javax.jdo.option.ConnectionURL` (or the hadoop-prefixed variant `spark.hadoop.javax.jdo.option.ConnectionURL`)
- Clusters referencing init scripts that set up external metastore connectivity
- Jobs using any of the affected clusters (either `existing_cluster_id` or a `new_cluster.spark_conf` with the same keys)
- Init scripts (workspace-level global and cluster-level) that contain metastore-related keywords
- Secret-scope references in the configs the customer must re-provision

All of these are surfaced in the `migration_tracking.cp_migration.discovery_inventory` table with `object_type = 'external_hive_metastore_ref'` during Phase 2 discovery.

## Manual steps on target

1. **Create the metastore credentials on target**
   - Put the metastore database user/password in a Databricks secret scope on the target workspace.
   - Scope name should match the source if the customer wants to re-use existing init scripts unchanged.

2. **Configure clusters**
   - For each cluster identified in discovery, create an equivalent cluster on target with the same `spark_conf`:
     ```
     spark.hadoop.javax.jdo.option.ConnectionURL  jdbc:mysql://<host>:3306/hive?<opts>
     spark.hadoop.javax.jdo.option.ConnectionDriverName  com.mysql.cj.jdbc.Driver
     spark.hadoop.javax.jdo.option.ConnectionUserName  {{secrets/<scope>/<user-key>}}
     spark.hadoop.javax.jdo.option.ConnectionPassword  {{secrets/<scope>/<password-key>}}
     spark.sql.hive.metastore.version  <version>
     spark.sql.hive.metastore.jars  builtin
     ```
   - Use the **same init scripts** if they configured the metastore; upload them to the target workspace first.

3. **Configure SQL warehouses**
   - Warehouse-level external metastore is uncommon but supported via workspace admin config. Replicate the relevant fields under the warehouse settings.

4. **Configure network connectivity**
   - The metastore database must be reachable from the target workspace's data-plane subnets. Typical requirements:
     - NSG outbound rule allowing port 3306 (MySQL) or 1433 (Azure SQL) to the metastore host
     - Private endpoint + private DNS if the metastore is behind Private Link
     - VNet peering if the metastore is in a different VNet
   - The source workspace's networking setup is a good reference; replicate the pieces that touch the metastore.

5. **Validate**
   - Start a cluster with the new config on target.
   - Run `SHOW DATABASES` — should return the same list as source.
   - Query a known table — e.g. `SELECT COUNT(*) FROM <db>.<table>` — to confirm both metastore connectivity and data-path access.

## Common pitfalls

- **Password rotation:** if the source workspace embedded the metastore password in an init script rather than a secret scope, the customer must move it to a scope on target. Hardcoded credentials in workspace files don't survive migration.
- **Driver JAR location:** if the source used a custom MySQL driver JAR stored in DBFS, copy that JAR to target's DBFS (or an ADLS volume) and update the library reference.
- **Metastore version:** the Databricks runtime must be compatible with the metastore schema version. If the source was on an older DBR, upgrading DBR on target may require a metastore schema upgrade.

## What this tool does NOT do

- Rewrite init scripts to swap old workspace URLs for new ones (some scripts embed those; customer must scan and update).
- Migrate the metastore database itself (it stays where it is).
- Provision the network path (customer-owned Azure config).
- Re-provision the metastore DB credentials (secrets don't export).

For everything else, discovery gives you the list; this document gives you the playbook.
