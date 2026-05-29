# modules/snowflake-foundation/outputs.tf
#
# Exports Snowflake resource identifiers consumed by:
#   - Root module outputs (terraform output → CI/CD env vars)
#   - dbt profiles.yml (database, schema, warehouse)
#   - Airflow Snowflake connection configuration
#   - azure-infra module (Databricks job parameters)

output "database_name" {
  description = "Fully-resolved name of the Snowflake database (includes environment suffix)."
  value       = snowflake_database.nyc311.name
}

output "schema_names" {
  description = "Map of medallion layer to Snowflake schema name."
  value = {
    bronze = snowflake_schema.bronze.name
    silver = snowflake_schema.silver.name
    gold   = snowflake_schema.gold.name
  }
}

output "warehouse_name" {
  description = "Name of the Snowflake virtual warehouse used by dbt and BI tooling."
  value       = snowflake_warehouse.nyc311.name
}

output "role_names" {
  description = "Map of logical role function to Snowflake role name."
  value = {
    admin       = snowflake_role.admin.name
    loader      = snowflake_role.loader.name
    transformer = snowflake_role.transformer.name
    reporter    = snowflake_role.reporter.name
  }
}
