# Resource names for dbt profiles, CI configuration and post-apply checks.

output "snowflake_database_name" {
  description = "Name of the provisioned Snowflake database (includes environment suffix for non-prod)"
  value       = module.snowflake_foundation.database_name
}

output "snowflake_warehouse_name" {
  description = "Name of the Snowflake virtual warehouse used by dbt and BI tooling"
  value       = module.snowflake_foundation.warehouse_name
}

output "snowflake_transformer_role" {
  description = "Snowflake TRANSFORMER role name — assigned to the dbt service user for BRONZE read and SILVER/GOLD write"
  value       = module.snowflake_foundation.role_names["transformer"]
}

output "snowflake_reporter_role" {
  description = "Snowflake REPORTER role name — assigned to BI tool service accounts for GOLD read-only access"
  value       = module.snowflake_foundation.role_names["reporter"]
}

output "snowflake_loader_role" {
  description = "Snowflake LOADER role name — assigned to the ingestion service account for BRONZE write"
  value       = module.snowflake_foundation.role_names["loader"]
}
