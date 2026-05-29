# outputs.tf
# Purpose: Exposes key resource identifiers from child modules for use in CI pipelines,
# downstream config, and manual verification after apply.
#
# NOTE: The azure_infra module is currently commented out in main.tf while the ingestion
# layer is under development. The three ADLS/Databricks outputs below are preserved as
# comments so they activate in sync with the module when it is re-enabled. Re-enable by
# uncommenting both the module block in main.tf and the outputs below.

# output "adls_storage_account_name" {
#   description = "Name of the Azure Data Lake Storage Gen2 account"
#   value       = module.azure_infra.storage_account_name
# }

# output "adls_bronze_container_url" {
#   description = "URL of the Bronze container in ADLS Gen2"
#   value       = module.azure_infra.bronze_container_url
# }

# output "databricks_workspace_url" {
#   description = "URL of the Databricks workspace"
#   value       = module.azure_infra.databricks_workspace_url
# }

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
  description = "Snowflake LOADER role name — assigned to the Databricks ingestion service account for BRONZE write"
  value       = module.snowflake_foundation.role_names["loader"]
}
