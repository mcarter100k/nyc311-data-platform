# modules/azure-infra/outputs.tf
# Purpose: Exports Azure resource identifiers consumed by the root module and the Databricks provider.

output "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account"
  value       = "" # stub — set to azurerm_storage_account.this.name after implementation
}

output "bronze_container_url" {
  description = "Full URL of the Bronze ADLS container (abfss:// scheme)"
  value       = "" # stub
}

output "silver_container_url" {
  description = "Full URL of the Silver ADLS container (abfss:// scheme)"
  value       = "" # stub
}

output "databricks_workspace_url" {
  description = "Hostname URL of the Databricks workspace (used by Databricks provider)"
  value       = "" # stub — set to azurerm_databricks_workspace.this.workspace_url
}

output "databricks_workspace_id" {
  description = "Azure resource ID of the Databricks workspace"
  value       = "" # stub
}
