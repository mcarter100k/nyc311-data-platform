# terraform/variables.tf
# Last validated: 2026-06-05 (terraform fmt + validate pass clean)
#
# Root-level input variable declarations for the NYC 311 Data Platform.
#
# Sensitive values (passwords, private keys, storage access keys) must NEVER
# be declared here — pass them via environment variables consumed directly by
# the provider. Only non-secret configuration belongs in this file.
#
# Recommended usage:
#   export TF_VAR_environment=dev
#   export SNOWFLAKE_ACCOUNT=MYORG-MYACCOUNT
#   terraform plan -var-file=envs/dev.tfvars

# ---------------------------------------------------------------------------
# Shared
# ---------------------------------------------------------------------------

variable "environment" {
  description = "Deployment environment. Controls resource name suffixes, data-retention windows, and warehouse sizing defaults."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------

variable "snowflake_role" {
  description = "Snowflake role assumed by Terraform during provisioning. Must have SYSADMIN or ACCOUNTADMIN privileges to create databases, warehouses, and roles."
  type        = string
  default     = "SYSADMIN"
}

variable "snowflake_database" {
  description = "Base name of the Snowflake database. The environment suffix is appended by the module for non-prod environments."
  type        = string
  default     = "NYC311_DB"
}

variable "warehouse_size" {
  description = "Snowflake warehouse size passed through to the foundation module."
  type        = string
  default     = "X-SMALL"
}

variable "auto_suspend_seconds" {
  description = "Seconds of warehouse inactivity before auto-suspend. 60s is appropriate for dev; 300s (5 min) absorbs bursty BI query patterns in prod without excessive cold-start latency."
  type        = number
  default     = 60
}

# ---------------------------------------------------------------------------
# Azure (used by azure-infra module — activate when building ingestion layer)
# ---------------------------------------------------------------------------

variable "azure_location" {
  description = "Azure region for all Azure resources (ADLS Gen2, Databricks workspace)."
  type        = string
  default     = "eastus2"
}

variable "resource_group_name" {
  description = "Name of the Azure resource group. Must already exist or be created by the azure-infra module."
  type        = string
  default     = "nyc311-data-platform-rg"
}
