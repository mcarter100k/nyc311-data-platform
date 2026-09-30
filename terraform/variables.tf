# Non-secret inputs only. Passwords, keys and access keys come from
# environment variables read by the provider (see main.tf).
#
# Usage:
#   export SNOWFLAKE_ACCOUNT=MYORG-MYACCOUNT
#   terraform plan -var environment=dev

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
