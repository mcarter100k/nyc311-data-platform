# modules/snowflake-foundation/variables.tf
#
# Input variables for the Snowflake foundation module.
# All sensitive credentials (password, private key) are passed via environment
# variables consumed directly by the provider — they must never appear here.

variable "environment" {
  description = "Deployment environment. Controls object name suffixes and data-retention windows."
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "database_name" {
  description = "Base name for the Snowflake database. The environment suffix is appended automatically for non-prod environments (e.g. NYC311_DB_DEV)."
  type        = string
  default     = "NYC311_DB"
}

variable "warehouse_size" {
  description = "Snowflake virtual warehouse T-shirt size. X-SMALL is sufficient for dbt workloads on this dataset; scale up for concurrent BI users."
  type        = string
  default     = "X-SMALL"

  validation {
    condition = contains(
      ["X-SMALL", "SMALL", "MEDIUM", "LARGE", "X-LARGE",
      "2X-LARGE", "3X-LARGE", "4X-LARGE", "5X-LARGE", "6X-LARGE"],
      var.warehouse_size
    )
    error_message = "warehouse_size must be a valid Snowflake warehouse size (X-SMALL through 6X-LARGE)."
  }
}

variable "auto_suspend_seconds" {
  description = "Seconds of inactivity before the warehouse auto-suspends. 60 seconds balances cold-start latency against idle credit burn. Lower in prod if query SLAs require it."
  type        = number
  default     = 60

  validation {
    condition     = var.auto_suspend_seconds >= 60
    error_message = "Snowflake enforces a minimum auto_suspend of 60 seconds."
  }
}
