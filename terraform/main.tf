# Root module for the Snowflake foundation. Written and validated in CI, never
# applied (applying needs a paid Snowflake account).
#
# Credentials come from environment variables, never .tf files:
#   SNOWFLAKE_ACCOUNT    account identifier, org-account format (MYORG-MYACCOUNT)
#   SNOWFLAKE_USER       service user with the SYSADMIN role
#   SNOWFLAKE_PASSWORD   or SNOWFLAKE_PRIVATE_KEY + SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
#   ARM_ACCESS_KEY       storage account key for the state backend (backend.tf)

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    # 0.89.x: after the provider's 0.87 rewrite, before the breaking changes in
    # 0.90+. Read the changelog before moving this pin.
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.89.0"
    }
  }
}

provider "snowflake" {
  # SYSADMIN provisions; the LOADER, TRANSFORMER and REPORTER roles it creates
  # are assigned to service users separately.
  role = var.snowflake_role
}

# Database, schemas, warehouse, roles and grants.
module "snowflake_foundation" {
  source = "./modules/snowflake-foundation"

  environment          = var.environment
  database_name        = var.snowflake_database
  warehouse_size       = var.warehouse_size
  auto_suspend_seconds = var.auto_suspend_seconds
}
