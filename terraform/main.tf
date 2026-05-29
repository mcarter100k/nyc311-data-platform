# terraform/main.tf
#
# Root Terraform configuration for the NYC 311 Data Platform.
# Declares required providers with explicit version pins and wires together
# the snowflake-foundation child module.
#
# Authentication — credentials are never stored in .tf files.
# Pass them via environment variables before running terraform plan/apply:
#
#   Snowflake:
#     SNOWFLAKE_ACCOUNT    — account identifier in org-account format (e.g. MYORG-MYACCOUNT)
#     SNOWFLAKE_USER       — service user with SYSADMIN role
#     SNOWFLAKE_PASSWORD   — password (or use SNOWFLAKE_PRIVATE_KEY + SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
#
#   Azure (for backend + azure-infra module):
#     ARM_CLIENT_ID        — service principal app ID
#     ARM_CLIENT_SECRET    — service principal secret
#     ARM_TENANT_ID        — Azure AD tenant
#     ARM_SUBSCRIPTION_ID  — target subscription
#     ARM_ACCESS_KEY       — storage account key for backend state locking

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    # terraform-provider-snowflake underwent a major API rewrite starting at v0.87.
    # Pinned to ~> 0.89 (post-rewrite stable series) to avoid both the pre-rewrite
    # deprecated resources and the further breaking changes introduced in 0.90+.
    # Re-evaluate this pin before upgrading; review the provider changelog carefully.
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.89.0"
    }

    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }
}

provider "snowflake" {
  # Account identifier and user credentials are sourced from environment variables
  # (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD / SNOWFLAKE_PRIVATE_KEY).
  # SYSADMIN is used for provisioning; application roles (LOADER, TRANSFORMER, REPORTER)
  # are created by this configuration and assigned to service users separately.
  role = var.snowflake_role
}

provider "azurerm" {
  features {}
  # Credentials sourced from ARM_* environment variables (see header above).
}

# ---------------------------------------------------------------------------
# snowflake-foundation module
# Provisions database, schemas, warehouse, roles, and all grants.
# ---------------------------------------------------------------------------

module "snowflake_foundation" {
  source = "./modules/snowflake-foundation"

  environment          = var.environment
  database_name        = var.snowflake_database
  warehouse_size       = var.warehouse_size
  auto_suspend_seconds = var.auto_suspend_seconds
}

# ---------------------------------------------------------------------------
# azure-infra module (ADLS Gen2 + Databricks workspace)
# Uncommment when building the ingestion layer.
# ---------------------------------------------------------------------------

# module "azure_infra" {
#   source = "./modules/azure-infra"
#
#   environment         = var.environment
#   resource_group_name = var.resource_group_name
#   location            = var.azure_location
# }
