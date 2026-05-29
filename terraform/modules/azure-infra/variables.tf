# modules/azure-infra/variables.tf
# Purpose: Input variables for the Azure infrastructure module.

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "storage_account_tier" {
  description = "Performance tier for ADLS Gen2 (Standard or Premium)"
  type        = string
  default     = "Standard"
}

variable "databricks_sku" {
  description = "Databricks workspace SKU (standard, premium, trial)"
  type        = string
  default     = "standard"
}
