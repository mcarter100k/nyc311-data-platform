# terraform/backend.tf
#
# Remote state configuration — Azure Blob Storage.
#
# State is stored remotely so that:
#   1. Multiple team members and CI runners share a single source of truth.
#   2. State is not lost if a local machine is wiped.
#   3. Azure Blob lease-based locking prevents concurrent applies from
#      corrupting state.
#
# ── Bootstrap (one-time, before `terraform init`) ────────────────────────────
#
# The storage account must exist before Terraform can write state to it.
# Run the following Azure CLI commands once per environment:
#
#   LOCATION="eastus2"
#   RG="nyc311-tfstate-rg"
#   SA="nyc311tfstate"          # must be globally unique — adjust as needed
#   CONTAINER="tfstate"
#
#   az group create --name $RG --location $LOCATION
#
#   az storage account create \
#     --name $SA \
#     --resource-group $RG \
#     --location $LOCATION \
#     --sku Standard_LRS \
#     --kind StorageV2 \
#     --min-tls-version TLS1_2 \
#     --allow-blob-public-access false
#
#   az storage container create \
#     --name $CONTAINER \
#     --account-name $SA \
#     --auth-mode login
#
# ── Authentication ────────────────────────────────────────────────────────────
#
# Pass the storage account access key via environment variable — never
# hardcode it here or in any .tfvars file:
#
#   export ARM_ACCESS_KEY=$(az storage account keys list \
#     --account-name nyc311tfstate \
#     --resource-group nyc311-tfstate-rg \
#     --query "[0].value" -o tsv)
#
#   terraform init
#
# In CI (GitHub Actions), set ARM_ACCESS_KEY as a repository secret and inject
# it via the `env:` block in the workflow step.
#
# ── State file key convention ─────────────────────────────────────────────────
#
# Use one state file per environment by passing -backend-config on init:
#
#   terraform init -backend-config="key=nyc311/dev/terraform.tfstate"
#   terraform init -backend-config="key=nyc311/prod/terraform.tfstate"
#
# This allows dev and prod state to coexist in the same container without
# risk of cross-environment overwrites.

terraform {
  backend "azurerm" {
    resource_group_name  = "nyc311-tfstate-rg"
    storage_account_name = "nyc311tfstate"
    container_name       = "tfstate"

    # Default key — override per-environment with -backend-config on init.
    key = "nyc311/dev/terraform.tfstate"

    # ARM_ACCESS_KEY must be set in the environment — never hardcoded.
  }
}
