# Remote state in Azure Blob Storage: shared between operators and CI, and
# blob leases lock the state so two applies cannot run at once.
#
# One-time bootstrap, before `terraform init` (the storage account must exist):
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
# Authenticate with the storage account key in the environment, never in a
# file:
#
#   export ARM_ACCESS_KEY=$(az storage account keys list \
#     --account-name nyc311tfstate \
#     --resource-group nyc311-tfstate-rg \
#     --query "[0].value" -o tsv)
#
#   terraform init
#
# One state file per environment, chosen on init:
#
#   terraform init -backend-config="key=nyc311/dev/terraform.tfstate"
#   terraform init -backend-config="key=nyc311/prod/terraform.tfstate"

terraform {
  backend "azurerm" {
    resource_group_name  = "nyc311-tfstate-rg"
    storage_account_name = "nyc311tfstate"
    container_name       = "tfstate"

    # Default key; override per environment with -backend-config.
    key = "nyc311/dev/terraform.tfstate"
  }
}
