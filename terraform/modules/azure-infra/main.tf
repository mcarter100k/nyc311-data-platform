# modules/azure-infra/main.tf
# Purpose: Provisions all Azure resources required by the pipeline:
#   - Resource Group
#   - Azure Data Lake Storage Gen2 (with Bronze, Silver containers)
#   - Azure Databricks Workspace (Standard or Premium tier)
#   - Service Principal + role assignments for Databricks ↔ ADLS access
# All resources are tagged with environment and project labels.

# stub — full resource definitions follow in the Terraform build phase
