# 🚀 Azure Data Platform — Binance Pipeline

## 📌 Overview

This project defines the infrastructure for a **data pipeline that ingests Binance market data into Azure**.

> ⚠️ **Note**  
> Resources are currently provisioned manually via the Azure Portal.  
> This Terraform configuration serves as:
> - 📄 Documentation of the deployed infrastructure  
> - 🏗️ A baseline for future Infrastructure-as-Code (IaC) automation  

---

## 🧱 Infrastructure Components

The following Azure resources are part of this setup:

- Azure Data Lake Storage Gen2 (Storage Account)
- Data Lake Filesystem (Container)
- Azure Synapse Workspace
- Dedicated SQL Pool (DW100c tier)
- Firewall rules for development access

---

## ⚙️ Terraform Configuration

### 🔹 Provider Configuration

```hcl
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=4.1.0"
    }
  }
}

provider "azurerm" {
  features {}
  resource_provider_registrations = "none"
}

🔹 Reference Existing Resource Group
data "azurerm_resource_group" "rg" {
  name = "de-for-oj"
}
🔹 Storage Account (ADLS Gen2)
resource "azurerm_storage_account" "adls" {
  name                     = "dezoom26adls01"
  resource_group_name      = data.azurerm_resource_group.rg.name
  location                 = data.azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  is_hns_enabled = true

  tags = {
    project = "dezoom26"
  }
}
🔹 Data Lake Filesystem
resource "azurerm_storage_data_lake_gen2_filesystem" "fs" {
  name               = "dezoom26-data"
  storage_account_id = azurerm_storage_account.adls.id
}
🔹 Synapse Workspace
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "dezoom26synapse01"
  resource_group_name                  = data.azurerm_resource_group.rg.name
  location                             = data.azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.fs.id

  sql_administrator_login          = "synapseadmin"
  sql_administrator_login_password = "Dezoom26@StrongPass1"

  identity {
    type = "SystemAssigned"
  }

  tags = {
    project = "dezoom26"
  }
}
🔹 Dedicated SQL Pool
resource "azurerm_synapse_sql_pool" "sqlpool" {
  name                 = "dezoom26sqlpool01"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  storage_account_type = "LRS"
  sku_name             = "DW100c"
  create_mode          = "Default"

  geo_backup_policy_enabled = false
}
🔹 Firewall Rules
Codespaces Access
resource "azurerm_synapse_firewall_rule" "codespace_ip" {
  name                 = "AllowCodespace"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id

  start_ip_address = "172.166.151.116"
  end_ip_address   = "172.166.151.116"
}
Personal Laptop Access
resource "azurerm_synapse_firewall_rule" "personal_ip" {
  name                 = "AllowPersonal"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id

  start_ip_address = "102.88.55.203"
  end_ip_address   = "102.88.55.203"
}
🚀 Deployment Steps

Run the following commands from your project directory:

terraform init
terraform plan
terraform apply