# We strongly recommend using the required_providers block to set the
# Azure Provider source and version being used
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=4.1.0"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
  resource_provider_registrations = "none"
}

# This Resource Group Already Exisit
data "azurerm_resource_group" "rg" {
  name = "de-for-oj"  
}

resource "azurerm_storage_account" "adls" {
  name                     = "dezoom26adls01" # must be globally unique
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

resource "azurerm_storage_data_lake_gen2_filesystem" "fs" {
  name               = "dezoom26-data"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "dezoom26synapse01" # must be globally unique
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

resource "azurerm_synapse_sql_pool" "sqlpool" {
  name                 = "dezoom26sqlpool01"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  storage_account_type = "LRS"
  sku_name = "DW100c"
  create_mode = "Default"
  geo_backup_policy_enabled = false

}

# Codespaces IP
resource "azurerm_synapse_firewall_rule" "codespace_ip" {
  name                 = "AllowCodespace"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id

  start_ip_address = "172.166.151.116"
  end_ip_address   = "172.166.151.116"
}

# Personal laptop IP
resource "azurerm_synapse_firewall_rule" "personal_ip" {
  name                 = "AllowPersonal"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id

  start_ip_address = "102.88.55.203"
  end_ip_address   = "102.88.55.203"
}
