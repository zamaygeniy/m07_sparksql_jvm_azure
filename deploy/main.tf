# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
      resource_group_name = "terraformrg"
      storage_account_name = "terraformsatraining"
      container_name = "terraform"
      key = "databricksNotebook.tfstate"
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

provider "databricks" {
  host = var.DATABRICKS_URL
}

data "databricks_current_user" "me" {}

resource "databricks_notebook" "sparkSqlHomework" {
  source = "${path.module}/${var.SOURCE_FILE_PATH}"
  path   = "/Shared/m07sparksql"
}

output "notebook_url" {
  value = databricks_notebook.sparkSqlHomework.url
}
