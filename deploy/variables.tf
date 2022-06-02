variable "DATABRICKS_URL" {
  type = string
  description = "The URL to the Azure Databricks workspace (must start with https://)"
}

variable "SOURCE_FILE_PATH" {
  type = string
  description = "The relative path to notebook source file (.dbc)"
  default = "/../notebooks/sparkSqlNotebook.dbc"
}