output "workspace_url" {
  description = "Databricks workspace URL"
  value       = databricks_mws_workspaces.this.workspace_url
}

output "workspace_id" {
  description = "Databricks workspace ID"
  value       = databricks_mws_workspaces.this.workspace_id
}

output "token" {
  description = "Databricks personal access token"
  value       = databricks_token.service_principal.token_value
  sensitive   = true
}

output "metastore_id" {
  description = "Unity Catalog metastore ID"
  value       = databricks_metastore.this.id
}

output "catalog_name" {
  description = "Test catalog name"
  value       = databricks_catalog.confluent_tableflow.name
}

output "schema_name" {
  description = "Test schema name"
  value       = databricks_schema.test_schema.name
}
