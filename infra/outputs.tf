output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks.workspace_url
}

output "databricks_token" {
  description = "Databricks personal access token for service principal"
  value       = module.databricks.token
  sensitive   = true
}

output "confluent_cloud_api_key" {
  description = "Confluent Cloud API key"
  value       = module.confluent.cloud_api_key
  sensitive   = true
}

output "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret"
  value       = module.confluent.cloud_api_secret
  sensitive   = true
}

output "confluent_kafka_api_key" {
  description = "Confluent Kafka cluster-scoped API key"
  value       = module.confluent.kafka_api_key
  sensitive   = true
}

output "confluent_kafka_api_secret" {
  description = "Confluent Kafka cluster-scoped API secret"
  value       = module.confluent.kafka_api_secret
  sensitive   = true
}

output "confluent_kafka_cluster_id" {
  description = "Confluent Kafka cluster ID"
  value       = module.confluent.kafka_cluster_id
}

output "confluent_environment_id" {
  description = "Confluent Cloud environment ID"
  value       = module.confluent.environment_id
}

output "confluent_schema_registry_url" {
  description = "Confluent Schema Registry URL"
  value       = module.confluent.schema_registry_url
}
