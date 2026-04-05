output "environment_id" {
  description = "Confluent Cloud environment ID"
  value       = confluent_environment.this.id
}

output "kafka_cluster_id" {
  description = "Kafka cluster ID"
  value       = confluent_kafka_cluster.this.id
}

output "kafka_bootstrap_endpoint" {
  description = "Kafka bootstrap server endpoint"
  value       = confluent_kafka_cluster.this.bootstrap_endpoint
}

output "schema_registry_url" {
  description = "Schema Registry REST endpoint"
  value       = data.confluent_schema_registry_cluster.this.rest_endpoint
}

output "cloud_api_key" {
  description = "Cloud-level API key for the service account"
  value       = confluent_api_key.cloud.id
  sensitive   = true
}

output "cloud_api_secret" {
  description = "Cloud-level API secret for the service account"
  value       = confluent_api_key.cloud.secret
  sensitive   = true
}

output "kafka_api_key" {
  description = "Cluster-scoped Kafka API key"
  value       = confluent_api_key.kafka.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Cluster-scoped Kafka API secret"
  value       = confluent_api_key.kafka.secret
  sensitive   = true
}

output "service_account_id" {
  description = "Confluent service account ID"
  value       = confluent_service_account.lineage_bridge.id
}
