# ── Confluent Core Module — Outputs ──────────────────────────────────────────

# ── Naming ──────────────────────────────────────────────────────────────────

output "demo_prefix" {
  description = "Generated demo prefix for naming downstream resources"
  value       = local.demo_prefix
}

# ── Environment ─────────────────────────────────────────────────────────────

output "environment_id" {
  description = "Confluent environment ID"
  value       = confluent_environment.demo.id
}

output "environment_display_name" {
  description = "Confluent environment display name"
  value       = confluent_environment.demo.display_name
}

output "environment_resource_name" {
  description = "Confluent environment resource name (CRN)"
  value       = confluent_environment.demo.resource_name
}

# ── Kafka Cluster ───────────────────────────────────────────────────────────

output "kafka_cluster_id" {
  description = "Kafka cluster ID"
  value       = confluent_kafka_cluster.demo.id
}

output "kafka_cluster_display_name" {
  description = "Kafka cluster display name"
  value       = confluent_kafka_cluster.demo.display_name
}

output "kafka_cluster_rest_endpoint" {
  description = "Kafka cluster REST endpoint"
  value       = confluent_kafka_cluster.demo.rest_endpoint
}

output "kafka_cluster_bootstrap_endpoint" {
  description = "Kafka cluster bootstrap endpoint"
  value       = confluent_kafka_cluster.demo.bootstrap_endpoint
}

output "kafka_cluster_api_version" {
  description = "Kafka cluster API version"
  value       = confluent_kafka_cluster.demo.api_version
}

output "kafka_cluster_kind" {
  description = "Kafka cluster kind"
  value       = confluent_kafka_cluster.demo.kind
}

output "kafka_cluster_rbac_crn" {
  description = "Kafka cluster RBAC CRN"
  value       = confluent_kafka_cluster.demo.rbac_crn
}

# ── Service Account ─────────────────────────────────────────────────────────

output "service_account_id" {
  description = "Service account ID"
  value       = confluent_service_account.demo.id
}

output "service_account_api_version" {
  description = "Service account API version"
  value       = confluent_service_account.demo.api_version
}

output "service_account_kind" {
  description = "Service account kind"
  value       = confluent_service_account.demo.kind
}

# ── API Keys ────────────────────────────────────────────────────────────────

output "kafka_api_key_id" {
  description = "Kafka API key ID"
  value       = confluent_api_key.kafka.id
}

output "kafka_api_key_secret" {
  description = "Kafka API key secret"
  value       = confluent_api_key.kafka.secret
  sensitive   = true
}

output "schema_registry_api_key_id" {
  description = "Schema Registry API key ID"
  value       = confluent_api_key.schema_registry.id
}

output "schema_registry_api_key_secret" {
  description = "Schema Registry API key secret"
  value       = confluent_api_key.schema_registry.secret
  sensitive   = true
}

output "flink_api_key_id" {
  description = "Flink API key ID"
  value       = confluent_api_key.flink.id
}

output "flink_api_key_secret" {
  description = "Flink API key secret"
  value       = confluent_api_key.flink.secret
  sensitive   = true
}

# ── Schema Registry ─────────────────────────────────────────────────────────

output "schema_registry_rest_endpoint" {
  description = "Schema Registry REST endpoint"
  value       = data.confluent_schema_registry_cluster.demo.rest_endpoint
}

output "schema_registry_cluster_id" {
  description = "Schema Registry cluster ID"
  value       = data.confluent_schema_registry_cluster.demo.id
}

# ── Flink ───────────────────────────────────────────────────────────────────

output "flink_compute_pool_id" {
  description = "Flink compute pool ID"
  value       = confluent_flink_compute_pool.demo.id
}

output "flink_region_rest_endpoint" {
  description = "Flink region REST endpoint"
  value       = data.confluent_flink_region.demo.rest_endpoint
}

output "flink_region_id" {
  description = "Flink region ID"
  value       = data.confluent_flink_region.demo.id
}

output "flink_region_api_version" {
  description = "Flink region API version"
  value       = data.confluent_flink_region.demo.api_version
}

output "flink_region_kind" {
  description = "Flink region kind"
  value       = data.confluent_flink_region.demo.kind
}

# ── Organization ────────────────────────────────────────────────────────────

output "organization_id" {
  description = "Confluent organization ID"
  value       = data.confluent_organization.demo.id
}

# ── Topics ──────────────────────────────────────────────────────────────────

output "orders_topic_name" {
  description = "Orders topic name"
  value       = confluent_kafka_topic.orders.topic_name
}

output "customers_topic_name" {
  description = "Customers topic name"
  value       = confluent_kafka_topic.customers.topic_name
}

# ── Role Binding IDs (for depends_on in downstream resources) ───────────────

output "cluster_admin_role_binding_id" {
  description = "CloudClusterAdmin role binding ID"
  value       = confluent_role_binding.cluster_admin.id
}

output "env_admin_role_binding_id" {
  description = "EnvironmentAdmin role binding ID"
  value       = confluent_role_binding.env_admin.id
}

# ── Datagen Connector IDs (for depends_on in Flink statements) ──────────────

output "datagen_orders_id" {
  description = "Datagen orders connector ID"
  value       = confluent_connector.datagen_orders.id
}

output "datagen_customers_id" {
  description = "Datagen customers connector ID"
  value       = confluent_connector.datagen_customers.id
}
