# ── BigQuery Demo — Input Variables ──────────────────────────────────────────

# ── Confluent Cloud ─────────────────────────────────────────────────────────

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (cloud-level, not cluster-scoped)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret"
  type        = string
  sensitive   = true
}

# ── GCP / BigQuery ──────────────────────────────────────────────────────────

variable "gcp_project_id" {
  description = "GCP project ID for BigQuery sink connector"
  type        = string
}

variable "gcp_sa_key_json" {
  description = "GCP service account JSON key content for BigQuery connector (auto-provisioned by provision-demo.sh)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "bigquery_dataset" {
  description = "BigQuery dataset name for sink connector"
  type        = string
  default     = "lineage_bridge"
}

variable "gcp_region" {
  description = "GCP region for Confluent Kafka cluster and Flink"
  type        = string
  default     = "us-east1"
}
