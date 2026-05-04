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
  description = <<-EOT
    BigQuery dataset name for sink connector. Auto-derived from demo_suffix
    when left empty (e.g. ``lineage_bridge_a1b2c3d4``). Only set explicitly
    when you need to share a dataset across runs.
  EOT
  type        = string
  default     = ""
}

variable "demo_suffix" {
  description = <<-EOT
    Pre-generated 8-char hex suffix used for the demo prefix and BigQuery
    dataset name. setup-tfvars.sh generates this so 'bq mk' (which runs
    before terraform apply) can name the dataset the same way the
    Confluent connector will.
  EOT
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "GCP region for Confluent Kafka cluster and Flink"
  type        = string
  default     = "us-east1"
}
