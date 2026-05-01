# ── LineageBridge Demo — Input Variables ──────────────────────────────────────

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

variable "confluent_tableflow_api_key" {
  description = "Confluent Tableflow API key (create with: confluent api-key create --resource tableflow)"
  type        = string
  sensitive   = true
}

variable "confluent_tableflow_api_secret" {
  description = "Confluent Tableflow API secret"
  type        = string
  sensitive   = true
}

variable "databricks_workspace_url" {
  description = "Databricks workspace URL (e.g. https://dbc-xxx.cloud.databricks.com)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token (for workspace provider)"
  type        = string
  sensitive   = true
}

variable "databricks_client_id" {
  description = "Databricks service principal OAuth client ID (for catalog integration + grants)"
  type        = string
  sensitive   = true
}

variable "databricks_client_secret" {
  description = "Databricks service principal OAuth client secret (for catalog integration)"
  type        = string
  sensitive   = true
}

variable "aws_account_id" {
  description = "AWS account ID (12 digits)"
  type        = string
}

variable "aws_region" {
  description = "AWS region for S3 bucket and Confluent Kafka cluster"
  type        = string
  default     = "us-east-1"
}

# ── GCP / BigQuery (optional — leave empty to skip) ─────────────────────────

variable "gcp_project_id" {
  description = "GCP project ID for BigQuery sink connector (leave empty to skip)"
  type        = string
  default     = ""
}

variable "gcp_sa_key_json" {
  description = "GCP service account JSON key content for BigQuery connector"
  type        = string
  sensitive   = true
  default     = ""
}

variable "bigquery_dataset" {
  description = "BigQuery dataset name for sink connector"
  type        = string
  default     = "lineage_bridge"
}
