# ── Glue Demo — Input Variables ──────────────────────────────────────────────

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

# ── AWS ─────────────────────────────────────────────────────────────────────

variable "aws_account_id" {
  description = "AWS account ID (12 digits)"
  type        = string
}

variable "aws_region" {
  description = "AWS region for S3 bucket and Confluent Kafka cluster"
  type        = string
  default     = "us-east-1"
}
