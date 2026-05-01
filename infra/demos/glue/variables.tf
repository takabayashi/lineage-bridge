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

# ── AWS DataZone (optional) ─────────────────────────────────────────────────
# When set, the demo wires LineageBridge's DataZone provider so the Streamlit
# UI's "Push to DataZone" button is available out of the box. Leave empty to
# skip — the rest of the demo works without DataZone.

variable "aws_datazone_domain_id" {
  description = "Existing AWS DataZone domain ID (dzd-xxxxxx). Empty = skip DataZone wiring."
  type        = string
  default     = ""
}

variable "aws_datazone_project_id" {
  description = "Existing DataZone project ID (owns registered Kafka assets). Empty = skip."
  type        = string
  default     = ""
}
