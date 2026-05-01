# ── Confluent Core Module — Input Variables ─────────────────────────────────

variable "demo_label" {
  description = "Short label identifying the demo variant (e.g. uc, glue, bq)"
  type        = string
  default     = ""
}

variable "cloud_provider" {
  description = "Cloud provider for Kafka cluster and Flink: AWS or GCP"
  type        = string
  default     = "AWS"

  validation {
    condition     = contains(["AWS", "GCP"], var.cloud_provider)
    error_message = "cloud_provider must be AWS or GCP"
  }
}

variable "cloud_region" {
  description = "Cloud region (e.g. us-east-1 for AWS, us-east1 for GCP)"
  type        = string
}

variable "topic_prefix" {
  description = "Prefix for Kafka topic names"
  type        = string
  default     = "lineage_bridge"
}

variable "flink_max_cfu" {
  description = "Maximum CFU for Flink compute pool"
  type        = number
  default     = 5
}
