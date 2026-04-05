variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "databricks_account_id" {
  description = "Databricks account ID (found in Account Console)"
  type        = string
  sensitive   = true
}

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

variable "test_cluster_id" {
  description = "Identifier used to name the test schema inside Unity Catalog"
  type        = string
  default     = "lkc-test-001"
}

variable "project_name" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "lineage-bridge"
}
