variable "aws_region" {
  description = "AWS region for Databricks workspace"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID"
  type        = string
  sensitive   = true
}

variable "test_cluster_id" {
  description = "Test cluster identifier used for schema naming"
  type        = string
}
