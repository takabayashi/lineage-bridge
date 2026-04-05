variable "aws_region" {
  description = "AWS region for Confluent Cloud cluster"
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
