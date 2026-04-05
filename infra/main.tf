terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }

  # Local backend (default for POC)
  backend "local" {
    path = "terraform.tfstate"
  }

  # Remote S3 backend (uncomment for shared state)
  # backend "s3" {
  #   bucket         = "lineage-bridge-terraform-state"
  #   key            = "infra/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "lineage-bridge-terraform-lock"
  #   encrypt        = true
  # }
}

# ── Providers ────────────────────────────────────────────────────────────────

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Databricks account-level provider (for MWS / workspace provisioning)
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}

# Databricks workspace-level provider (configured after workspace is created)
provider "databricks" {
  alias = "workspace"
  host  = module.databricks.workspace_url
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# ── Modules ──────────────────────────────────────────────────────────────────

module "databricks" {
  source = "./modules/databricks"

  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    aws                  = aws
  }

  aws_region             = var.aws_region
  environment            = var.environment
  project_name           = var.project_name
  databricks_account_id  = var.databricks_account_id
  test_cluster_id        = var.test_cluster_id
}

module "confluent" {
  source = "./modules/confluent"

  aws_region   = var.aws_region
  environment  = var.environment
  project_name = var.project_name
}
