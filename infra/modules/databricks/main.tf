terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      version               = "~> 1.40"
      configuration_aliases = [databricks.account, databricks.workspace]
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  prefix     = "${var.project_name}-${var.environment}"
  aws_account_id = data.aws_caller_identity.current.account_id
}

# ── S3: Workspace Root Storage ───────────────────────────────────────────────

resource "aws_s3_bucket" "workspace_root" {
  bucket        = "${local.prefix}-workspace-root"
  force_destroy = true

  tags = {
    Name = "${local.prefix}-workspace-root"
  }
}

resource "aws_s3_bucket_versioning" "workspace_root" {
  bucket = aws_s3_bucket.workspace_root.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "workspace_root" {
  bucket = aws_s3_bucket.workspace_root.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "workspace_root" {
  bucket = aws_s3_bucket.workspace_root.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "workspace_root" {
  bucket = aws_s3_bucket.workspace_root.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root" # Databricks AWS account
        }
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.workspace_root.arn,
          "${aws_s3_bucket.workspace_root.arn}/*"
        ]
      }
    ]
  })
}

# ── IAM: Cross-Account Role for Databricks ──────────────────────────────────

resource "aws_iam_role" "databricks_cross_account" {
  name = "${local.prefix}-databricks-cross-account"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
        }
      }
    ]
  })

  tags = {
    Name = "${local.prefix}-databricks-cross-account"
  }
}

resource "aws_iam_role_policy" "databricks_cross_account" {
  name = "${local.prefix}-databricks-cross-account"
  role = aws_iam_role.databricks_cross_account.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.workspace_root.arn,
          "${aws_s3_bucket.workspace_root.arn}/*",
          aws_s3_bucket.metastore.arn,
          "${aws_s3_bucket.metastore.arn}/*"
        ]
      },
      {
        Sid    = "EC2Access"
        Effect = "Allow"
        Action = [
          "ec2:AssociateIamInstanceProfile",
          "ec2:AttachVolume",
          "ec2:AuthorizeSecurityGroupEgress",
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:CancelSpotInstanceRequests",
          "ec2:CreateTags",
          "ec2:CreateVolume",
          "ec2:DeleteTags",
          "ec2:DeleteVolume",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeIamInstanceProfileAssociations",
          "ec2:DescribeInstanceStatus",
          "ec2:DescribeInstances",
          "ec2:DescribeInternetGateways",
          "ec2:DescribeNatGateways",
          "ec2:DescribeNetworkAcls",
          "ec2:DescribePrefixLists",
          "ec2:DescribeReservedInstancesOfferings",
          "ec2:DescribeRouteTables",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSpotInstanceRequests",
          "ec2:DescribeSpotPriceHistory",
          "ec2:DescribeSubnets",
          "ec2:DescribeVolumes",
          "ec2:DescribeVpcAttribute",
          "ec2:DescribeVpcs",
          "ec2:DetachVolume",
          "ec2:DisassociateIamInstanceProfile",
          "ec2:ReplaceIamInstanceProfileAssociation",
          "ec2:RequestSpotInstances",
          "ec2:RevokeSecurityGroupEgress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:RunInstances",
          "ec2:TerminateInstances"
        ]
        Resource = "*"
      }
    ]
  })
}

# ── Databricks MWS: Credentials ─────────────────────────────────────────────

resource "databricks_mws_credentials" "this" {
  provider         = databricks.account
  credentials_name = "${local.prefix}-credentials"
  role_arn         = aws_iam_role.databricks_cross_account.arn
}

# ── Databricks MWS: Storage Configuration ───────────────────────────────────

resource "databricks_mws_storage_configurations" "this" {
  provider                   = databricks.account
  account_id                 = var.databricks_account_id
  storage_configuration_name = "${local.prefix}-storage"
  bucket_name                = aws_s3_bucket.workspace_root.bucket
}

# ── Databricks MWS: Workspace ───────────────────────────────────────────────

resource "databricks_mws_workspaces" "this" {
  provider       = databricks.account
  account_id     = var.databricks_account_id
  workspace_name = "${local.prefix}-workspace"
  aws_region     = var.aws_region

  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
}

# ── S3: Unity Catalog Metastore Storage ──────────────────────────────────────

resource "aws_s3_bucket" "metastore" {
  bucket        = "${local.prefix}-metastore"
  force_destroy = true

  tags = {
    Name = "${local.prefix}-metastore"
  }
}

resource "aws_s3_bucket_versioning" "metastore" {
  bucket = aws_s3_bucket.metastore.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "metastore" {
  bucket = aws_s3_bucket.metastore.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "metastore" {
  bucket = aws_s3_bucket.metastore.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Unity Catalog: Metastore ────────────────────────────────────────────────

resource "databricks_metastore" "this" {
  provider      = databricks.account
  name          = "${local.prefix}-metastore"
  region        = var.aws_region
  storage_root  = "s3://${aws_s3_bucket.metastore.bucket}/metastore"
  force_destroy = true
}

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  metastore_id = databricks_metastore.this.id
  workspace_id = databricks_mws_workspaces.this.workspace_id
}

# ── Unity Catalog: Test Catalog & Schema ─────────────────────────────────────

resource "databricks_catalog" "confluent_tableflow" {
  provider = databricks.workspace
  name     = "confluent_tableflow"
  comment  = "Test catalog for Confluent Tableflow integration"

  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_schema" "test_schema" {
  provider    = databricks.workspace
  catalog_name = databricks_catalog.confluent_tableflow.name
  name         = replace(var.test_cluster_id, "-", "_")
  comment      = "Schema for cluster ${var.test_cluster_id}"
}

# ── Service Principal & Token ────────────────────────────────────────────────

resource "databricks_service_principal" "lineage_bridge" {
  provider     = databricks.account
  display_name = "${local.prefix}-service-principal"
}

resource "databricks_mws_permission_assignment" "sp_workspace" {
  provider     = databricks.account
  workspace_id = databricks_mws_workspaces.this.workspace_id
  principal_id = databricks_service_principal.lineage_bridge.id
  permissions  = ["USER"]
}

resource "databricks_token" "service_principal" {
  provider = databricks.workspace
  comment  = "Token for ${local.prefix} service principal"

  depends_on = [
    databricks_metastore_assignment.this,
    databricks_mws_permission_assignment.sp_workspace
  ]
}
