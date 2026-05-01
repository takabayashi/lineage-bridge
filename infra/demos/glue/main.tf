# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge Demo: Tableflow → AWS Glue
#
# End-to-end lineage: Datagen → Kafka → Flink → Tableflow (Iceberg) → Glue
# Simpler demo focused on AWS-native catalog integration.
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }
}

# ── Providers ───────────────────────────────────────────────────────────────

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret

  tableflow_api_key    = var.confluent_tableflow_api_key
  tableflow_api_secret = var.confluent_tableflow_api_secret
}

provider "aws" {
  region = var.aws_region
}

# ═══════════════════════════════════════════════════════════════════════════════
# CONFLUENT CORE (via module)
# ═══════════════════════════════════════════════════════════════════════════════

module "core" {
  source = "../modules/confluent-core"

  demo_label                 = "glue"
  cloud_provider             = "AWS"
  cloud_region               = var.aws_region
}

# ── Wait for datagen connectors to register schemas ─────────────────────────

resource "time_sleep" "datagen_warmup" {
  create_duration = "60s"
  depends_on      = [module.core]
}

# ═══════════════════════════════════════════════════════════════════════════════
# FLINK — SQL Statements
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_flink_statement" "drop_enriched_orders" {
  organization {
    id = module.core.organization_id
  }

  environment {
    id = module.core.environment_id
  }

  compute_pool {
    id = module.core.flink_compute_pool_id
  }

  principal {
    id = module.core.service_account_id
  }

  statement_name = "${module.core.demo_prefix}-drop-enriched-orders"
  rest_endpoint  = module.core.flink_region_rest_endpoint

  statement = "DROP TABLE IF EXISTS `lineage_bridge.enriched_orders`;"

  properties = {
    "sql.current-catalog"  = module.core.environment_display_name
    "sql.current-database" = module.core.kafka_cluster_display_name
  }

  credentials {
    key    = module.core.flink_api_key_id
    secret = module.core.flink_api_key_secret
  }

  depends_on = [time_sleep.datagen_warmup]

  lifecycle {
    ignore_changes = all
  }
}

resource "confluent_flink_statement" "drop_order_stats" {
  organization {
    id = module.core.organization_id
  }

  environment {
    id = module.core.environment_id
  }

  compute_pool {
    id = module.core.flink_compute_pool_id
  }

  principal {
    id = module.core.service_account_id
  }

  statement_name = "${module.core.demo_prefix}-drop-order-stats"
  rest_endpoint  = module.core.flink_region_rest_endpoint

  statement = "DROP TABLE IF EXISTS `lineage_bridge.order_stats`;"

  properties = {
    "sql.current-catalog"  = module.core.environment_display_name
    "sql.current-database" = module.core.kafka_cluster_display_name
  }

  credentials {
    key    = module.core.flink_api_key_id
    secret = module.core.flink_api_key_secret
  }

  depends_on = [time_sleep.datagen_warmup]

  lifecycle {
    ignore_changes = all
  }
}

resource "confluent_flink_statement" "enriched_orders" {
  organization {
    id = module.core.organization_id
  }

  environment {
    id = module.core.environment_id
  }

  compute_pool {
    id = module.core.flink_compute_pool_id
  }

  principal {
    id = module.core.service_account_id
  }

  statement_name = "${module.core.demo_prefix}-enrich-orders"
  rest_endpoint  = module.core.flink_region_rest_endpoint

  statement = <<-SQL
    CREATE TABLE `lineage_bridge.enriched_orders` AS
    SELECT
      o.`order_id`,
      o.`customer_id`,
      c.`name`       AS `customer_name`,
      c.`country`    AS `customer_country`,
      o.`product_name`,
      o.`quantity`,
      o.`price`,
      o.`order_status`,
      o.`created_at`
    FROM `${module.core.orders_topic_name}` o
    LEFT JOIN `${module.core.customers_topic_name}` c
      ON o.`customer_id` = c.`customer_id`;
  SQL

  properties = {
    "sql.current-catalog"  = module.core.environment_display_name
    "sql.current-database" = module.core.kafka_cluster_display_name
  }

  credentials {
    key    = module.core.flink_api_key_id
    secret = module.core.flink_api_key_secret
  }

  depends_on = [
    confluent_flink_statement.drop_enriched_orders,
  ]
}

resource "confluent_flink_statement" "order_stats" {
  organization {
    id = module.core.organization_id
  }

  environment {
    id = module.core.environment_id
  }

  compute_pool {
    id = module.core.flink_compute_pool_id
  }

  principal {
    id = module.core.service_account_id
  }

  statement_name = "${module.core.demo_prefix}-order-stats"
  rest_endpoint  = module.core.flink_region_rest_endpoint

  statement = <<-SQL
    CREATE TABLE `lineage_bridge.order_stats` AS
    SELECT
      `order_status`,
      COUNT(*)        AS `order_count`,
      SUM(`quantity`) AS `total_quantity`,
      window_start,
      window_end
    FROM TABLE(
      TUMBLE(TABLE `${module.core.orders_topic_name}`, DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE)
    )
    GROUP BY `order_status`, window_start, window_end;
  SQL

  properties = {
    "sql.current-catalog"  = module.core.environment_display_name
    "sql.current-database" = module.core.kafka_cluster_display_name
  }

  credentials {
    key    = module.core.flink_api_key_id
    secret = module.core.flink_api_key_secret
  }

  depends_on = [
    confluent_flink_statement.drop_order_stats,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# AWS — S3 Bucket + IAM Role (for Confluent Tableflow BYOB)
# ═══════════════════════════════════════════════════════════════════════════════

resource "aws_s3_bucket" "tableflow" {
  bucket        = "${module.core.demo_prefix}-tableflow"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "tableflow" {
  bucket                  = aws_s3_bucket.tableflow.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tableflow" {
  bucket = aws_s3_bucket.tableflow.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_iam_role" "tableflow" {
  name = "${module.core.demo_prefix}-tableflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "SelfAssume"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.aws_account_id}:root" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  lifecycle {
    ignore_changes = [assume_role_policy]
  }
}

resource "aws_iam_role_policy" "tableflow_s3" {
  name = "tableflow-s3-access"
  role = aws_iam_role.tableflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:*"
        Resource = [aws_s3_bucket.tableflow.arn, "${aws_s3_bucket.tableflow.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "tableflow_glue" {
  name = "tableflow-glue-access"
  role = aws_iam_role.tableflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/*",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/*/*",
        ]
      }
    ]
  })
}

resource "aws_s3_bucket_policy" "tableflow" {
  bucket = aws_s3_bucket.tableflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "SharedRoleAccess"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.tableflow.arn }
        Action    = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource  = [aws_s3_bucket.tableflow.arn, "${aws_s3_bucket.tableflow.arn}/*"]
      }
    ]
  })
}

# ═══════════════════════════════════════════════════════════════════════════════
# CONFLUENT — Provider Integration (Tableflow → S3)
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_provider_integration" "aws" {
  display_name = "${module.core.demo_prefix}-aws"

  environment {
    id = module.core.environment_id
  }

  aws {
    customer_role_arn = aws_iam_role.tableflow.arn
  }
}

resource "time_sleep" "provider_integration_destroy_delay" {
  destroy_duration = "60s"

  triggers = {
    provider_integration_id = confluent_provider_integration.aws.id
  }
}

# ── IAM Trust Policy — Single Phase (Confluent only, no Databricks) ────────

resource "terraform_data" "update_trust_confluent" {
  triggers_replace = [confluent_provider_integration.aws.id]

  provisioner "local-exec" {
    command = <<-EOT
      aws iam update-assume-role-policy --output json \
        --role-name "${aws_iam_role.tableflow.name}" \
        --policy-document '${jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "SelfAssume"
        Effect    = "Allow"
        Principal = { AWS = ["arn:aws:iam::${var.aws_account_id}:root", aws_iam_role.tableflow.arn] }
        Action    = "sts:AssumeRole"
      },
      {
        Sid       = "ConfluentAssumeRole"
        Effect    = "Allow"
        Principal = { AWS = confluent_provider_integration.aws.aws[0].iam_role_arn }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = confluent_provider_integration.aws.aws[0].external_id
          }
        }
      },
      {
        Sid       = "ConfluentTagSession"
        Effect    = "Allow"
        Principal = { AWS = confluent_provider_integration.aws.aws[0].iam_role_arn }
        Action    = "sts:TagSession"
      }
    ]
})}'
    EOT
  }
}

resource "time_sleep" "trust_propagation" {
  create_duration = "30s"
  depends_on      = [terraform_data.update_trust_confluent]
}

# ═══════════════════════════════════════════════════════════════════════════════
# TABLEFLOW — BYOB / Iceberg + AWS Glue Catalog Integration
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_catalog_integration" "glue" {
  display_name = "${module.core.demo_prefix}-glue"

  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  aws_glue {
    provider_integration_id = time_sleep.provider_integration_destroy_delay.triggers["provider_integration_id"]
  }

  credentials {
    key    = var.confluent_tableflow_api_key
    secret = var.confluent_tableflow_api_secret
  }

  depends_on = [
    aws_iam_role_policy.tableflow_glue,
    time_sleep.trust_propagation,
  ]
}

resource "confluent_tableflow_topic" "orders" {
  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  display_name  = module.core.orders_topic_name
  table_formats = ["ICEBERG"]

  byob_aws {
    bucket_name             = aws_s3_bucket.tableflow.bucket
    provider_integration_id = time_sleep.provider_integration_destroy_delay.triggers["provider_integration_id"]
  }

  credentials {
    key    = var.confluent_tableflow_api_key
    secret = var.confluent_tableflow_api_secret
  }

  depends_on = [
    time_sleep.trust_propagation,
    confluent_catalog_integration.glue,
  ]
}

resource "confluent_tableflow_topic" "customers" {
  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  display_name  = module.core.customers_topic_name
  table_formats = ["ICEBERG"]

  byob_aws {
    bucket_name             = aws_s3_bucket.tableflow.bucket
    provider_integration_id = time_sleep.provider_integration_destroy_delay.triggers["provider_integration_id"]
  }

  credentials {
    key    = var.confluent_tableflow_api_key
    secret = var.confluent_tableflow_api_secret
  }

  depends_on = [
    time_sleep.trust_propagation,
    confluent_catalog_integration.glue,
  ]
}

resource "confluent_tableflow_topic" "order_stats" {
  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  display_name  = "lineage_bridge.order_stats"
  table_formats = ["ICEBERG"]

  byob_aws {
    bucket_name             = aws_s3_bucket.tableflow.bucket
    provider_integration_id = time_sleep.provider_integration_destroy_delay.triggers["provider_integration_id"]
  }

  credentials {
    key    = var.confluent_tableflow_api_key
    secret = var.confluent_tableflow_api_secret
  }

  depends_on = [
    confluent_flink_statement.order_stats,
    confluent_catalog_integration.glue,
    time_sleep.trust_propagation,
  ]
}

# ── Health Check ────────────────────────────────────────────────────────────

resource "terraform_data" "health_check" {
  input = "health-check"

  provisioner "local-exec" {
    command = "bash ${path.module}/scripts/wait-for-ready.sh"

    environment = {
      CONFLUENT_API_KEY    = var.confluent_tableflow_api_key
      CONFLUENT_API_SECRET = var.confluent_tableflow_api_secret
      ENV_ID               = module.core.environment_id
      CLUSTER_ID           = module.core.kafka_cluster_id
      TOPIC_NAMES          = "lineage_bridge.orders_v2,lineage_bridge.customers_v2,lineage_bridge.order_stats"
    }
  }

  depends_on = [
    confluent_tableflow_topic.orders,
    confluent_tableflow_topic.customers,
    confluent_tableflow_topic.order_stats,
  ]
}
