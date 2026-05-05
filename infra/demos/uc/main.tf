# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge Demo: Tableflow → Databricks Unity Catalog
#
# End-to-end lineage: PostgreSQL → Kafka → Flink → ksqlDB → Tableflow → UC
# This is the flagship demo with the richest lineage graph.
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
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
    null = {
      source  = "hashicorp/null"
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

provider "databricks" {
  host  = var.databricks_workspace_url
  token = var.databricks_token
}

provider "aws" {
  region = var.aws_region
}

# ═══════════════════════════════════════════════════════════════════════════════
# CONFLUENT CORE (via module)
# ═══════════════════════════════════════════════════════════════════════════════

module "core" {
  source = "../modules/confluent-core"

  demo_label     = "uc"
  cloud_provider = "AWS"
  cloud_region   = var.aws_region
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
# KSQLDB — Cluster + API Key + Queries
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_ksql_cluster" "demo" {
  display_name = "${module.core.demo_prefix}-ksqldb"
  csu          = 4

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  credential_identity {
    id = module.core.service_account_id
  }

  environment {
    id = module.core.environment_id
  }

  depends_on = [
    module.core,
  ]
}

resource "confluent_api_key" "ksqldb" {
  display_name = "${module.core.demo_prefix}-ksqldb-key"
  description  = "ksqlDB API key for demo"

  owner {
    id          = module.core.service_account_id
    api_version = module.core.service_account_api_version
    kind        = module.core.service_account_kind
  }

  managed_resource {
    id          = confluent_ksql_cluster.demo.id
    api_version = confluent_ksql_cluster.demo.api_version
    kind        = confluent_ksql_cluster.demo.kind

    environment {
      id = module.core.environment_id
    }
  }
}

resource "null_resource" "ksqldb_high_value_orders" {
  triggers = {
    ksql_cluster_id = confluent_ksql_cluster.demo.id
  }

  provisioner "local-exec" {
    command = <<-EOT
      curl -s -u "${confluent_api_key.ksqldb.id}:${confluent_api_key.ksqldb.secret}" \
        -X POST "${confluent_ksql_cluster.demo.rest_endpoint}/ksql" \
        -H "Content-Type: application/vnd.ksql.v1+json" \
        -d '{
          "ksql": "CREATE STREAM IF NOT EXISTS orders_stream (order_id STRING, customer_id STRING, product_name STRING, quantity INT, price DOUBLE, order_status STRING, created_at STRING) WITH (KAFKA_TOPIC='"'"'${module.core.orders_topic_name}'"'"', VALUE_FORMAT='"'"'AVRO'"'"');",
          "streamsProperties": {}
        }'
      sleep 5
      curl -s -u "${confluent_api_key.ksqldb.id}:${confluent_api_key.ksqldb.secret}" \
        -X POST "${confluent_ksql_cluster.demo.rest_endpoint}/ksql" \
        -H "Content-Type: application/vnd.ksql.v1+json" \
        -d '{
          "ksql": "CREATE STREAM IF NOT EXISTS high_value_orders WITH (KAFKA_TOPIC='"'"'lineage_bridge.high_value_orders'"'"', VALUE_FORMAT='"'"'AVRO'"'"') AS SELECT order_id, customer_id, product_name, quantity, price, order_status FROM orders_stream WHERE price > 50.0 EMIT CHANGES;",
          "streamsProperties": {}
        }'
    EOT
  }

  depends_on = [
    confluent_api_key.ksqldb,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# AWS — PostgreSQL RDS (sink target for enriched orders)
# ═══════════════════════════════════════════════════════════════════════════════

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_security_group" "postgres" {
  name        = "${module.core.demo_prefix}-postgres"
  description = "Allow PostgreSQL access from anywhere (demo only)"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "PostgreSQL from anywhere (demo)"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${module.core.demo_prefix}-postgres" }
}

resource "aws_db_subnet_group" "demo" {
  name       = "${module.core.demo_prefix}-postgres"
  subnet_ids = data.aws_subnets.default.ids

  tags = { Name = "${module.core.demo_prefix}-postgres" }
}

resource "random_password" "postgres" {
  length  = 24
  special = false
}

resource "aws_db_instance" "postgres" {
  identifier     = "${module.core.demo_prefix}-postgres"
  engine         = "postgres"
  engine_version = "16"
  instance_class = "db.t4g.micro"

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = "lineage_bridge"
  username = "lineage_bridge"
  password = random_password.postgres.result

  db_subnet_group_name   = aws_db_subnet_group.demo.name
  vpc_security_group_ids = [aws_security_group.postgres.id]
  publicly_accessible    = true
  skip_final_snapshot    = true

  tags = { Name = "${module.core.demo_prefix}-postgres" }
}

# ── PostgreSQL Sink Connector ───────────────────────────────────────────────

resource "confluent_connector" "postgres_sink" {
  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  config_nonsensitive = {
    "connector.class"          = "PostgresSink"
    "name"                     = "${module.core.demo_prefix}-postgres-sink"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = module.core.service_account_id
    "input.data.format"        = "AVRO"
    "connection.host"          = aws_db_instance.postgres.address
    "connection.port"          = "5432"
    "connection.user"          = aws_db_instance.postgres.username
    "db.name"                  = aws_db_instance.postgres.db_name
    "ssl.mode"                 = "require"
    "insert.mode"              = "UPSERT"
    "pk.mode"                  = "record_value"
    "pk.fields"                = "order_id"
    "auto.create"              = "true"
    "auto.evolve"              = "true"
    "topics"                   = "lineage_bridge.enriched_orders"
    "tasks.max"                = "1"
  }

  config_sensitive = {
    "connection.password" = random_password.postgres.result
  }

  depends_on = [
    confluent_flink_statement.enriched_orders,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# AWS — S3 Bucket + IAM Role (shared by Confluent Tableflow + Databricks UC)
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

# ═══════════════════════════════════════════════════════════════════════════════
# DATABRICKS — Storage Credential + Two-Phase IAM Trust
# ═══════════════════════════════════════════════════════════════════════════════

resource "databricks_storage_credential" "tableflow" {
  name    = "${module.core.demo_prefix}-tableflow"
  comment = "Shared credential for Confluent Tableflow BYOB bucket"

  aws_iam_role {
    role_arn = aws_iam_role.tableflow.arn
  }
}

# Wait for the freshly-created IAM role to propagate before referencing it
# as a principal in its own trust policy. Without this, phase 1 intermittently
# fails with `MalformedPolicyDocument: Invalid principal in policy` even though
# the role exists, because IAM's policy validator lags role creation.
resource "time_sleep" "role_propagation" {
  create_duration = "20s"
  triggers = {
    role_arn = aws_iam_role.tableflow.arn
  }
}

# Phase 1: Add Databricks trust (storage credential external ID)
resource "terraform_data" "update_trust_phase1" {
  triggers_replace = [databricks_storage_credential.tableflow.id]

  provisioner "local-exec" {
    command = <<-EOT
      aws iam update-assume-role-policy --output json \
        --role-name "${aws_iam_role.tableflow.name}" \
        --policy-document '${jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = [
            databricks_storage_credential.tableflow.aws_iam_role[0].unity_catalog_iam_arn,
            aws_iam_role.tableflow.arn
          ]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = databricks_storage_credential.tableflow.aws_iam_role[0].external_id
          }
        }
      }
    ]
})}'
    EOT
}

depends_on = [databricks_storage_credential.tableflow, time_sleep.role_propagation]
}

resource "time_sleep" "phase1" {
  create_duration = "60s"
  depends_on      = [terraform_data.update_trust_phase1]
}

# Phase 2: Add Confluent trust (provider integration role ARN + external ID)
resource "terraform_data" "update_trust_phase2" {
  triggers_replace = [confluent_provider_integration.aws.id]

  provisioner "local-exec" {
    command = <<-EOT
      aws iam update-assume-role-policy --output json \
        --role-name "${aws_iam_role.tableflow.name}" \
        --policy-document '${jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = [
            databricks_storage_credential.tableflow.aws_iam_role[0].unity_catalog_iam_arn,
            aws_iam_role.tableflow.arn
          ]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = databricks_storage_credential.tableflow.aws_iam_role[0].external_id
          }
        }
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

depends_on = [time_sleep.phase1, confluent_provider_integration.aws]
}

resource "time_sleep" "phase2" {
  create_duration = "30s"
  depends_on      = [terraform_data.update_trust_phase2]
}

# ═══════════════════════════════════════════════════════════════════════════════
# DATABRICKS — External Location + Catalog + Schema + Grants
# ═══════════════════════════════════════════════════════════════════════════════

resource "databricks_external_location" "tableflow" {
  name            = "${module.core.demo_prefix}-tableflow"
  url             = "s3://${aws_s3_bucket.tableflow.bucket}/"
  credential_name = databricks_storage_credential.tableflow.name
  comment         = "External location for Confluent Tableflow BYOB data"
  force_destroy   = true
  skip_validation = true

  depends_on = [time_sleep.phase2]
}

resource "databricks_catalog" "demo" {
  name          = replace(module.core.demo_prefix, "-", "_")
  storage_root  = "s3://${aws_s3_bucket.tableflow.bucket}/${replace(module.core.demo_prefix, "-", "_")}/"
  comment       = "Demo catalog for LineageBridge Tableflow integration"
  force_destroy = true

  depends_on = [databricks_external_location.tableflow]
}

resource "databricks_schema" "demo" {
  catalog_name = databricks_catalog.demo.name
  name         = replace(module.core.kafka_cluster_id, "-", "_")
  comment      = "Schema for Kafka cluster ${module.core.kafka_cluster_id}"
}

# Metastore-level grant: CREATE_EXTERNAL_METADATA enables LineageBridge's
# native-lineage push, which writes Confluent topics to the Databricks
# Lineage tab via /api/2.0/lineage-tracking/external-lineage. Without
# this grant the push falls back to the legacy TBLPROPERTIES + bridge
# table writer (and emits one WARNING per topic on PERMISSION_DENIED).
data "databricks_current_metastore" "this" {}

resource "databricks_grants" "metastore_external_metadata" {
  metastore = data.databricks_current_metastore.this.id

  grant {
    principal  = var.databricks_client_id
    privileges = ["CREATE_EXTERNAL_METADATA"]
  }
}

resource "databricks_grants" "storage_credential" {
  storage_credential = databricks_storage_credential.tableflow.id

  grant {
    principal  = var.databricks_client_id
    privileges = ["ALL_PRIVILEGES", "CREATE_EXTERNAL_LOCATION", "CREATE_EXTERNAL_TABLE", "READ_FILES", "WRITE_FILES"]
  }
}

resource "databricks_grants" "external_location" {
  external_location = databricks_external_location.tableflow.id

  grant {
    principal  = var.databricks_client_id
    privileges = ["ALL_PRIVILEGES", "CREATE_EXTERNAL_TABLE", "CREATE_EXTERNAL_VOLUME", "READ_FILES", "WRITE_FILES", "CREATE_MANAGED_STORAGE"]
  }
}

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.demo.name

  grant {
    principal  = "account users"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }

  grant {
    principal  = var.databricks_client_id
    privileges = ["ALL_PRIVILEGES", "USE_CATALOG", "CREATE_SCHEMA", "USE_SCHEMA", "CREATE_TABLE"]
  }
}

resource "databricks_grants" "schema" {
  schema = "${databricks_catalog.demo.name}.${databricks_schema.demo.name}"

  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA", "SELECT", "CREATE_TABLE", "MODIFY"]
  }
}

# Confluent Tableflow auto-creates a SECOND schema in the catalog using the
# raw cluster ID (with hyphens) — distinct from databricks_schema.demo above
# which uses underscores. The notebook job reads from this hyphen-schema (the
# only place the materialized tables actually live) and writes its derived
# `customer_order_summary` table back into it. Without an explicit grant here,
# the job fails with PERMISSION_DENIED on CREATE TABLE.
#
# We grant on the schema by name (Tableflow owns its lifecycle, not Terraform)
# and depend on health_check so the schema exists by the time the grant runs.
resource "databricks_grants" "tableflow_schema" {
  schema = "${databricks_catalog.demo.name}.${module.core.kafka_cluster_id}"

  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA", "SELECT", "CREATE_TABLE", "MODIFY"]
  }

  depends_on = [
    confluent_catalog_integration.demo,
    confluent_tableflow_topic.orders,
    confluent_tableflow_topic.customers,
    terraform_data.health_check,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# TABLEFLOW — BYOB / Delta Lake + Unity Catalog Integration
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_tableflow_topic" "orders" {
  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  display_name  = module.core.orders_topic_name
  table_formats = ["DELTA"]

  byob_aws {
    bucket_name             = aws_s3_bucket.tableflow.bucket
    provider_integration_id = time_sleep.provider_integration_destroy_delay.triggers["provider_integration_id"]
  }

  credentials {
    key    = var.confluent_tableflow_api_key
    secret = var.confluent_tableflow_api_secret
  }

  depends_on = [time_sleep.phase2]
}

resource "confluent_tableflow_topic" "customers" {
  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  display_name  = module.core.customers_topic_name
  table_formats = ["DELTA"]

  byob_aws {
    bucket_name             = aws_s3_bucket.tableflow.bucket
    provider_integration_id = time_sleep.provider_integration_destroy_delay.triggers["provider_integration_id"]
  }

  credentials {
    key    = var.confluent_tableflow_api_key
    secret = var.confluent_tableflow_api_secret
  }

  depends_on = [time_sleep.phase2]
}

resource "confluent_catalog_integration" "demo" {
  display_name = "${module.core.demo_prefix}-uc"

  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  unity {
    workspace_endpoint = var.databricks_workspace_url
    catalog_name       = databricks_catalog.demo.name
    client_id          = var.databricks_client_id
    client_secret      = var.databricks_client_secret
  }

  credentials {
    key    = var.confluent_tableflow_api_key
    secret = var.confluent_tableflow_api_secret
  }

  depends_on = [
    databricks_grants.catalog,
    databricks_grants.schema,
    confluent_tableflow_topic.orders,
    confluent_tableflow_topic.customers,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# DATABRICKS — SQL Warehouse (used by lineage-push CLI for UC table updates)
# ═══════════════════════════════════════════════════════════════════════════════

# Serverless 2X-Small with aggressive auto-stop. Sized for the lineage-push
# workload (small UC ALTER TABLE/COMMENT statements), not analytics. Without a
# warehouse, `lineage-bridge-extract --push-lineage` skips Test 3 in the UC
# integration suite.
resource "databricks_sql_endpoint" "lineage_push" {
  name             = "${module.core.demo_prefix}-warehouse"
  cluster_size     = "2X-Small"
  auto_stop_mins   = 5
  warehouse_type   = "PRO"
  enable_serverless_compute = true

  tags {
    custom_tags {
      key   = "managed-by"
      value = "lineage-bridge-demo"
    }
  }
}

# ═══════════════════════════════════════════════════════════════════════════════
# DATABRICKS — Processing Jobs
# ═══════════════════════════════════════════════════════════════════════════════

resource "databricks_notebook" "customer_order_summary" {
  path     = "/Shared/${module.core.demo_prefix}/customer_order_summary"
  language = "PYTHON"

  content_base64 = base64encode(<<-PYTHON
    # Databricks notebook: Customer Order Summary
    # Reads from Tableflow-materialized tables and produces an analytics table.

    catalog = "${databricks_catalog.demo.name}"
    schema  = "${module.core.kafka_cluster_id}"

    spark.sql(f"USE CATALOG `{catalog}`")
    spark.sql(f"USE SCHEMA `{schema}`")

    # ── Read materialized tables (from Confluent Tableflow) ──────────────
    orders    = spark.table("lineage_bridge_orders_v2")
    customers = spark.table("lineage_bridge_customers_v2")

    # ── Process: join + aggregate ────────────────────────────────────────
    from pyspark.sql import functions as F

    summary = (
        orders.alias("o")
        .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "inner")
        .groupBy(
            F.col("c.name").alias("customer_name"),
            F.col("c.country").alias("customer_country"),
        )
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("o.quantity").alias("total_quantity"),
            F.sum("o.price").alias("total_revenue"),
            F.avg("o.price").alias("avg_order_value"),
            F.max("o.created_at").alias("last_order_at"),
        )
    )

    # ── Write to a new managed UC table ──────────────────────────────────
    (
        summary.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("customer_order_summary")
    )

    print(f"Wrote {summary.count()} rows to {catalog}.{schema}.customer_order_summary")
  PYTHON
  )
}

resource "databricks_job" "customer_order_summary" {
  name = "${module.core.demo_prefix}-customer-order-summary"

  task {
    task_key = "summarize"

    notebook_task {
      notebook_path = databricks_notebook.customer_order_summary.path
    }

    environment_key = "Default"
  }

  environment {
    environment_key = "Default"

    spec {
      client = "1"
    }
  }

  schedule {
    quartz_cron_expression = "0 0/5 * ? * * *"
    timezone_id            = "UTC"
  }

  depends_on = [
    databricks_grants.catalog,
    databricks_grants.schema,
    databricks_grants.tableflow_schema,
    confluent_catalog_integration.demo,
  ]
}

# ── Pre-Destroy Cleanup ────────────────────────────────────────────────────

resource "terraform_data" "catalog_cleanup" {
  input = jsonencode({
    catalog = databricks_catalog.demo.name
    host    = var.databricks_workspace_url
    token   = var.databricks_token
  })

  provisioner "local-exec" {
    when    = destroy
    command = "bash ${path.module}/scripts/cleanup-catalog.sh"

    environment = {
      DATABRICKS_HOST  = jsondecode(self.input).host
      DATABRICKS_TOKEN = jsondecode(self.input).token
      CATALOG_NAME     = jsondecode(self.input).catalog
    }
  }

  depends_on = [
    confluent_catalog_integration.demo,
    databricks_catalog.demo,
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
      TOPIC_NAMES          = "lineage_bridge.orders_v2,lineage_bridge.customers_v2"
    }
  }

  depends_on = [confluent_catalog_integration.demo]
}
