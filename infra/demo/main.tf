# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge Demo Infrastructure
#
# Self-contained demo: Kafka → Tableflow (BYOB/Delta) → S3 → Databricks UC
# Everything in one file for simplicity. Run: terraform init && terraform apply
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

# ── Providers ────────────────────────────────────────────────────────────────

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

# ── Locals ───────────────────────────────────────────────────────────────────

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  demo_prefix = "lb-demo-${random_id.suffix.hex}"
}

# ═══════════════════════════════════════════════════════════════════════════════
# CONFLUENT CLOUD
# ═══════════════════════════════════════════════════════════════════════════════

# ── Environment ──────────────────────────────────────────────────────────────

resource "confluent_environment" "demo" {
  display_name = local.demo_prefix

  stream_governance {
    package = "ESSENTIALS"
  }
}

# ── Kafka Cluster ────────────────────────────────────────────────────────────

resource "confluent_kafka_cluster" "demo" {
  display_name = local.demo_prefix
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.aws_region

  standard {}

  environment {
    id = confluent_environment.demo.id
  }
}

# ── Service Account + RBAC ───────────────────────────────────────────────────

resource "confluent_service_account" "demo" {
  display_name = "${local.demo_prefix}-sa"
  description  = "Demo service account for LineageBridge"
}

resource "confluent_role_binding" "cluster_admin" {
  principal   = "User:${confluent_service_account.demo.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.demo.rbac_crn
}

resource "confluent_role_binding" "env_admin" {
  principal   = "User:${confluent_service_account.demo.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.demo.resource_name
}

# ── API Keys ─────────────────────────────────────────────────────────────────

resource "confluent_api_key" "kafka" {
  display_name = "${local.demo_prefix}-kafka-key"
  description  = "Cluster-scoped API key for demo"

  owner {
    id          = confluent_service_account.demo.id
    api_version = confluent_service_account.demo.api_version
    kind        = confluent_service_account.demo.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.demo.id
    api_version = confluent_kafka_cluster.demo.api_version
    kind        = confluent_kafka_cluster.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.cluster_admin]
}

resource "confluent_api_key" "schema_registry" {
  display_name = "${local.demo_prefix}-sr-key"
  description  = "Schema Registry API key for demo"

  owner {
    id          = confluent_service_account.demo.id
    api_version = confluent_service_account.demo.api_version
    kind        = confluent_service_account.demo.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.demo.id
    api_version = data.confluent_schema_registry_cluster.demo.api_version
    kind        = data.confluent_schema_registry_cluster.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.env_admin]
}

# ── Schema Registry ─────────────────────────────────────────────────────────

data "confluent_schema_registry_cluster" "demo" {
  environment {
    id = confluent_environment.demo.id
  }

  depends_on = [confluent_kafka_cluster.demo]
}

# ── Kafka Topics ─────────────────────────────────────────────────────────────

resource "confluent_kafka_topic" "orders" {
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  topic_name       = "lineage_bridge.orders_v2"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_topic" "customers" {
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  topic_name       = "lineage_bridge.customers_v2"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

# ── Datagen Source Connectors ────────────────────────────────────────────────

resource "confluent_connector" "datagen_orders" {
  environment {
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  config_nonsensitive = {
    "connector.class"          = "DatagenSource"
    "name"                     = "${local.demo_prefix}-datagen-orders"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.demo.id
    "kafka.topic"              = confluent_kafka_topic.orders.topic_name
    "output.data.format"       = "AVRO"
    "schema.string"            = file("${path.module}/schemas/orders.avsc")
    "max.interval"             = "2000"
    "tasks.max"                = "1"
  }

  depends_on = [
    confluent_role_binding.cluster_admin,
  ]
}

resource "confluent_connector" "datagen_customers" {
  environment {
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  config_nonsensitive = {
    "connector.class"          = "DatagenSource"
    "name"                     = "${local.demo_prefix}-datagen-customers"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.demo.id
    "kafka.topic"              = confluent_kafka_topic.customers.topic_name
    "output.data.format"       = "AVRO"
    "schema.string"            = file("${path.module}/schemas/customers.avsc")
    "max.interval"             = "3000"
    "tasks.max"                = "1"
  }

  depends_on = [
    confluent_role_binding.cluster_admin,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# FLINK — Compute Pool + SQL Statements
# ═══════════════════════════════════════════════════════════════════════════════

# ── Flink Compute Pool ──────────────────────────────────────────────────────

resource "confluent_flink_compute_pool" "demo" {
  display_name = "${local.demo_prefix}-flink"
  cloud        = "AWS"
  region       = var.aws_region
  max_cfu      = 5

  environment {
    id = confluent_environment.demo.id
  }
}

# ── Flink API Key (scoped to Flink region) ──────────────────────────────────

resource "confluent_api_key" "flink" {
  display_name = "${local.demo_prefix}-flink-key"
  description  = "Flink API key for demo"

  owner {
    id          = confluent_service_account.demo.id
    api_version = confluent_service_account.demo.api_version
    kind        = confluent_service_account.demo.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.demo.id
    api_version = data.confluent_flink_region.demo.api_version
    kind        = data.confluent_flink_region.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.env_admin]
}

# ── Flink SQL: Drop stale tables before CTAS (idempotent re-provisioning) ───

resource "confluent_flink_statement" "drop_enriched_orders" {
  organization {
    id = data.confluent_organization.demo.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }

  principal {
    id = confluent_service_account.demo.id
  }

  statement_name = "${local.demo_prefix}-drop-enriched-orders"
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  statement = "DROP TABLE IF EXISTS `lineage_bridge.enriched_orders`;"

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.display_name
    "sql.current-database" = confluent_kafka_cluster.demo.display_name
  }

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_connector.datagen_orders,
    confluent_connector.datagen_customers,
  ]

  lifecycle {
    ignore_changes = all
  }
}

resource "confluent_flink_statement" "drop_order_stats" {
  organization {
    id = data.confluent_organization.demo.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }

  principal {
    id = confluent_service_account.demo.id
  }

  statement_name = "${local.demo_prefix}-drop-order-stats"
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  statement = "DROP TABLE IF EXISTS `lineage_bridge.order_stats`;"

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.display_name
    "sql.current-database" = confluent_kafka_cluster.demo.display_name
  }

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_connector.datagen_orders,
  ]

  lifecycle {
    ignore_changes = all
  }
}

# ── Flink SQL: Enriched Orders (CTAS — JOIN orders + customers) ─────────────

resource "confluent_flink_statement" "enriched_orders" {
  organization {
    id = data.confluent_organization.demo.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }

  principal {
    id = confluent_service_account.demo.id
  }

  statement_name = "${local.demo_prefix}-enrich-orders"
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

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
    FROM `${confluent_kafka_topic.orders.topic_name}` o
    LEFT JOIN `${confluent_kafka_topic.customers.topic_name}` c
      ON o.`customer_id` = c.`customer_id`;
  SQL

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.display_name
    "sql.current-database" = confluent_kafka_cluster.demo.display_name
  }

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_flink_statement.drop_enriched_orders,
    confluent_connector.datagen_orders,
    confluent_connector.datagen_customers,
  ]
}

# ── Flink SQL: Order Stats (CTAS — tumbling window aggregation) ─────────────

resource "confluent_flink_statement" "order_stats" {
  organization {
    id = data.confluent_organization.demo.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }

  principal {
    id = confluent_service_account.demo.id
  }

  statement_name = "${local.demo_prefix}-order-stats"
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  statement = <<-SQL
    CREATE TABLE `lineage_bridge.order_stats` AS
    SELECT
      `order_status`,
      COUNT(*)        AS `order_count`,
      SUM(`quantity`) AS `total_quantity`,
      window_start,
      window_end
    FROM TABLE(
      TUMBLE(TABLE `${confluent_kafka_topic.orders.topic_name}`, DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE)
    )
    GROUP BY `order_status`, window_start, window_end;
  SQL

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.display_name
    "sql.current-database" = confluent_kafka_cluster.demo.display_name
  }

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_flink_statement.drop_order_stats,
    confluent_connector.datagen_orders,
  ]
}

# ── Data sources (needed by Flink statements) ──────────────────────────────

data "confluent_organization" "demo" {}

data "confluent_flink_region" "demo" {
  cloud  = "AWS"
  region = var.aws_region
}

# ═══════════════════════════════════════════════════════════════════════════════
# KSQLDB — Cluster + API Key + Queries
# ═══════════════════════════════════════════════════════════════════════════════

# ── ksqlDB Cluster ──────────────────────────────────────────────────────────

resource "confluent_ksql_cluster" "demo" {
  display_name = "${local.demo_prefix}-ksqldb"
  csu          = 4

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  credential_identity {
    id = confluent_service_account.demo.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  depends_on = [
    confluent_role_binding.cluster_admin,
    confluent_role_binding.env_admin,
    confluent_api_key.schema_registry,
  ]
}

# ── ksqlDB API Key ──────────────────────────────────────────────────────────

resource "confluent_api_key" "ksqldb" {
  display_name = "${local.demo_prefix}-ksqldb-key"
  description  = "ksqlDB API key for demo"

  owner {
    id          = confluent_service_account.demo.id
    api_version = confluent_service_account.demo.api_version
    kind        = confluent_service_account.demo.kind
  }

  managed_resource {
    id          = confluent_ksql_cluster.demo.id
    api_version = confluent_ksql_cluster.demo.api_version
    kind        = confluent_ksql_cluster.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.env_admin]
}

# ── ksqlDB Queries ─────────────────────────────────────────────────────────

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
          "ksql": "CREATE STREAM IF NOT EXISTS orders_stream (order_id STRING, customer_id STRING, product_name STRING, quantity INT, price DOUBLE, order_status STRING, created_at STRING) WITH (KAFKA_TOPIC='"'"'${confluent_kafka_topic.orders.topic_name}'"'"', VALUE_FORMAT='"'"'AVRO'"'"');",
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
    confluent_connector.datagen_orders,
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
  name        = "${local.demo_prefix}-postgres"
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

  tags = { Name = "${local.demo_prefix}-postgres" }
}

resource "aws_db_subnet_group" "demo" {
  name       = "${local.demo_prefix}-postgres"
  subnet_ids = data.aws_subnets.default.ids

  tags = { Name = "${local.demo_prefix}-postgres" }
}

resource "random_password" "postgres" {
  length  = 24
  special = false
}

resource "aws_db_instance" "postgres" {
  identifier     = "${local.demo_prefix}-postgres"
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

  tags = { Name = "${local.demo_prefix}-postgres" }
}

# ── PostgreSQL Sink Connector (enriched orders → RDS) ──────────────────────

resource "confluent_connector" "postgres_sink" {
  environment {
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  config_nonsensitive = {
    "connector.class"          = "PostgresSink"
    "name"                     = "${local.demo_prefix}-postgres-sink"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.demo.id
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
    confluent_role_binding.cluster_admin,
    confluent_flink_statement.enriched_orders,
  ]
}

# ── BigQuery Sink Connector V2 (order_stats → BigQuery) ──────────────────────
#
# IMPORTANT: BigQuery connectors (BigQuerySink / BigQueryStorageSink) are ONLY
# available on GCP-hosted Kafka clusters. This demo cluster runs on AWS
# (cloud = "AWS"), so this connector is DISABLED by default.
#
# To enable: change the Kafka cluster to cloud = "GCP" and set gcp_project_id.
# The connector sinks the Flink-aggregated order_stats topic to BigQuery,
# creating a dual-path lineage:
#   order_stats → Tableflow → Glue (Iceberg)
#   order_stats → BigQuery Connector → BigQuery
#
# Reference working config: /Users/taka/projects/pocs/mb-nts/infra/connectors.tf

resource "confluent_connector" "bigquery_sink" {
  # Disabled: BigQuery connectors require GCP-hosted clusters.
  # Set to 1 when the Kafka cluster is on GCP and gcp_project_id is configured.
  count = 0

  environment {
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  config_nonsensitive = {
    "connector.class"          = "BigQueryStorageSink"
    "name"                     = "${local.demo_prefix}-bigquery-sink"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.demo.id
    "input.data.format"        = "AVRO"
    "ingestion.mode"           = "STREAMING"
    "topics"                   = "lineage_bridge.order_stats"
    "project"                  = var.gcp_project_id
    "datasets"                 = var.bigquery_dataset
    "auto.create.tables"       = "true"
    "auto.update.schemas"      = "true"
    "sanitize.topics"          = "true"
    "sanitize.field.names"     = "true"
    "tasks.max"                = "1"
  }

  config_sensitive = {
    "keyfile" = var.gcp_sa_key_json
  }

  depends_on = [
    confluent_role_binding.cluster_admin,
    confluent_flink_statement.order_stats,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# AWS — S3 Bucket + IAM Role (shared by Confluent Tableflow + Databricks UC)
# ═══════════════════════════════════════════════════════════════════════════════

# ── S3 Bucket ────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "tableflow" {
  bucket        = "${local.demo_prefix}-tableflow"
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

# ── IAM Role (single role shared by Confluent + Databricks) ─────────────────
#
# Created with self-assume only. Trust policy is updated in two phases via
# local-exec after we get the external IDs from Databricks storage credential
# and Confluent provider integration.

resource "aws_iam_role" "tableflow" {
  name = "${local.demo_prefix}-tableflow-role"

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
  display_name = "${local.demo_prefix}-aws"

  environment {
    id = confluent_environment.demo.id
  }

  aws {
    customer_role_arn = aws_iam_role.tableflow.arn
  }
}

# Confluent API returns 409 if the provider integration is deleted while
# tableflow topics or catalog integrations still reference it. Terraform
# infers destroy order from attribute refs but the API deregisters
# dependents asynchronously. This sleep runs on destroy only, after all
# dependents are destroyed but before the provider integration is.
resource "time_sleep" "provider_integration_destroy_delay" {
  destroy_duration = "60s"

  triggers = {
    provider_integration_id = confluent_provider_integration.aws.id
  }
}

# ═══════════════════════════════════════════════════════════════════════════════
# DATABRICKS — Storage Credential (needs IAM role ARN)
# ═══════════════════════════════════════════════════════════════════════════════

resource "databricks_storage_credential" "tableflow" {
  name    = "${local.demo_prefix}-tableflow"
  comment = "Shared credential for Confluent Tableflow BYOB bucket"

  aws_iam_role {
    role_arn = aws_iam_role.tableflow.arn
  }
}

# ═══════════════════════════════════════════════════════════════════════════════
# IAM TRUST POLICY — Two-Phase Update
# ═══════════════════════════════════════════════════════════════════════════════
#
# Phase 1: Add Databricks trust (storage credential external ID)
# Phase 2: Add Confluent trust (provider integration role ARN + external ID)
#
# This two-phase approach is required because:
# - The IAM role must exist before the storage credential (needs role ARN)
# - The storage credential generates an external ID we need in the trust policy
# - The provider integration generates a role ARN + external ID we need too
# - Databricks validates the trust policy when creating the external location

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
        Sid       = "SelfAssume"
        Effect    = "Allow"
        Principal = { AWS = ["arn:aws:iam::${var.aws_account_id}:root", aws_iam_role.tableflow.arn] }
        Action    = "sts:AssumeRole"
      },
      {
        Sid       = "DatabricksAssumeRole"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::414351767826:root" }
        Action    = "sts:AssumeRole"
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

depends_on = [databricks_storage_credential.tableflow]
}

resource "time_sleep" "phase1" {
  create_duration = "60s"
  depends_on      = [terraform_data.update_trust_phase1]
}

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
        Sid       = "SelfAssume"
        Effect    = "Allow"
        Principal = { AWS = ["arn:aws:iam::${var.aws_account_id}:root", aws_iam_role.tableflow.arn] }
        Action    = "sts:AssumeRole"
      },
      {
        Sid       = "DatabricksAssumeRole"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::414351767826:root" }
        Action    = "sts:AssumeRole"
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
  name            = "${local.demo_prefix}-tableflow"
  url             = "s3://${aws_s3_bucket.tableflow.bucket}/"
  credential_name = databricks_storage_credential.tableflow.name
  comment         = "External location for Confluent Tableflow BYOB data"
  force_destroy   = true
  skip_validation = true

  depends_on = [time_sleep.phase2]
}

resource "databricks_catalog" "demo" {
  name          = replace(local.demo_prefix, "-", "_")
  storage_root  = "s3://${aws_s3_bucket.tableflow.bucket}/${replace(local.demo_prefix, "-", "_")}/"
  comment       = "Demo catalog for LineageBridge Tableflow integration"
  force_destroy = true

  depends_on = [databricks_external_location.tableflow]
}

resource "databricks_schema" "demo" {
  catalog_name = databricks_catalog.demo.name
  name         = replace(confluent_kafka_cluster.demo.id, "-", "_")
  comment      = "Schema for Kafka cluster ${confluent_kafka_cluster.demo.id}"
}

# ── Grants ───────────────────────────────────────────────────────────────────

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

# ═══════════════════════════════════════════════════════════════════════════════
# TABLEFLOW TOPICS — BYOB / Delta Lake
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_tableflow_topic" "orders" {
  environment {
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  display_name  = confluent_kafka_topic.orders.topic_name
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
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  display_name  = confluent_kafka_topic.customers.topic_name
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

# ═══════════════════════════════════════════════════════════════════════════════
# CATALOG INTEGRATION — Unity Catalog
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_catalog_integration" "demo" {
  display_name = "${local.demo_prefix}-uc"

  environment {
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
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
# CATALOG INTEGRATION — AWS Glue
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_catalog_integration" "glue" {
  display_name = "${local.demo_prefix}-glue"

  environment {
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
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
    time_sleep.phase2,
  ]
}

# ── Tableflow: order_stats → Iceberg → Glue ─────────────────────────────────

resource "confluent_tableflow_topic" "order_stats" {
  environment {
    id = confluent_environment.demo.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
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
    time_sleep.phase2,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# DATABRICKS — Processing Jobs
# ═══════════════════════════════════════════════════════════════════════════════

# ── Notebook: Build customer order summary from materialized tables ────────

resource "databricks_notebook" "customer_order_summary" {
  path     = "/Shared/${local.demo_prefix}/customer_order_summary"
  language = "PYTHON"

  content_base64 = base64encode(<<-PYTHON
    # Databricks notebook: Customer Order Summary
    # Reads from Tableflow-materialized tables and produces an analytics table.

    catalog = "${databricks_catalog.demo.name}"
    schema  = "${confluent_kafka_cluster.demo.id}"

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
  name = "${local.demo_prefix}-customer-order-summary"

  # Note: run_as with a service principal requires the deploying user to have
  # the account-level "servicePrincipal.user" role, which can't be granted via
  # workspace PAT. The job runs as the PAT user instead — sufficient for demo.

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
    confluent_catalog_integration.demo,
  ]
}

# ── Pre-Destroy Cleanup ──────────────────────────────────────────────────────
#
# The catalog integration auto-creates a schema (e.g. lkc-d2y617) with tables
# inside the Databricks catalog. These are NOT managed by Terraform and block
# catalog deletion. This resource cleans them up on terraform destroy.

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

# ── Health Check ─────────────────────────────────────────────────────────────

resource "terraform_data" "health_check" {
  input = "health-check"

  provisioner "local-exec" {
    command = "bash ${path.module}/scripts/wait-for-ready.sh"

    environment = {
      CONFLUENT_API_KEY    = var.confluent_tableflow_api_key
      CONFLUENT_API_SECRET = var.confluent_tableflow_api_secret
      ENV_ID               = confluent_environment.demo.id
      CLUSTER_ID           = confluent_kafka_cluster.demo.id
      TOPIC_NAMES          = "lineage_bridge.orders_v2,lineage_bridge.customers_v2"
    }
  }

  depends_on = [confluent_catalog_integration.demo]
}
