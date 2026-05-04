# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge Demo: Connector → BigQuery
#
# End-to-end lineage: Datagen → Kafka → Flink → BigQuery Sink → BigQuery
# Uses a GCP-hosted Kafka cluster (required for BigQuery connectors).
# Simplest demo — no Tableflow, no S3, no IAM.
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
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
}

# Google provider authenticates with the same SA the BQ Sink connectors use
# (the keyfile is provisioned by scripts/provision-demo.sh and passed in via
# var.gcp_sa_key_json). The SA already has roles/bigquery.dataEditor on the
# dataset since it creates tables via the connector.
provider "google" {
  project     = var.gcp_project_id
  region      = var.gcp_region
  credentials = var.gcp_sa_key_json
}

# ═══════════════════════════════════════════════════════════════════════════════
# CONFLUENT CORE (via module — GCP cluster!)
# ═══════════════════════════════════════════════════════════════════════════════

module "core" {
  source = "../modules/confluent-core"

  demo_label     = "bq"
  demo_suffix    = var.demo_suffix
  cloud_provider = "GCP"
  cloud_region   = var.gcp_region
}

# ── Dataset name: derive from demo_prefix when not explicitly set ───────────
# Bash needs to know this value before terraform apply (see setup-tfvars.sh
# pinning demo_suffix), and the connector resources need the exact same
# string. Computing it here in one place keeps them in sync.
locals {
  bigquery_dataset_name = (
    var.bigquery_dataset != "" ? var.bigquery_dataset
    : replace(module.core.demo_prefix, "-", "_")
  )
  # Demo SA created out-of-band by scripts/provision-demo.sh (SA_NAME constant
  # there). Hardcoded in both places — keep them in sync.
  gcp_sa_email = "lb-demo-bigquery@${var.gcp_project_id}.iam.gserviceaccount.com"
}

# ── Wait for datagen connectors to register schemas ─────────────────────────
# Datagen connectors need ~30-60s to start producing data and register Avro
# schemas in Schema Registry. Flink CTAS statements will fail if schemas
# don't exist yet.

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
# BIGQUERY — Sink Connectors (enriched_orders + order_stats → BigQuery)
# ═══════════════════════════════════════════════════════════════════════════════

resource "confluent_connector" "bigquery_sink_enriched" {
  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  config_nonsensitive = {
    "connector.class"          = "BigQueryStorageSink"
    "name"                     = "${module.core.demo_prefix}-bq-enriched"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = module.core.service_account_id
    "input.data.format"        = "AVRO"
    "ingestion.mode"           = "STREAMING"
    "topics"                   = "lineage_bridge.enriched_orders"
    "project"                  = var.gcp_project_id
    "datasets"                 = local.bigquery_dataset_name
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
    confluent_flink_statement.enriched_orders,
  ]
}

resource "confluent_connector" "bigquery_sink_order_stats" {
  environment {
    id = module.core.environment_id
  }

  kafka_cluster {
    id = module.core.kafka_cluster_id
  }

  config_nonsensitive = {
    "connector.class"          = "BigQueryStorageSink"
    "name"                     = "${module.core.demo_prefix}-bq-stats"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = module.core.service_account_id
    "input.data.format"        = "AVRO"
    "input.key.format"         = "AVRO"
    "ingestion.mode"           = "STREAMING"
    "topics"                   = "lineage_bridge.order_stats"
    "project"                  = var.gcp_project_id
    "datasets"                 = local.bigquery_dataset_name
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
    confluent_flink_statement.order_stats,
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
# BIGQUERY — Scheduled CTAS joining the two sink tables
# ═══════════════════════════════════════════════════════════════════════════════
# Creates a third BQ table by joining `enriched_orders` and `order_stats` on
# `order_status`. Runs every hour via the BigQuery Data Transfer Service.
#
# Why this exists: it extends the lineage chain into the GCP side so the BQ
# Lineage tab shows the full pipeline end-to-end — Datagen → Kafka → Flink →
# Sink connector → BQ table → BQ Query → joined table. The Confluent metadata
# we registered in Dataplex Catalog stays clickable on every upstream node.

# Prerequisites for this resource (API enable + SA self-actAs binding) are
# set up by scripts/provision-demo.sh using the operator's gcloud credentials,
# because the demo SA terraform runs as can't manage project-level service
# enablement or its own IAM policy without admin permissions.
resource "google_bigquery_data_transfer_config" "joined_orders_ctas" {
  project                = var.gcp_project_id
  location               = "us"
  display_name           = "${module.core.demo_prefix} joined_orders CTAS"
  data_source_id         = "scheduled_query"
  schedule               = "every 1 hours"
  destination_dataset_id = local.bigquery_dataset_name

  # NOTE: query is a plain SELECT (no CREATE TABLE DDL) — the Data Transfer
  # Service writes the result into the destination table managed by
  # destination_table_name_template + write_disposition. Using DDL here would
  # conflict with write_disposition and the run fails with INVALID_ARGUMENT.
  params = {
    query                            = <<-SQL
      SELECT
        eo.order_id,
        eo.customer_id,
        eo.customer_name,
        eo.customer_country,
        eo.product_name,
        eo.quantity,
        eo.price,
        eo.order_status,
        os.order_count    AS status_order_count,
        os.total_quantity AS status_total_quantity,
        CURRENT_TIMESTAMP() AS computed_at
      FROM `${var.gcp_project_id}.${local.bigquery_dataset_name}.lineage_bridge_enriched_orders` eo
      LEFT JOIN `${var.gcp_project_id}.${local.bigquery_dataset_name}.lineage_bridge_order_stats` os
        ON eo.order_status = os.order_status
    SQL
    destination_table_name_template = "lineage_bridge_joined_orders"
    write_disposition               = "WRITE_TRUNCATE"
  }

  service_account_name = local.gcp_sa_email

  depends_on = [
    confluent_connector.bigquery_sink_enriched,
    confluent_connector.bigquery_sink_order_stats,
  ]
}
