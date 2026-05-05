# ─────────────────────────────────────────────────────────────────────────────
# Confluent Core Module
#
# Shared foundation for all LineageBridge demos: environment, Kafka cluster,
# service account, RBAC, API keys, topics, datagen connectors, and Flink pool.
# Parameterized by cloud_provider and cloud_region to support AWS and GCP.
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

# ── Naming ──────────────────────────────────────────────────────────────────

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  # Honour an externally-pinned suffix when the caller (e.g. BigQuery's
  # setup-tfvars.sh) needs bash to know the value before terraform apply.
  effective_suffix = var.demo_suffix != "" ? var.demo_suffix : random_id.suffix.hex
  demo_prefix      = var.demo_label != "" ? "lb-${var.demo_label}-${local.effective_suffix}" : "lb-demo-${local.effective_suffix}"
}

# ── Environment ─────────────────────────────────────────────────────────────

resource "confluent_environment" "demo" {
  display_name = local.demo_prefix

  stream_governance {
    package = "ESSENTIALS"
  }
}

# ── Kafka Cluster ───────────────────────────────────────────────────────────

resource "confluent_kafka_cluster" "demo" {
  display_name = local.demo_prefix
  availability = "SINGLE_ZONE"
  cloud        = var.cloud_provider
  region       = var.cloud_region

  standard {}

  environment {
    id = confluent_environment.demo.id
  }
}

# ── Service Account + RBAC ──────────────────────────────────────────────────

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

# ── Schema Registry ─────────────────────────────────────────────────────────

data "confluent_schema_registry_cluster" "demo" {
  environment {
    id = confluent_environment.demo.id
  }

  depends_on = [confluent_kafka_cluster.demo]
}

# ── API Keys ────────────────────────────────────────────────────────────────

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

# ── Kafka Topics ────────────────────────────────────────────────────────────

resource "confluent_kafka_topic" "orders" {
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  topic_name       = "${var.topic_prefix}.orders_v2"
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

  topic_name       = "${var.topic_prefix}.customers_v2"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

# ── Datagen Source Connectors ───────────────────────────────────────────────

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

# ── Flink Compute Pool ─────────────────────────────────────────────────────

resource "confluent_flink_compute_pool" "demo" {
  display_name = "${local.demo_prefix}-flink"
  cloud        = var.cloud_provider
  region       = var.cloud_region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.demo.id
  }
}

# ── Data Sources ────────────────────────────────────────────────────────────

data "confluent_organization" "demo" {}

data "confluent_flink_region" "demo" {
  cloud  = var.cloud_provider
  region = var.cloud_region
}
