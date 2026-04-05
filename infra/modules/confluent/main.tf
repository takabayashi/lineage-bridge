terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

locals {
  prefix = "${var.project_name}-${var.environment}"
}

# ── Environment ──────────────────────────────────────────────────────────────

resource "confluent_environment" "this" {
  display_name = "lineage-bridge-dev"

  stream_governance {
    package = "ESSENTIALS"
  }
}

# ── Kafka Cluster ────────────────────────────────────────────────────────────

resource "confluent_kafka_cluster" "this" {
  display_name = "${local.prefix}-cluster"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.aws_region

  basic {}

  environment {
    id = confluent_environment.this.id
  }
}

# ── Schema Registry ─────────────────────────────────────────────────────────

data "confluent_schema_registry_cluster" "this" {
  environment {
    id = confluent_environment.this.id
  }

  depends_on = [confluent_kafka_cluster.this]
}

# ── Service Account ─────────────────────────────────────────────────────────

resource "confluent_service_account" "lineage_bridge" {
  display_name = "lineage-bridge-sa"
  description  = "Service account for LineageBridge integration"
}

# ── Cloud API Key (for the service account) ──────────────────────────────────

resource "confluent_api_key" "cloud" {
  display_name = "${local.prefix}-cloud-api-key"
  description  = "Cloud-level API key for lineage-bridge-sa"

  owner {
    id          = confluent_service_account.lineage_bridge.id
    api_version = confluent_service_account.lineage_bridge.api_version
    kind        = confluent_service_account.lineage_bridge.kind
  }
}

# ── Cluster-Scoped API Key ───────────────────────────────────────────────────

resource "confluent_api_key" "kafka" {
  display_name = "${local.prefix}-kafka-api-key"
  description  = "Cluster-scoped API key for lineage-bridge-sa"

  owner {
    id          = confluent_service_account.lineage_bridge.id
    api_version = confluent_service_account.lineage_bridge.api_version
    kind        = confluent_service_account.lineage_bridge.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }

  depends_on = [confluent_role_binding.cluster_admin]
}

# ── Role Bindings ────────────────────────────────────────────────────────────

resource "confluent_role_binding" "cluster_admin" {
  principal   = "User:${confluent_service_account.lineage_bridge.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.this.rbac_crn
}

# ── Test Topics ──────────────────────────────────────────────────────────────

resource "confluent_kafka_topic" "orders" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  topic_name       = "orders"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_topic" "payments" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  topic_name       = "payments"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_topic" "users" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  topic_name       = "users"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_topic" "enriched_orders" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  topic_name       = "enriched-orders"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}
