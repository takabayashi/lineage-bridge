# LineageBridge Demos

Three self-contained demos, each showcasing a different data catalog integration.
All demos include Kafka topics, Flink SQL processing, and datagen source connectors.

## Demo Comparison

| | UC | Glue | BigQuery |
|---|---|---|---|
| **Catalog** | Databricks Unity Catalog | AWS Glue Data Catalog | Google BigQuery |
| **Cloud** | AWS (us-east-1) | AWS (us-east-1) | GCP (us-east1) |
| **Integration** | Tableflow (Delta) | Tableflow (Iceberg) | Sink Connector (V2) |
| **Topics** | orders, customers | orders, customers | orders, customers |
| **Flink** | enriched_orders, order_stats | enriched_orders, order_stats | enriched_orders, order_stats |
| **ksqlDB** | high_value_orders | - | - |
| **PostgreSQL** | Sink (enriched_orders) | - | - |
| **Databricks Job** | customer_order_summary | - | - |
| **Est. Cost** | ~$711/mo | ~$211/mo | ~$211/mo |
| **Resources** | ~55 | ~30 | ~22 |

## Quick Start

```bash
# Choose a demo
cd uc        # or: cd glue, cd bigquery

# Set up credentials (interactive)
make setup

# Provision infrastructure
make demo-up

# Run LineageBridge extraction
cd ../../..
uv run lineage-bridge-extract

# Start the UI
uv run streamlit run lineage_bridge/ui/app.py

# Tear down when done
cd infra/demos/uc  # or whichever demo
make demo-down
```

## Architecture

All demos share a `confluent-core` Terraform module that creates:
- Confluent Cloud environment + Kafka cluster
- Service account + RBAC (CloudClusterAdmin, EnvironmentAdmin)
- API keys (Kafka, Schema Registry, Flink)
- Topics (orders_v2, customers_v2) + Datagen source connectors
- Flink compute pool

Each demo adds catalog-specific resources on top.

## Prerequisites

- Terraform >= 1.5
- Confluent CLI (logged in: `confluent login`)
- AWS CLI (for UC/Glue demos: `aws configure`)
- GCP CLI (for BigQuery demo: `gcloud auth login`)
