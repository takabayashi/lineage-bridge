# LineageBridge Demo Infrastructure

Self-contained demo that provisions a complete **Kafka -> Tableflow (BYOB/Delta) -> S3 -> Databricks Unity Catalog** pipeline with a single command.

## Architecture

```
Datagen Connectors -> Kafka Topics (Avro) -> Tableflow (BYOB/Delta) -> S3 Bucket -> Unity Catalog
  (fake e-commerce)    lineage_bridge.orders_v2        Delta Lake          shared       auto-synced
                       lineage_bridge.customers_v2      format           IAM role     external tables
```

A single IAM role is shared by both Confluent (write path) and Databricks (read path). The trust policy is built in two phases to resolve the circular dependency between the storage credential, provider integration, and IAM role.

## Prerequisites

- **Terraform** >= 1.5
- **Confluent CLI** logged in (`confluent login`)
- **AWS CLI** configured (`aws configure`) with an account that allows cross-account IAM trust
- **Confluent Cloud** account with a cloud-level API key (not cluster-scoped)
- **Databricks** workspace with Unity Catalog enabled, plus:
  - Personal access token
  - Service principal with OAuth credentials
- **jq** and **python3** installed

## Quick Start

```bash
cd infra/demo

# 1. Configure credentials
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values (see comments in the file)
# Note: confluent_tableflow_api_key/secret can be left as placeholders —
#       make demo-up will create them automatically

# 2. Provision everything (~5 min)
make demo-up

# 3. Copy .env for LineageBridge
cd ../..
make -C infra/demo env >> .env

# 4. Run extraction + UI
uv run lineage-bridge-extract
uv run streamlit run lineage_bridge/ui/app.py
```

## Teardown

```bash
# Destroy everything (single command)
make demo-down
```

This cleans up auto-created Databricks schemas/tables, then destroys all Confluent, AWS, and Databricks resources. The S3 bucket has `force_destroy = true` so Delta Lake data files are also deleted.

## Make Targets

| Target      | Description |
|-------------|-------------|
| `demo-up`   | Provision everything from scratch (init + apply + Tableflow key bootstrap) |
| `demo-down` | Tear down everything (catalog cleanup + terraform destroy) |
| `clean`     | Tear down + remove local Terraform state files |
| `status`    | Show current Terraform resources |
| `env`       | Print .env contents for LineageBridge |
| `help`      | Show available targets |

## What Gets Created

| Provider   | Resources |
|------------|-----------|
| Confluent  | Environment, Kafka cluster (Basic), 2 topics, 2 Datagen connectors, Schema Registry schemas, Provider integration, 2 Tableflow topics (BYOB/Delta), Catalog integration (Unity) |
| AWS        | S3 bucket (Delta Lake data), IAM role (shared by Confluent + Databricks), bucket policy |
| Databricks | Storage credential, external location, catalog, schema, grants |

## Data Domain

The demo generates **e-commerce** data with two Datagen source connectors:

- **lineage_bridge.orders_v2** -- order_id, customer_id, product_name, quantity, price, status, created_at
- **lineage_bridge.customers_v2** -- customer_id, name, email, country, signup_date

Records are generated every 2-3 seconds in Avro format.

## How It Works

The `make demo-up` script handles a two-step bootstrap:

1. **First `terraform apply`** -- creates environment, cluster, service account, AWS resources, and Databricks resources. Tableflow topics will fail (expected) if no valid Tableflow API key exists yet.
2. **Tableflow key creation** -- uses `confluent api-key create --resource tableflow` to create a key for the new environment, and automatically updates `terraform.tfvars`.
3. **Second `terraform apply`** -- creates Tableflow topics (BYOB/Delta), catalog integration, and runs the health check.

If the Tableflow key in `terraform.tfvars` is already valid, step 2-3 are skipped.

## Estimated Cost

~$50-80/month (Confluent basic cluster + connectors dominate; AWS S3 minimal; Databricks $0 for catalog-only resources). **Destroy when not in use.**
