#!/usr/bin/env bash
# Generate .env file from Terraform outputs
# Usage: ./infra/demo/scripts/env-from-terraform.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$PROJECT_ROOT/infra"
ENV_FILE="$PROJECT_ROOT/.env"

if [ ! -d "$INFRA_DIR/.terraform" ]; then
  echo "Error: Terraform not initialized. Run 'make infra-init' first." >&2
  exit 1
fi

echo "Extracting Terraform outputs..."
cd "$INFRA_DIR"
TF_OUTPUT=$(terraform output -json)

# Helper: extract a value from the JSON output
tf_val() {
  echo "$TF_OUTPUT" | python3 -c "import sys, json; print(json.load(sys.stdin)['$1']['value'])"
}

echo "Writing $ENV_FILE ..."

cat > "$ENV_FILE" <<EOF
# Auto-generated from Terraform outputs — do not edit manually
# Generated at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Databricks
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=$(tf_val databricks_workspace_url)
LINEAGE_BRIDGE_DATABRICKS_TOKEN=$(tf_val databricks_token)

# Confluent Cloud
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=$(tf_val confluent_cloud_api_key)
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=$(tf_val confluent_cloud_api_secret)
LINEAGE_BRIDGE_CONFLUENT_KAFKA_API_KEY=$(tf_val confluent_kafka_api_key)
LINEAGE_BRIDGE_CONFLUENT_KAFKA_API_SECRET=$(tf_val confluent_kafka_api_secret)
LINEAGE_BRIDGE_CONFLUENT_KAFKA_CLUSTER_ID=$(tf_val confluent_kafka_cluster_id)
LINEAGE_BRIDGE_CONFLUENT_ENVIRONMENT_ID=$(tf_val confluent_environment_id)
LINEAGE_BRIDGE_CONFLUENT_SCHEMA_REGISTRY_URL=$(tf_val confluent_schema_registry_url)
EOF

echo "Done. Environment written to $ENV_FILE"
