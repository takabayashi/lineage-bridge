#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Full end-to-end demo provisioning.
#
# Handles the two-step Tableflow API key bootstrap:
#   1. First terraform apply — creates environment, cluster, SA, AWS, Databricks
#      (Tableflow topics will fail if no valid key exists yet — that's expected)
#   2. Creates a Tableflow API key via Confluent CLI
#   3. Updates terraform.tfvars with the new key
#   4. Second terraform apply — creates Tableflow topics + catalog integration
#
# Prerequisites:
#   - terraform.tfvars populated (Tableflow key can be placeholder)
#   - confluent CLI logged in (confluent login)
#   - AWS CLI configured (aws configure)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
TFVARS="$DEMO_DIR/terraform.tfvars"

cd "$DEMO_DIR"

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  LineageBridge Demo — Full Provisioning"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# ── Step 1: terraform init ─────────────────────────────────────────────────

echo "▸ Step 1/4: Initializing Terraform..."
terraform init -input=false

# ── Step 2: First apply (infra sans Tableflow) ─────────────────────────────

echo ""
echo "▸ Step 2/4: First apply — provisioning base infrastructure..."
echo "  (Tableflow topic errors are expected if key is not yet valid)"
echo ""

# Apply but don't fail on Tableflow errors — the rest of the infra will be created
terraform apply -auto-approve 2>&1 || true

# Check if we got an environment + SA
ENV_ID=$(terraform output -raw confluent_environment_id 2>/dev/null || echo "")
CLUSTER_ID=$(terraform output -raw confluent_cluster_id 2>/dev/null || echo "")
SA_ID=$(terraform state show confluent_service_account.demo 2>/dev/null | grep '^\s*id\s' | awk -F'"' '{print $2}' || echo "")

if [[ -z "$ENV_ID" || -z "$SA_ID" ]]; then
  echo "ERROR: Base infrastructure failed to provision. Check errors above."
  exit 1
fi

echo ""
echo "  Environment: $ENV_ID"
echo "  Cluster:     $CLUSTER_ID"
echo "  Service Account: $SA_ID"

# ── Step 3: Create Tableflow API key ───────────────────────────────────────

echo ""
echo "▸ Step 3/4: Creating Tableflow API key..."

# Check if Tableflow topics already exist (key is valid)
TF_ORDERS=$(terraform state show confluent_tableflow_topic.orders 2>/dev/null | head -1 || echo "")

if [[ -n "$TF_ORDERS" ]]; then
  echo "  Tableflow topics already provisioned — skipping key creation"
else
  # Create Tableflow API key via Confluent CLI
  KEY_STDERR=$(mktemp)
  set +e
  KEY_OUTPUT=$(confluent api-key create --resource tableflow \
    --service-account "$SA_ID" \
    --environment "$ENV_ID" \
    -o json 2>"$KEY_STDERR")
  KEY_STATUS=$?
  set -e

  if [[ $KEY_STATUS -ne 0 ]]; then
    echo "ERROR: 'confluent api-key create' exited $KEY_STATUS."
    echo "--- stdout ---"; echo "${KEY_OUTPUT:-<empty>}"
    echo "--- stderr ---"; cat "$KEY_STDERR"
    echo ""
    echo "Make sure you're logged in: confluent login"
    rm -f "$KEY_STDERR"
    exit 1
  fi

  if ! TF_API_KEY=$(printf '%s' "$KEY_OUTPUT" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_key'])" 2>/dev/null) \
     || ! TF_API_SECRET=$(printf '%s' "$KEY_OUTPUT" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_secret'])" 2>/dev/null); then
    echo "ERROR: could not parse api_key/api_secret from 'confluent api-key create' output."
    echo "--- stdout ---"; echo "${KEY_OUTPUT:-<empty>}"
    echo "--- stderr ---"; cat "$KEY_STDERR"
    rm -f "$KEY_STDERR"
    exit 1
  fi
  rm -f "$KEY_STDERR"

  echo "  Key created: $TF_API_KEY"

  # Update terraform.tfvars with the new key
  if [[ -f "$TFVARS" ]]; then
    # Use temp file for portability (macOS sed -i requires backup extension)
    sed "s|^confluent_tableflow_api_key.*=.*|confluent_tableflow_api_key    = \"$TF_API_KEY\"|" "$TFVARS" > "$TFVARS.tmp"
    sed "s|^confluent_tableflow_api_secret.*=.*|confluent_tableflow_api_secret = \"$TF_API_SECRET\"|" "$TFVARS.tmp" > "$TFVARS"
    rm -f "$TFVARS.tmp"
    echo "  Updated terraform.tfvars with new Tableflow key"
  fi

  # ── Step 4: Second apply (Tableflow + catalog integration) ───────────────

  echo ""
  echo "▸ Step 4/4: Second apply — provisioning Tableflow + catalog integration..."
  echo ""

  terraform apply -auto-approve
fi

# ── Generate .env ─────────────────────────────────────────────────────────

echo ""
echo "▸ Generating .env file..."

PROJECT_DIR="$(dirname "$(dirname "$DEMO_DIR")")"
ENV_FILE="$PROJECT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    backup="$ENV_FILE.backup.$(date +%Y%m%d_%H%M%S)"
    cp "$ENV_FILE" "$backup"
    echo "  Backed up existing .env to $(basename "$backup")"
fi

terraform output -raw demo_env_file 2>/dev/null \
    | sed 's/^    //' \
    > "$ENV_FILE"

echo "  Written to $ENV_FILE"

# ── Done ───────────────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  Demo provisioned successfully!"
echo ""
echo "  .env has been configured automatically."
echo "  Starting the UI..."
echo ""
echo "  Other commands:"
echo "    make extract      # run extraction CLI (headless)"
echo "    make watch        # run change-detection watcher"
echo "    make docker-ui    # run UI via Docker"
echo "    make demo-down    # tear down all infrastructure"
echo "══════════════════════════════════════════════════════════════════"
echo ""
