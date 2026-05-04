#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge Glue Demo — Full Provisioning
#
# Two-step Tableflow API key bootstrap (same as UC demo).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
TFVARS="$DEMO_DIR/terraform.tfvars"

cd "$DEMO_DIR"

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  LineageBridge Glue Demo — Provisioning"
echo "  (Tableflow → AWS Glue)"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# ── Step 1: terraform init ─────────────────────────────────────────────────

echo "▸ Step 1/4: Initializing Terraform..."
terraform init -input=false

# ── Step 2: First apply ───────────────────────────────────────────────────

echo ""
echo "▸ Step 2/4: First apply — provisioning base infrastructure..."
echo "  (Tableflow topic errors are expected if key is not yet valid)"
echo ""

terraform apply -auto-approve 2>&1 || true

ENV_ID=$(terraform output -raw confluent_environment_id 2>/dev/null || echo "")
CLUSTER_ID=$(terraform output -raw confluent_cluster_id 2>/dev/null || echo "")
SA_ID=$(terraform state show 'module.core.confluent_service_account.demo' 2>/dev/null | grep '^\s*id\s' | awk -F'"' '{print $2}' || echo "")

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

TF_ORDERS=$(terraform state show confluent_tableflow_topic.orders 2>/dev/null | head -1 || echo "")

if [[ -n "$TF_ORDERS" ]]; then
  echo "  Tableflow topics already provisioned — skipping key creation"
else
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

  if [[ -f "$TFVARS" ]]; then
    sed "s|^confluent_tableflow_api_key.*=.*|confluent_tableflow_api_key    = \"$TF_API_KEY\"|" "$TFVARS" > "$TFVARS.tmp"
    sed "s|^confluent_tableflow_api_secret.*=.*|confluent_tableflow_api_secret = \"$TF_API_SECRET\"|" "$TFVARS.tmp" > "$TFVARS"
    rm -f "$TFVARS.tmp"
    echo "  Updated terraform.tfvars with new Tableflow key"
  fi

  echo ""
  echo "▸ Step 4/4: Second apply — provisioning Tableflow + Glue integration..."
  echo ""

  terraform apply -auto-approve
fi

# ── Generate .env ─────────────────────────────────────────────────────────

echo ""
echo "▸ Generating .env file..."

PROJECT_DIR="$(cd "$DEMO_DIR/../../.." && pwd)"
ENV_FILE="$DEMO_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    backup="$ENV_FILE.backup.$(date +%Y%m%d_%H%M%S)"
    cp "$ENV_FILE" "$backup"
    echo "  Backed up existing .env to $(basename "$backup")"
fi

terraform output -raw demo_env_file 2>/dev/null \
    | sed 's/^    //' \
    > "$ENV_FILE"

echo "  Written to $ENV_FILE"

# Persist this demo's credentials into the local encrypted cache so the UI
# retains keys for prior demos when multiple are provisioned in sequence.
CACHE_ENV_ID=$(terraform output -raw confluent_environment_id 2>/dev/null || echo "")
CACHE_CLUSTER_ID=$(terraform output -raw confluent_cluster_id 2>/dev/null || echo "")
if [[ -n "$CACHE_ENV_ID" && -n "$CACHE_CLUSTER_ID" ]]; then
  ( cd "$PROJECT_DIR" && uv run python scripts/cache_demo_credentials.py \
      --env-file "$ENV_FILE" \
      --env-id "$CACHE_ENV_ID" \
      --cluster-id "$CACHE_CLUSTER_ID" \
      --demo-name glue ) || echo "  Warning: failed to update local credential cache"
fi

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  Glue Demo provisioned successfully!"
echo ""
echo "  Commands:"
echo "    make env          # print .env contents"
echo "    make demo-down    # tear down infrastructure"
echo ""
echo "  From the project root:"
echo "    uv run lineage-bridge-extract   # run extraction CLI"
echo "    uv run streamlit run lineage_bridge/ui/app.py  # start UI"
echo "══════════════════════════════════════════════════════════════════"
echo ""
