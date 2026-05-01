#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge BigQuery Demo — Full Provisioning
#
# Handles GCP service account creation for the BigQuery connector:
#   1. Checks gcloud CLI + authentication
#   2. Creates a GCP service account with BigQuery permissions
#   3. Creates a JSON key and passes it to Terraform
#   4. Creates BigQuery dataset if needed
#   5. Runs terraform apply (single step — no Tableflow bootstrap needed)
#
# Prerequisites:
#   - terraform.tfvars populated (at least confluent + gcp_project_id)
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - confluent CLI logged in (confluent login)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
TFVARS="$DEMO_DIR/terraform.tfvars"
SA_KEY_FILE="$DEMO_DIR/gcp-sa-key.json"
SA_NAME="lb-demo-bigquery"

cd "$DEMO_DIR"

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  LineageBridge BigQuery Demo — Provisioning"
echo "  (Connector → BigQuery)"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# ── Prerequisites ─────────────────────────────────────────────────────────

if ! command -v gcloud &>/dev/null; then
  echo "ERROR: gcloud CLI not found."
  echo "  Install: https://cloud.google.com/sdk/docs/install"
  echo "  Or:      brew install google-cloud-sdk"
  exit 1
fi

if ! command -v terraform &>/dev/null; then
  echo "ERROR: terraform CLI not found."
  exit 1
fi

# ── Read GCP project from tfvars ──────────────────────────────────────────

GCP_PROJECT=""
if [ -f "$TFVARS" ]; then
  GCP_PROJECT=$(grep -E '^gcp_project_id' "$TFVARS" 2>/dev/null \
    | sed 's/.*= *"\(.*\)"/\1/' || true)
fi

if [ -z "$GCP_PROJECT" ]; then
  GCP_PROJECT=$(gcloud config get project 2>/dev/null || true)
fi

if [ -z "$GCP_PROJECT" ]; then
  echo "ERROR: No GCP project ID found."
  echo "  Set it in terraform.tfvars (gcp_project_id) or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

echo "  GCP Project: $GCP_PROJECT"

# ── Read BigQuery dataset from tfvars (default: lineage_bridge) ───────────

BQ_DATASET="lineage_bridge"
if [ -f "$TFVARS" ]; then
  TFVAR_DATASET=$(grep -E '^bigquery_dataset' "$TFVARS" 2>/dev/null \
    | sed 's/.*= *"\(.*\)"/\1/' || true)
  if [ -n "$TFVAR_DATASET" ]; then
    BQ_DATASET="$TFVAR_DATASET"
  fi
fi

# ── Step 1: Terraform init ────────────────────────────────────────────────

echo ""
echo "▸ Step 1/5: Initializing Terraform..."
terraform init -input=false

# ── Step 2: Create GCP Service Account ────────────────────────────────────

echo ""
echo "▸ Step 2/5: Setting up GCP service account..."

SA_EMAIL="${SA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com"

# Check if SA already exists
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$GCP_PROJECT" &>/dev/null; then
  echo "  Service account already exists: $SA_EMAIL"
else
  echo "  Creating service account: $SA_NAME"
  gcloud iam service-accounts create "$SA_NAME" \
    --project="$GCP_PROJECT" \
    --display-name="LineageBridge BigQuery Demo" \
    --description="Service account for Confluent BigQuery sink connector (demo)"

  echo "  Created: $SA_EMAIL"
fi

# Grant BigQuery roles (idempotent — won't error if already granted)
echo "  Granting BigQuery permissions..."

for ROLE in "roles/bigquery.dataEditor" "roles/bigquery.jobUser"; do
  gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="$ROLE" \
    --condition=None \
    --quiet \
    > /dev/null 2>&1
  echo "    Granted: $ROLE"
done

# ── Step 3: Create SA key ─────────────────────────────────────────────

echo ""
echo "▸ Step 3/5: Creating service account key..."

if [ -f "$SA_KEY_FILE" ]; then
  # Verify the existing key is for the right SA
  EXISTING_SA=$(python3 -c "import json; print(json.load(open('$SA_KEY_FILE')).get('client_email',''))" 2>/dev/null || true)
  if [ "$EXISTING_SA" = "$SA_EMAIL" ]; then
    echo "  Key already exists for $SA_EMAIL — reusing"
  else
    echo "  Key exists but for a different SA ($EXISTING_SA) — creating new key"
    gcloud iam service-accounts keys create "$SA_KEY_FILE" \
      --iam-account="$SA_EMAIL" \
      --project="$GCP_PROJECT"
    echo "  Key saved to: $SA_KEY_FILE"
  fi
else
  gcloud iam service-accounts keys create "$SA_KEY_FILE" \
    --iam-account="$SA_EMAIL" \
    --project="$GCP_PROJECT"
  echo "  Key saved to: $SA_KEY_FILE"
fi

# ── Step 4: Ensure BigQuery dataset exists ────────────────────────────

echo ""
echo "▸ Step 4/5: Ensuring BigQuery dataset exists..."

if bq show --project_id="$GCP_PROJECT" "$BQ_DATASET" &>/dev/null; then
  echo "  Dataset ${GCP_PROJECT}:${BQ_DATASET} already exists"
else
  echo "  Creating dataset: ${GCP_PROJECT}:${BQ_DATASET}"
  bq mk --project_id="$GCP_PROJECT" \
    --dataset \
    --location="US" \
    "$BQ_DATASET" 2>/dev/null || echo "  Warning: could not create dataset (may need manual creation)"
fi

# ── Step 5: Terraform apply ──────────────────────────────────────────

echo ""
echo "▸ Step 5/5: Applying — provisioning GCP cluster + BigQuery connectors..."
echo ""

# Pass the SA key content to Terraform via environment variable
# (avoids writing the full JSON blob into terraform.tfvars)
export TF_VAR_gcp_sa_key_json
TF_VAR_gcp_sa_key_json=$(cat "$SA_KEY_FILE")

terraform apply -auto-approve

# ── Generate .env ─────────────────────────────────────────────────────────

echo ""
echo "▸ Generating .env file..."

PROJECT_DIR="$(cd "$DEMO_DIR/../../.." && pwd)"
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

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  BigQuery Demo provisioned successfully!"
echo ""
echo "  GCP Service Account: $SA_EMAIL"
echo "  BigQuery Dataset:    ${GCP_PROJECT}:${BQ_DATASET}"
echo "  SA Key File:         $SA_KEY_FILE"
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
