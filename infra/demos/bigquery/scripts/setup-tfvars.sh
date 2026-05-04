#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge BigQuery Demo — Credential Setup
#
# Auto-detects:
#   - Confluent Cloud API key (from .env, env vars, or creates via CLI)
#   - GCP project ID (from gcloud config)
#
# Prompts for anything it can't auto-detect.
#
# Prerequisites (auto-installed via Homebrew if missing):
#   - confluent CLI (confluent login --save)
#   - gcloud CLI (gcloud auth login)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(cd "$DEMO_DIR/../../.." && pwd)"
TFVARS="$DEMO_DIR/terraform.tfvars"

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  LineageBridge BigQuery Demo — Credential Setup"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# ── Prerequisites: CLI tools ──────────────────────────────────────────────

MISSING_CLIS=()

if ! command -v confluent &>/dev/null; then
    MISSING_CLIS+=("confluent")
fi
if ! command -v gcloud &>/dev/null; then
    MISSING_CLIS+=("gcloud")
fi

if [ ${#MISSING_CLIS[@]} -gt 0 ]; then
    echo "  Missing CLI tools: ${MISSING_CLIS[*]}"
    echo ""

    if command -v brew &>/dev/null; then
        read -rp "  Install via Homebrew? [Y/n]: " install_clis
        if [ "${install_clis:-Y}" != "n" ] && [ "${install_clis:-Y}" != "N" ]; then
            for cli in "${MISSING_CLIS[@]}"; do
                case "$cli" in
                    confluent)
                        echo "  Installing Confluent CLI..."
                        brew install confluentinc/tap/cli
                        ;;
                    gcloud)
                        echo "  Installing Google Cloud SDK..."
                        brew install google-cloud-sdk
                        ;;
                esac
            done
            echo ""
            echo "  CLIs installed. Logging in..."
            echo ""
            for cli in "${MISSING_CLIS[@]}"; do
                case "$cli" in
                    confluent)
                        echo "  ▸ Confluent Cloud login:"
                        confluent login --save || echo "  Warning: confluent login failed"
                        echo ""
                        ;;
                    gcloud)
                        echo "  ▸ GCP login:"
                        gcloud auth login || echo "  Warning: gcloud auth login failed"
                        echo ""
                        ;;
                esac
            done
        else
            echo "  Skipping. Install manually:"
            for cli in "${MISSING_CLIS[@]}"; do
                case "$cli" in
                    confluent) echo "    brew install confluentinc/tap/cli && confluent login --save" ;;
                    gcloud)    echo "    brew install google-cloud-sdk && gcloud auth login" ;;
                esac
            done
            echo ""
        fi
    else
        echo "  Homebrew not found. Install the missing CLIs manually."
        exit 1
    fi
fi

# ── Helpers ───────────────────────────────────────────────────────────────

get_tfvar() {
    local key="$1"
    if [ -f "$TFVARS" ]; then
        grep -E "^${key}\s*=" "$TFVARS" 2>/dev/null \
            | sed 's/.*= *"\(.*\)"/\1/' \
            | grep -v 'YOUR_' || true
    fi
}

prompt() {
    local var_name="$1"
    local prompt_text="$2"
    local default="$3"
    local value

    if [ -n "$default" ]; then
        read -rp "  $prompt_text [$default]: " value
        value="${value:-$default}"
    else
        read -rp "  $prompt_text: " value
    fi

    eval "$var_name=\"$value\""
}

# ── Confluent Cloud API Key ──────────────────────────────────────────────

echo "── Confluent Cloud ───────────────────────────────────────────"
echo ""

CONFLUENT_KEY=$(get_tfvar "confluent_cloud_api_key")
CONFLUENT_SECRET=$(get_tfvar "confluent_cloud_api_secret")

# Try .env file
if [ -z "$CONFLUENT_KEY" ] && [ -f "$PROJECT_DIR/.env" ]; then
    CONFLUENT_KEY=$(grep LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY "$PROJECT_DIR/.env" 2>/dev/null | sed 's/.*=//' || true)
    CONFLUENT_SECRET=$(grep LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET "$PROJECT_DIR/.env" 2>/dev/null | sed 's/.*=//' || true)
fi

# Try environment variables
if [ -z "$CONFLUENT_KEY" ]; then
    CONFLUENT_KEY="${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY:-}"
    CONFLUENT_SECRET="${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET:-}"
fi

if [ -n "$CONFLUENT_KEY" ]; then
    echo "  Found Confluent Cloud API key: $CONFLUENT_KEY"
else
    echo "  No Confluent Cloud API key found."
    echo ""
    echo "  Create one at: https://confluent.cloud/settings/api-keys"
    echo "  Or via CLI: confluent api-key create --resource cloud"
    echo ""
    prompt CONFLUENT_KEY "Confluent Cloud API key" ""
    prompt CONFLUENT_SECRET "Confluent Cloud API secret" ""
fi

echo ""

# ── GCP Project ID ───────────────────────────────────────────────────────

echo "── GCP / BigQuery ────────────────────────────────────────────"
echo ""

GCP_PROJECT=$(get_tfvar "gcp_project_id")

if [ -z "$GCP_PROJECT" ]; then
    GCP_PROJECT=$(gcloud config get project 2>/dev/null || true)
fi

if [ -n "$GCP_PROJECT" ]; then
    echo "  Detected GCP project: $GCP_PROJECT"
    read -rp "  Use this project? [Y/n]: " use_project
    if [ "${use_project:-Y}" = "n" ] || [ "${use_project:-Y}" = "N" ]; then
        prompt GCP_PROJECT "GCP project ID" ""
    fi
else
    prompt GCP_PROJECT "GCP project ID" ""
fi

# Demo suffix — pinned in tfvars so bash and terraform agree on the
# random hex used by demo_prefix and the BigQuery dataset name. Reuse
# any value already in tfvars (so re-running setup is idempotent), else
# generate a fresh 8-char hex.
DEMO_SUFFIX=$(get_tfvar "demo_suffix")
if [ -z "$DEMO_SUFFIX" ]; then
  DEMO_SUFFIX=$(openssl rand -hex 4)
fi

# BigQuery dataset — auto-derive from demo_suffix unless the user has
# explicitly pinned a different name (e.g. to share data across runs).
BQ_DATASET=$(get_tfvar "bigquery_dataset")
DEFAULT_BQ_DATASET="lineage_bridge_${DEMO_SUFFIX}"
BQ_DATASET="${BQ_DATASET:-$DEFAULT_BQ_DATASET}"
prompt BQ_DATASET "BigQuery dataset name (auto-suffixed; override only to share)" "$BQ_DATASET"

# GCP region
GCP_REGION=$(get_tfvar "gcp_region")
GCP_REGION="${GCP_REGION:-us-east1}"
prompt GCP_REGION "GCP region for Confluent cluster" "$GCP_REGION"

echo ""

# ── Write terraform.tfvars ───────────────────────────────────────────────

echo "── Writing terraform.tfvars ──────────────────────────────────"
echo ""

cat > "$TFVARS" <<EOF
# ── LineageBridge BigQuery Demo — Terraform Variables ──────────────────────
# Auto-generated by setup-tfvars.sh on $(date +%Y-%m-%d)

# Confluent Cloud
confluent_cloud_api_key    = "$CONFLUENT_KEY"
confluent_cloud_api_secret = "$CONFLUENT_SECRET"

# Demo identity — pinned so bash + terraform agree (used by 'bq mk' and
# the BigQuery sink connector to name the same dataset).
demo_suffix      = "$DEMO_SUFFIX"

# GCP / BigQuery
gcp_project_id   = "$GCP_PROJECT"
bigquery_dataset = "$BQ_DATASET"
gcp_region       = "$GCP_REGION"

# gcp_sa_key_json is auto-provisioned by provision-demo.sh
# (passed via TF_VAR_gcp_sa_key_json environment variable)
EOF

echo "  Written to: $TFVARS"
echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  Setup complete! Run: make demo-up"
echo "══════════════════════════════════════════════════════════════════"
echo ""
