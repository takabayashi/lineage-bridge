#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge UC Demo — Credential Setup
#
# Auto-detects:
#   - Confluent Cloud API key (from .env, env vars, or creates via CLI)
#   - AWS account ID + region (from aws sts get-caller-identity)
#   - Tableflow API key placeholder (created later by provision-demo.sh)
#
# Prompts for anything it can't auto-detect:
#   - Databricks workspace URL
#   - Databricks personal access token
#   - Databricks service principal client ID + secret
#
# Prerequisites (auto-installed via Homebrew if missing):
#   - confluent CLI (confluent login --save)
#   - AWS CLI (aws sso login)
#   - Databricks CLI (databricks configure)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(cd "$DEMO_DIR/../../.." && pwd)"
TFVARS="$DEMO_DIR/terraform.tfvars"

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  LineageBridge UC Demo — Credential Setup"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# ── Prerequisites: CLI tools ───────────────────────────────────────────────

MISSING_CLIS=()

if ! command -v confluent &>/dev/null; then
    MISSING_CLIS+=("confluent")
fi
if ! command -v aws &>/dev/null; then
    MISSING_CLIS+=("aws")
fi
if ! command -v databricks &>/dev/null; then
    MISSING_CLIS+=("databricks")
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
                    aws)
                        echo "  Installing AWS CLI..."
                        brew install awscli
                        ;;
                    databricks)
                        echo "  Installing Databricks CLI..."
                        brew install databricks
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
                    aws)
                        echo "  ▸ AWS SSO login:"
                        aws sso login || aws configure || echo "  Warning: AWS login failed"
                        echo ""
                        ;;
                    databricks)
                        echo "  ▸ Databricks login:"
                        databricks configure || echo "  Warning: databricks configure failed"
                        echo ""
                        ;;
                esac
            done
        else
            echo "  Skipping. Install manually:"
            for cli in "${MISSING_CLIS[@]}"; do
                case "$cli" in
                    confluent)  echo "    brew install confluentinc/tap/cli && confluent login --save" ;;
                    aws)        echo "    brew install awscli && aws sso login" ;;
                    databricks) echo "    brew install databricks && databricks configure" ;;
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
    local is_secret="${4:-false}"
    local value

    if [ -n "$default" ]; then
        if [ "$is_secret" = "true" ]; then
            local masked="${default:0:4}...${default: -4}"
            read -rp "  $prompt_text [$masked]: " value
        else
            read -rp "  $prompt_text [$default]: " value
        fi
        eval "$var_name=\"${value:-$default}\""
    else
        if [ "$is_secret" = "true" ]; then
            read -rsp "  $prompt_text: " value
            echo ""
        else
            read -rp "  $prompt_text: " value
        fi
        eval "$var_name=\"$value\""
    fi
}

# ── Confluent Cloud API Key ──────────────────────────────────────────────

echo "── Confluent Cloud ───────────────────────────────────────────"
echo ""

CC_KEY=$(get_tfvar confluent_cloud_api_key)
CC_SECRET=$(get_tfvar confluent_cloud_api_secret)

# Try .env file
if [ -z "$CC_KEY" ] && [ -f "$PROJECT_DIR/.env" ]; then
    CC_KEY=$(grep LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY "$PROJECT_DIR/.env" 2>/dev/null | sed 's/.*=//' || true)
    CC_SECRET=$(grep LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET "$PROJECT_DIR/.env" 2>/dev/null | sed 's/.*=//' || true)
fi

# Try environment variables
if [ -z "$CC_KEY" ]; then
    CC_KEY="${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY:-}"
    CC_SECRET="${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET:-}"
fi

if [ -n "$CC_KEY" ]; then
    echo "  Found Confluent Cloud API key: $CC_KEY"
else
    echo "  No Confluent Cloud API key found."
    echo ""
    echo "  Create one at: https://confluent.cloud/settings/api-keys"
    echo "  Or via CLI: confluent api-key create --resource cloud"
    echo ""
    prompt CC_KEY "Confluent Cloud API key" ""
    prompt CC_SECRET "Confluent Cloud API secret" "" true
fi

echo ""

# ── AWS ──────────────────────────────────────────────────────────────────

echo "── AWS ─────────────────────────────────────────────────────────"
echo ""

AWS_ACCOUNT_ID=$(get_tfvar aws_account_id)
AWS_REGION=$(get_tfvar aws_region)

if [ -z "$AWS_ACCOUNT_ID" ] && command -v aws &>/dev/null; then
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)
    if [ -n "$AWS_ACCOUNT_ID" ]; then
        echo "  Detected AWS account: $AWS_ACCOUNT_ID"
    fi
fi

if [ -z "$AWS_REGION" ] && command -v aws &>/dev/null; then
    AWS_REGION=$(aws configure get region 2>/dev/null || true)
fi

prompt AWS_ACCOUNT_ID "AWS account ID (12 digits)" "${AWS_ACCOUNT_ID:-}"
prompt AWS_REGION "AWS region" "${AWS_REGION:-us-east-1}"

echo ""

# ── Databricks ───────────────────────────────────────────────────────────

echo "── Databricks ──────────────────────────────────────────────────"
echo ""

DB_URL=$(get_tfvar databricks_workspace_url)
DB_TOKEN=$(get_tfvar databricks_token)
DB_CLIENT_ID=$(get_tfvar databricks_client_id)
DB_CLIENT_SECRET=$(get_tfvar databricks_client_secret)

# Try environment vars
if [ -z "$DB_URL" ] && [ -n "${DATABRICKS_HOST:-}" ]; then
    DB_URL="$DATABRICKS_HOST"
    echo "  Found workspace URL in DATABRICKS_HOST"
fi

if [ -z "$DB_TOKEN" ] && [ -n "${DATABRICKS_TOKEN:-}" ]; then
    DB_TOKEN="$DATABRICKS_TOKEN"
    echo "  Found token in DATABRICKS_TOKEN"
fi

prompt DB_URL "Workspace URL (e.g. https://dbc-xxx.cloud.databricks.com)" "${DB_URL:-}"

# Ensure URL has https:// prefix
if [[ "$DB_URL" != https://* ]]; then
    DB_URL="https://$DB_URL"
fi

prompt DB_TOKEN "Personal access token" "${DB_TOKEN:-}" true
prompt DB_CLIENT_ID "Service principal client ID" "${DB_CLIENT_ID:-}"
prompt DB_CLIENT_SECRET "Service principal OAuth secret" "${DB_CLIENT_SECRET:-}" true

echo ""

# ── Tableflow API key (placeholder — provision-demo.sh creates it) ───────

TF_KEY=$(get_tfvar confluent_tableflow_api_key)
TF_SECRET=$(get_tfvar confluent_tableflow_api_secret)

if [ -z "$TF_KEY" ]; then
    TF_KEY="PLACEHOLDER"
    TF_SECRET="PLACEHOLDER"
    echo "  Tableflow API key will be created automatically during provisioning."
    echo ""
fi

# ── Write terraform.tfvars ───────────────────────────────────────────────

echo "── Writing terraform.tfvars ──────────────────────────────────"
echo ""

cat > "$TFVARS" <<EOF
# ── LineageBridge UC Demo — Terraform Variables ──────────────────────────
# Auto-generated by setup-tfvars.sh on $(date +%Y-%m-%d)

# Confluent Cloud
confluent_cloud_api_key    = "$CC_KEY"
confluent_cloud_api_secret = "$CC_SECRET"

# Confluent Tableflow (auto-provisioned by provision-demo.sh if placeholder)
confluent_tableflow_api_key    = "$TF_KEY"
confluent_tableflow_api_secret = "$TF_SECRET"

# AWS
aws_region     = "$AWS_REGION"
aws_account_id = "$AWS_ACCOUNT_ID"

# Databricks
databricks_workspace_url = "$DB_URL"
databricks_token         = "$DB_TOKEN"
databricks_client_id     = "$DB_CLIENT_ID"
databricks_client_secret = "$DB_CLIENT_SECRET"
EOF

echo "  Written to: $TFVARS"
echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  Setup complete! Run: make demo-up"
echo "══════════════════════════════════════════════════════════════════"
echo ""
