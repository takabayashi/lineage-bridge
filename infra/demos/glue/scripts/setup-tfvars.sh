#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge Glue Demo — Credential Setup
#
# Auto-detects:
#   - Confluent Cloud API key (from .env, env vars, or creates via CLI)
#   - AWS account ID + region (from aws sts get-caller-identity)
#   - Tableflow API key placeholder (created later by provision-demo.sh)
#
# Prerequisites (auto-installed via Homebrew if missing):
#   - confluent CLI (confluent login --save)
#   - AWS CLI (aws sso login)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(cd "$DEMO_DIR/../../.." && pwd)"
TFVARS="$DEMO_DIR/terraform.tfvars"

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  LineageBridge Glue Demo — Credential Setup"
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
                esac
            done
        else
            echo "  Skipping. Install manually:"
            for cli in "${MISSING_CLIS[@]}"; do
                case "$cli" in
                    confluent)  echo "    brew install confluentinc/tap/cli && confluent login --save" ;;
                    aws)        echo "    brew install awscli && aws sso login" ;;
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
# ── LineageBridge Glue Demo — Terraform Variables ────────────────────────
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
EOF

echo "  Written to: $TFVARS"
echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  Setup complete! Run: make demo-up"
echo "══════════════════════════════════════════════════════════════════"
echo ""
