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

# ── AWS DataZone (optional) ───────────────────────────────────────────────
#
# Auto-detect if a domain + project already exist. Single-match auto-picks;
# multiple matches prompt; zero matches just leaves DataZone disabled (the
# demo still works — only the "Push to DataZone" button stays hidden).

echo "── AWS DataZone (optional) ─────────────────────────────────────"
echo ""

DZ_DOMAIN_ID=$(get_tfvar aws_datazone_domain_id)
DZ_PROJECT_ID=$(get_tfvar aws_datazone_project_id)

if [ -z "$DZ_DOMAIN_ID" ] && command -v aws &>/dev/null; then
    DZ_DOMAINS_JSON=$(aws datazone list-domains --region "$AWS_REGION" \
        --query 'items[?status==`AVAILABLE`].{id:id, name:name}' \
        --output json 2>/dev/null || echo "[]")
    DZ_DOMAIN_COUNT=$(echo "$DZ_DOMAINS_JSON" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
    if [ "$DZ_DOMAIN_COUNT" = "1" ]; then
        DZ_DOMAIN_ID=$(echo "$DZ_DOMAINS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")
        DZ_DOMAIN_NAME=$(echo "$DZ_DOMAINS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['name'])")
        echo "  Detected DataZone domain: $DZ_DOMAIN_ID ($DZ_DOMAIN_NAME)"
    elif [ "$DZ_DOMAIN_COUNT" -gt 1 ]; then
        echo "  Multiple DataZone domains found:"
        echo "$DZ_DOMAINS_JSON" | python3 -c "import sys,json; [print(f\"    - {d['id']}  {d['name']}\") for d in json.load(sys.stdin)]"
        echo ""
        read -rp "  DataZone domain ID (blank to skip): " DZ_DOMAIN_ID
    else
        echo "  No DataZone domains found in $AWS_REGION — skipping."
    fi
fi

if [ -n "$DZ_DOMAIN_ID" ] && [ -z "$DZ_PROJECT_ID" ] && command -v aws &>/dev/null; then
    DZ_PROJECTS_JSON=$(aws datazone list-projects --domain-identifier "$DZ_DOMAIN_ID" \
        --region "$AWS_REGION" --query 'items[].{id:id, name:name}' \
        --output json 2>/dev/null || echo "[]")
    DZ_PROJECT_COUNT=$(echo "$DZ_PROJECTS_JSON" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)

    # Prefer a project literally named `lineage-bridge` if present — that's the
    # one this demo creates / expects. Falls back to single-match auto-pick;
    # multiple unnamed matches prompt; zero matches skips DataZone entirely.
    DZ_PREFERRED_ID=$(echo "$DZ_PROJECTS_JSON" \
        | python3 -c "import sys,json; m=[p for p in json.load(sys.stdin) if p['name']=='lineage-bridge']; print(m[0]['id'] if m else '')" 2>/dev/null || echo "")

    if [ -n "$DZ_PREFERRED_ID" ]; then
        DZ_PROJECT_ID="$DZ_PREFERRED_ID"
        echo "  Detected DataZone project: $DZ_PROJECT_ID (lineage-bridge)"
    elif [ "$DZ_PROJECT_COUNT" = "1" ]; then
        DZ_PROJECT_ID=$(echo "$DZ_PROJECTS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")
        DZ_PROJECT_NAME=$(echo "$DZ_PROJECTS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['name'])")
        echo "  Detected DataZone project: $DZ_PROJECT_ID ($DZ_PROJECT_NAME)"
    elif [ "$DZ_PROJECT_COUNT" -gt 1 ]; then
        echo "  Multiple DataZone projects in $DZ_DOMAIN_ID (no project named 'lineage-bridge'):"
        echo "$DZ_PROJECTS_JSON" | python3 -c "import sys,json; [print(f\"    - {p['id']}  {p['name']}\") for p in json.load(sys.stdin)]"
        echo ""
        echo "  Tip: create one named 'lineage-bridge' to skip this prompt next time:"
        echo "       aws datazone create-project --domain-identifier $DZ_DOMAIN_ID \\"
        echo "         --name lineage-bridge --region $AWS_REGION"
        echo ""
        read -rp "  DataZone project ID (blank to skip DataZone wiring): " DZ_PROJECT_ID
    else
        echo "  No DataZone projects found in domain — skipping."
        DZ_DOMAIN_ID=""
    fi
fi

# Both must be set; otherwise wipe both so the .env stays clean.
if [ -z "$DZ_DOMAIN_ID" ] || [ -z "$DZ_PROJECT_ID" ]; then
    DZ_DOMAIN_ID=""
    DZ_PROJECT_ID=""
fi

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

# AWS DataZone (optional — leave empty to skip Push to DataZone wiring)
aws_datazone_domain_id  = "$DZ_DOMAIN_ID"
aws_datazone_project_id = "$DZ_PROJECT_ID"
EOF

echo "  Written to: $TFVARS"
echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  Setup complete! Run: make demo-up"
echo "══════════════════════════════════════════════════════════════════"
echo ""
