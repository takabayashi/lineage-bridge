#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Interactive setup for demo terraform.tfvars
#
# Detects credentials from CLIs (confluent, aws, databricks) and environment,
# prompts for anything missing, and writes terraform.tfvars.
#
# What it auto-detects:
#   - Confluent Cloud API key (from .env, env vars, or creates via CLI)
#   - AWS account ID + region (from aws sts get-caller-identity)
#   - Tableflow API key placeholder (created later by provision-demo.sh)
#
# What it prompts for (cannot be auto-detected):
#   - Databricks workspace URL
#   - Databricks personal access token
#   - Databricks service principal client ID + secret
#
# Prerequisites:
#   - confluent CLI logged in (confluent login)
#   - AWS CLI configured (aws configure)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$(dirname "$DEMO_DIR")")"
TFVARS="$DEMO_DIR/terraform.tfvars"

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  LineageBridge Demo — Credential Setup"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# ── Helper: read existing tfvars value ──────────────────────────────────────

get_tfvar() {
    local key="$1"
    if [ -f "$TFVARS" ]; then
        grep -E "^${key}\s*=" "$TFVARS" 2>/dev/null \
            | sed 's/.*=\s*"\(.*\)"/\1/' \
            | grep -v 'YOUR_' || true
    fi
}

# ── Helper: prompt with default ─────────────────────────────────────────────

prompt() {
    local var_name="$1"
    local prompt_text="$2"
    local default="$3"
    local is_secret="${4:-false}"

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

# ═════════════════════════════════════════════════════════════════════════════
# 1. CONFLUENT CLOUD API KEY
# ═════════════════════════════════════════════════════════════════════════════

echo "▸ Confluent Cloud"
echo ""

CC_KEY=$(get_tfvar confluent_cloud_api_key)
CC_SECRET=$(get_tfvar confluent_cloud_api_secret)

# Check .env in project root
if [ -z "$CC_KEY" ] && [ -f "$PROJECT_DIR/.env" ]; then
    CC_KEY=$(grep -E '^LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=' "$PROJECT_DIR/.env" 2>/dev/null \
        | cut -d= -f2- | tr -d '[:space:]"'"'" || true)
    CC_SECRET=$(grep -E '^LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=' "$PROJECT_DIR/.env" 2>/dev/null \
        | cut -d= -f2- | tr -d '[:space:]"'"'" || true)
    if [ -n "$CC_KEY" ]; then
        echo "  Found Cloud API key in .env: ${CC_KEY:0:4}...${CC_KEY: -4}"
    fi
fi

# Check env vars
if [ -z "$CC_KEY" ] && [ -n "${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY:-}" ]; then
    CC_KEY="$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY"
    CC_SECRET="${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET:-}"
    echo "  Found Cloud API key in environment"
fi

if [ -z "$CC_KEY" ]; then
    echo "  No Cloud API key found."

    if command -v confluent &>/dev/null && confluent environment list &>/dev/null 2>&1; then
        read -rp "  Create one via Confluent CLI? [Y/n]: " create_key
        if [ "${create_key:-Y}" != "n" ] && [ "${create_key:-Y}" != "N" ]; then
            echo "  Creating Cloud API key..."
            output=$(confluent api-key create --resource cloud --description "LineageBridge demo" -o json 2>&1)
            CC_KEY=$(echo "$output" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('api_key', d.get('key','')))")
            CC_SECRET=$(echo "$output" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('api_secret', d.get('secret','')))")
            echo "  Created: ${CC_KEY:0:4}...${CC_KEY: -4}"
        fi
    fi

    if [ -z "$CC_KEY" ]; then
        prompt CC_KEY "Cloud API key" ""
        prompt CC_SECRET "Cloud API secret" "" true
    fi
fi

echo ""

# ═════════════════════════════════════════════════════════════════════════════
# 2. AWS
# ═════════════════════════════════════════════════════════════════════════════

echo "▸ AWS"
echo ""

AWS_ACCOUNT_ID=$(get_tfvar aws_account_id)
AWS_REGION=$(get_tfvar aws_region)

# Auto-detect from AWS CLI
if [ -z "$AWS_ACCOUNT_ID" ] && command -v aws &>/dev/null; then
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)
    if [ -n "$AWS_ACCOUNT_ID" ]; then
        echo "  Detected AWS account: $AWS_ACCOUNT_ID"
    fi
fi

if [ -z "$AWS_REGION" ] && command -v aws &>/dev/null; then
    AWS_REGION=$(aws configure get region 2>/dev/null || true)
    if [ -n "$AWS_REGION" ]; then
        echo "  Detected AWS region: $AWS_REGION"
    fi
fi

prompt AWS_ACCOUNT_ID "AWS account ID (12 digits)" "${AWS_ACCOUNT_ID:-}"
prompt AWS_REGION "AWS region" "${AWS_REGION:-us-east-1}"

echo ""

# ═════════════════════════════════════════════════════════════════════════════
# 3. DATABRICKS
# ═════════════════════════════════════════════════════════════════════════════

echo "▸ Databricks"
echo ""

DB_URL=$(get_tfvar databricks_workspace_url)
DB_TOKEN=$(get_tfvar databricks_token)
DB_CLIENT_ID=$(get_tfvar databricks_client_id)
DB_CLIENT_SECRET=$(get_tfvar databricks_client_secret)

HAS_DB_CLI=false
if command -v databricks &>/dev/null; then
    HAS_DB_CLI=true
fi

# ── Workspace URL ───────────────────────────────────────────────────────────

if [ -z "$DB_URL" ]; then
    # Try databricks CLI config
    if [ "$HAS_DB_CLI" = true ]; then
        DB_URL=$(databricks auth env 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    env=d.get('env',d)
    print(env.get('DATABRICKS_HOST',''))
except: pass
" 2>/dev/null || true)
    fi

    # Try env var
    if [ -z "$DB_URL" ] && [ -n "${DATABRICKS_HOST:-}" ]; then
        DB_URL="$DATABRICKS_HOST"
    fi

    if [ -n "$DB_URL" ]; then
        echo "  Detected workspace: $DB_URL"
    fi
fi

prompt DB_URL "Workspace URL (e.g. https://dbc-xxx.cloud.databricks.com)" "${DB_URL:-}"

# Ensure URL has https:// prefix
if [[ "$DB_URL" != https://* ]]; then
    DB_URL="https://$DB_URL"
fi

# ── Personal Access Token ───────────────────────────────────────────────────

if [ -z "$DB_TOKEN" ] && [ -n "${DATABRICKS_TOKEN:-}" ]; then
    DB_TOKEN="$DATABRICKS_TOKEN"
    echo "  Found token in DATABRICKS_TOKEN"
fi

if [ -z "$DB_TOKEN" ] && [ "$HAS_DB_CLI" = true ]; then
    # Try to get token from CLI auth
    DB_TOKEN=$(databricks auth env 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    env=d.get('env',d)
    print(env.get('DATABRICKS_TOKEN',''))
except: pass
" 2>/dev/null || true)
    if [ -n "$DB_TOKEN" ]; then
        echo "  Found token from databricks CLI auth"
    fi
fi

if [ -z "$DB_TOKEN" ]; then
    if [ "$HAS_DB_CLI" = true ]; then
        read -rp "  Create a personal access token via CLI? [Y/n]: " create_pat
        if [ "${create_pat:-Y}" != "n" ] && [ "${create_pat:-Y}" != "N" ]; then
            echo "  Creating PAT (lifetime: 90 days)..."
            pat_output=$(databricks tokens create --comment "LineageBridge demo" --lifetime-seconds 7776000 -o json 2>&1)
            if [ $? -eq 0 ]; then
                DB_TOKEN=$(echo "$pat_output" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('token_value', d.get('access_token','')))")
                if [ -n "$DB_TOKEN" ]; then
                    echo "  PAT created: ${DB_TOKEN:0:5}..."
                else
                    echo "  Warning: could not parse token from output"
                fi
            else
                echo "  Warning: PAT creation failed — you can enter one manually"
                echo "  $pat_output"
            fi
        fi
    fi

    if [ -z "$DB_TOKEN" ]; then
        prompt DB_TOKEN "Personal access token" "" true
    fi
fi

# ── Service Principal ──────────────────────────────────────────────────────

echo ""
echo "  Service principal (for Confluent <-> UC catalog integration):"

if [ -z "$DB_CLIENT_ID" ] && [ -n "$DB_URL" ] && [ -n "$DB_TOKEN" ]; then
    # List service principals via workspace SCIM API (works with any workspace PAT)
    sp_list=$(curl -s "${DB_URL}/api/2.0/preview/scim/v2/ServicePrincipals" \
        -H "Authorization: Bearer ${DB_TOKEN}" 2>/dev/null \
        | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    sps = d.get('Resources', [])
    # Output as JSON array with just what we need
    out = []
    for sp in sps:
        out.append({
            'display_name': sp.get('displayName', 'unnamed'),
            'application_id': sp.get('applicationId', ''),
            'active': sp.get('active', True),
            'id': sp.get('id', ''),
        })
    json.dump(out, sys.stdout)
except:
    print('[]')
" 2>/dev/null || echo "[]")

    sp_count=$(echo "$sp_list" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")

    if [ "$sp_count" -gt 0 ]; then
        echo ""
        echo "  Existing service principals:"
        echo "$sp_list" | python3 -c "
import json, sys
sps = json.load(sys.stdin)
for i, sp in enumerate(sps):
    name = sp.get('display_name', 'unnamed')
    app_id = sp.get('application_id', '')
    active = sp.get('active', True)
    status = '' if active else ' (inactive)'
    print(f'    [{i+1}] {name} — {app_id}{status}')
print(f'    [0] Create new service principal')
" 2>/dev/null

        read -rp "  Select [1-${sp_count}, or 0 to create new]: " sp_choice

        if [ "${sp_choice:-0}" = "0" ]; then
            _create_sp=true
        else
            # Select existing
            DB_CLIENT_ID=$(echo "$sp_list" | python3 -c "
import json, sys
sps = json.load(sys.stdin)
idx = int(sys.argv[1]) - 1
if 0 <= idx < len(sps):
    print(sps[idx].get('application_id', ''))
" "$sp_choice" 2>/dev/null || true)
            if [ -n "$DB_CLIENT_ID" ]; then
                echo "  Selected: $DB_CLIENT_ID"
            else
                _create_sp=true
            fi
        fi
    else
        echo "  No service principals found in workspace."
        _create_sp=true
    fi

    # Create new service principal via workspace SCIM API
    if [ "${_create_sp:-false}" = "true" ]; then
        read -rp "  Create a new service principal? [Y/n]: " confirm_create
        if [ "${confirm_create:-Y}" != "n" ] && [ "${confirm_create:-Y}" != "N" ]; then
            read -rp "  Service principal name [lineage-bridge]: " sp_name
            sp_name="${sp_name:-lineage-bridge}"
            echo "  Creating service principal '$sp_name'..."
            sp_output=$(curl -s -X POST "${DB_URL}/api/2.0/preview/scim/v2/ServicePrincipals" \
                -H "Authorization: Bearer ${DB_TOKEN}" \
                -H "Content-Type: application/json" \
                -d "{\"displayName\": \"${sp_name}\", \"active\": true}" 2>&1)
            DB_CLIENT_ID=$(echo "$sp_output" | python3 -c "import json,sys; print(json.load(sys.stdin).get('applicationId',''))" 2>/dev/null || true)
            if [ -n "$DB_CLIENT_ID" ]; then
                echo "  Created: $DB_CLIENT_ID"
            else
                echo "  Warning: creation failed"
                echo "  $sp_output" | head -5
            fi
        fi
    fi

    # Create OAuth secret for the service principal
    # This requires account-level access (accounts.cloud.databricks.com)
    if [ -n "$DB_CLIENT_ID" ] && [ -z "$DB_CLIENT_SECRET" ]; then
        echo ""
        echo "  OAuth secret creation requires account-level access."

        # Try via databricks CLI (account-level profile)
        if [ "$HAS_DB_CLI" = true ]; then
            echo "  Trying via databricks CLI..."
            if secret_output=$(databricks service-principal-secrets create "$DB_CLIENT_ID" -o json 2>&1); then
                DB_CLIENT_SECRET=$(echo "$secret_output" | python3 -c "import json,sys; print(json.load(sys.stdin).get('secret',''))" 2>/dev/null || true)
                if [ -n "$DB_CLIENT_SECRET" ]; then
                    echo "  OAuth secret created (save this — it won't be shown again)"
                fi
            fi
        fi

        # Try via account console API if CLI failed
        if [ -z "$DB_CLIENT_SECRET" ]; then
            # Extract account ID from workspace URL or ask
            ACCOUNT_ID=""
            # Try to get from workspace API
            account_info=$(curl -s "${DB_URL}/api/2.0/preview/scim/v2/Me" \
                -H "Authorization: Bearer ${DB_TOKEN}" 2>/dev/null || true)
            # The workspace doesn't directly expose account ID, so we ask
            echo ""
            echo "  Could not auto-create OAuth secret."
            echo "  Create one manually at: https://accounts.cloud.databricks.com"
            echo "    -> Service Principals -> ${DB_CLIENT_ID} -> Generate OAuth Secret"
        fi
    fi
fi

# Fall back to manual entry if anything is still missing
if [ -z "$DB_CLIENT_ID" ]; then
    echo ""
    echo "  Create a service principal at: https://accounts.cloud.databricks.com"
    prompt DB_CLIENT_ID "Service principal client ID (application_id)" ""
fi
if [ -z "$DB_CLIENT_SECRET" ]; then
    prompt DB_CLIENT_SECRET "Service principal OAuth secret" "" true
fi

echo ""

# ═════════════════════════════════════════════════════════════════════════════
# 4. TABLEFLOW API KEY (placeholder — provision-demo.sh creates it)
# ═════════════════════════════════════════════════════════════════════════════

TF_KEY=$(get_tfvar confluent_tableflow_api_key)
TF_SECRET=$(get_tfvar confluent_tableflow_api_secret)

if [ -z "$TF_KEY" ]; then
    TF_KEY="PLACEHOLDER"
    TF_SECRET="PLACEHOLDER"
    echo "  Tableflow API key will be created automatically during provisioning."
    echo ""
fi

# ═════════════════════════════════════════════════════════════════════════════
# WRITE terraform.tfvars
# ═════════════════════════════════════════════════════════════════════════════

cat > "$TFVARS" <<EOF
# ── LineageBridge Demo — Terraform Variables ─────────────────────────────────
# Auto-generated by setup-tfvars.sh on $(date +%Y-%m-%d)

# AWS
aws_region     = "${AWS_REGION}"
aws_account_id = "${AWS_ACCOUNT_ID}"

# Confluent Cloud
confluent_cloud_api_key    = "${CC_KEY}"
confluent_cloud_api_secret = "${CC_SECRET}"

# Confluent Tableflow (auto-provisioned by provision-demo.sh if placeholder)
confluent_tableflow_api_key    = "${TF_KEY}"
confluent_tableflow_api_secret = "${TF_SECRET}"

# Databricks
databricks_workspace_url = "${DB_URL}"
databricks_token         = "${DB_TOKEN}"
databricks_client_id     = "${DB_CLIENT_ID}"
databricks_client_secret = "${DB_CLIENT_SECRET}"
EOF

echo "══════════════════════════════════════════════════════════════════"
echo "  terraform.tfvars written successfully!"
echo ""
echo "  Summary:"
echo "    Confluent Cloud:  ${CC_KEY:0:4}...${CC_KEY: -4}"
echo "    AWS Account:      ${AWS_ACCOUNT_ID}"
echo "    AWS Region:       ${AWS_REGION}"
echo "    Databricks:       ${DB_URL}"
echo "    Tableflow key:    $([ "$TF_KEY" = "PLACEHOLDER" ] && echo "will be auto-created" || echo "${TF_KEY:0:4}...")"
echo ""
echo "  Next: run 'make demo-up' to provision the infrastructure."
echo "══════════════════════════════════════════════════════════════════"
echo ""
