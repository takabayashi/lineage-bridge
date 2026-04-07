#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Ensure Confluent Cloud API key is available for LineageBridge.
#
# Checks .env for LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY. If missing or empty,
# creates a Cloud API key via the Confluent CLI and appends it to .env.
#
# Prerequisites:
#   - `confluent` CLI installed and logged in (`confluent login`)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ENV_FILE=".env"

# ── Check if key already exists ─────────────────────────────────────────────

if [ -f "$ENV_FILE" ]; then
    key=$(grep -E '^LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '[:space:]"'"'" || true)
    secret=$(grep -E '^LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '[:space:]"'"'" || true)

    if [ -n "$key" ] && [ -n "$secret" ]; then
        exit 0
    fi
fi

# ── Also check environment variables directly ──────────────────────────────

if [ -n "${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY:-}" ] && [ -n "${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET:-}" ]; then
    exit 0
fi

# ── Key not found — provision via Confluent CLI ─────────────────────────────

echo ""
echo "  ┌──────────────────────────────────────────────────────────┐"
echo "  │  No Confluent Cloud API key found.                       │"
echo "  │  Creating one via the Confluent CLI...                   │"
echo "  └──────────────────────────────────────────────────────────┘"
echo ""

# Check confluent CLI is available
if ! command -v confluent &>/dev/null; then
    echo "  ERROR: 'confluent' CLI not found."
    echo "  Install it: https://docs.confluent.io/confluent-cli/current/install.html"
    echo "  Then run: confluent login"
    exit 1
fi

# Check logged in
if ! confluent environment list &>/dev/null; then
    echo "  ERROR: Not logged in to Confluent Cloud."
    echo "  Run: confluent login"
    exit 1
fi

# Create a Cloud API key (org-scoped, read-only for lineage extraction)
echo "  Creating Cloud API key..."
output=$(confluent api-key create --resource cloud --description "LineageBridge auto-provisioned" -o json 2>&1)

if [ $? -ne 0 ]; then
    echo "  ERROR: Failed to create API key."
    echo "  $output"
    echo ""
    echo "  You can create one manually:"
    echo "    confluent api-key create --resource cloud --description 'LineageBridge'"
    echo "  Then add to .env:"
    echo "    LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=<key>"
    echo "    LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=<secret>"
    exit 1
fi

new_key=$(echo "$output" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['api_key'])" 2>/dev/null || echo "$output" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['key'])" 2>/dev/null)
new_secret=$(echo "$output" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['api_secret'])" 2>/dev/null || echo "$output" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['secret'])" 2>/dev/null)

if [ -z "$new_key" ] || [ -z "$new_secret" ]; then
    echo "  ERROR: Could not parse API key from CLI output."
    echo "  Raw output: $output"
    exit 1
fi

# ── Write to .env ───────────────────────────────────────────────────────────

# Create .env from example if it doesn't exist
if [ ! -f "$ENV_FILE" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example "$ENV_FILE"
        echo "  Created .env from .env.example"
    else
        touch "$ENV_FILE"
    fi
fi

# Remove any existing empty key/secret lines
sed -i.bak '/^LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=$/d' "$ENV_FILE" 2>/dev/null || true
sed -i.bak '/^LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=$/d' "$ENV_FILE" 2>/dev/null || true
rm -f "$ENV_FILE.bak"

# Append the new credentials
echo "" >> "$ENV_FILE"
echo "# Auto-provisioned by LineageBridge ($(date +%Y-%m-%d))" >> "$ENV_FILE"
echo "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=$new_key" >> "$ENV_FILE"
echo "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=$new_secret" >> "$ENV_FILE"

masked="${new_key:0:4}...${new_key: -4}"
echo ""
echo "  ✓ Cloud API key created: $masked"
echo "  ✓ Saved to .env"
echo ""
