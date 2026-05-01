#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Poll Confluent REST API until all demo resources are healthy.
#
# Required env vars:
#   CONFLUENT_API_KEY, CONFLUENT_API_SECRET, ENV_ID, CLUSTER_ID, TOPIC_NAMES
#
# TOPIC_NAMES is a comma-separated list (e.g. "lineage_bridge.orders,lineage_bridge.customers")
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

BASE_URL="https://api.confluent.cloud"
MAX_ATTEMPTS=60
SLEEP_SECONDS=10

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  LineageBridge Demo — Health Check"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# ── Check Tableflow topics ──────────────────────────────────────────────────

ERRORS=0
IFS=',' read -ra TOPICS <<< "${TOPIC_NAMES}"

for TOPIC in "${TOPICS[@]}"; do
  TOPIC=$(echo "${TOPIC}" | xargs)  # trim whitespace
  TF_URL="${BASE_URL}/tableflow/v1/tableflow-topics?environment=${ENV_ID}&spec.kafka_cluster=${CLUSTER_ID}"

  # Check if this topic's Tableflow entry exists and is ACTIVE
  echo -n "Checking Tableflow: ${TOPIC}..."

  for ((i=1; i<=MAX_ATTEMPTS; i++)); do
    RESPONSE=$(curl -s "${TF_URL}" \
      -u "${CONFLUENT_API_KEY}:${CONFLUENT_API_SECRET}" \
      -H "Content-Type: application/json" 2>/dev/null || echo '{"data":[]}')

    PHASE=$(echo "${RESPONSE}" | jq -r \
      ".data[] | select(.spec.display_name == \"${TOPIC}\" or .spec.topic_name == \"${TOPIC}\") | .status.phase" \
      2>/dev/null || echo "")

    if [[ "${PHASE}" == "ACTIVE" || "${PHASE}" == "RUNNING" ]]; then
      echo " ${PHASE}"
      break
    fi

    if [[ $i -eq $MAX_ATTEMPTS ]]; then
      echo " TIMEOUT (phase: ${PHASE:-unknown})"
      ERRORS=$((ERRORS + 1))
      break
    fi

    echo -n "."
    sleep "${SLEEP_SECONDS}"
  done
done

# ── Check catalog integration ───────────────────────────────────────────────

CI_URL="${BASE_URL}/tableflow/v1/catalog-integrations?environment=${ENV_ID}&spec.kafka_cluster=${CLUSTER_ID}"

echo -n "Checking catalog integration..."

for ((i=1; i<=MAX_ATTEMPTS; i++)); do
  RESPONSE=$(curl -s "${CI_URL}" \
    -u "${CONFLUENT_API_KEY}:${CONFLUENT_API_SECRET}" \
    -H "Content-Type: application/json" 2>/dev/null || echo '{"data":[]}')

  CI_STATUS=$(echo "${RESPONSE}" | jq -r '.data[0].status.phase // empty' 2>/dev/null || echo "")

  if [[ "${CI_STATUS}" == "CONNECTED" || "${CI_STATUS}" == "ACTIVE" ]]; then
    echo " ${CI_STATUS}"
    break
  fi

  if [[ $i -eq $MAX_ATTEMPTS ]]; then
    echo " TIMEOUT (status: ${CI_STATUS:-unknown})"
    ERRORS=$((ERRORS + 1))
    break
  fi

  echo -n "."
  sleep "${SLEEP_SECONDS}"
done

# ── Check Datagen connectors ────────────────────────────────────────────────

CONNECTORS_URL="${BASE_URL}/connect/v1/environments/${ENV_ID}/clusters/${CLUSTER_ID}/connectors?expand=status"

echo -n "Checking connectors..."

CONN_RESPONSE=$(curl -s "${CONNECTORS_URL}" \
  -u "${CONFLUENT_API_KEY}:${CONFLUENT_API_SECRET}" \
  -H "Content-Type: application/json" 2>/dev/null || echo "{}")

RUNNING_COUNT=$(echo "${CONN_RESPONSE}" | jq '[.[] | select(.status.connector.state == "RUNNING")] | length' 2>/dev/null || echo "0")

if [[ "${RUNNING_COUNT}" -ge 2 ]]; then
  echo " ${RUNNING_COUNT} connectors RUNNING"
else
  echo " WARNING: only ${RUNNING_COUNT} connectors running (expected 2)"
  ERRORS=$((ERRORS + 1))
fi

# ── Summary ──────────────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════════════"

if [[ $ERRORS -eq 0 ]]; then
  echo "  Demo ready!"
  echo ""
  echo "  Run LineageBridge extraction from project root:"
  echo "    uv run lineage-bridge-extract"
  echo ""
  echo "  Or start the UI:"
  echo "    uv run streamlit run lineage_bridge/ui/app.py"
else
  echo "  WARNING: ${ERRORS} check(s) failed — see above for details."
  echo "  Resources may still be provisioning. Re-run this script to check:"
  echo "    bash infra/demos/glue/scripts/wait-for-ready.sh"
fi

echo "══════════════════════════════════════════════════════════════════"
echo ""
