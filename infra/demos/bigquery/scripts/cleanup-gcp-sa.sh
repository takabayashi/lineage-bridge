#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Clean up the GCP service account created for the BigQuery demo.
#
# Deletes the service account and its keys. Safe to run multiple times.
#
# Required env vars:
#   GCP_PROJECT_ID — GCP project containing the service account
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is required}"

SA_NAME="lb-demo-bigquery"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

echo "Cleaning up GCP service account: $SA_EMAIL"

if ! gcloud iam service-accounts describe "$SA_EMAIL" --project="$GCP_PROJECT_ID" &>/dev/null; then
  echo "  Service account not found — nothing to clean up"
  exit 0
fi

# Remove IAM role bindings
for ROLE in "roles/bigquery.dataEditor" "roles/bigquery.jobUser"; do
  gcloud projects remove-iam-policy-binding "$GCP_PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="$ROLE" \
    --quiet \
    > /dev/null 2>&1 || true
  echo "  Removed: $ROLE"
done

# Delete the service account (also deletes all its keys)
gcloud iam service-accounts delete "$SA_EMAIL" \
  --project="$GCP_PROJECT_ID" \
  --quiet

echo "  Deleted service account: $SA_EMAIL"

# Remove local key file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
SA_KEY_FILE="$DEMO_DIR/gcp-sa-key.json"

if [ -f "$SA_KEY_FILE" ]; then
  rm -f "$SA_KEY_FILE"
  echo "  Removed local key file: gcp-sa-key.json"
fi

echo "GCP cleanup complete"
