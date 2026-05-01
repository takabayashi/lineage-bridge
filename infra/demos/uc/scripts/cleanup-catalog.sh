#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Clean up auto-created schemas and tables from the Databricks catalog.
#
# The Confluent catalog integration auto-creates schemas (e.g. lkc-XXXXX)
# with tables inside the Databricks catalog. These are NOT managed by
# Terraform and block catalog deletion during terraform destroy.
#
# Required env vars:
#   DATABRICKS_HOST  — workspace URL (e.g. https://dbc-xxx.cloud.databricks.com)
#   DATABRICKS_TOKEN — personal access token
#   CATALOG_NAME     — catalog to clean (e.g. lineage_bridge_demo)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

: "${DATABRICKS_HOST:?DATABRICKS_HOST is required}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is required}"
: "${CATALOG_NAME:?CATALOG_NAME is required}"

echo "Cleaning up auto-created schemas in catalog: ${CATALOG_NAME}"

SCHEMAS=$(curl -s "${DATABRICKS_HOST}/api/2.1/unity-catalog/schemas?catalog_name=${CATALOG_NAME}" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
for s in data.get('schemas', []):
    if s.get('name') not in ('default', 'information_schema'):
        print(s['name'])
" 2>/dev/null || echo "")

if [[ -z "${SCHEMAS}" ]]; then
  echo "No schemas to clean up"
  exit 0
fi

for SCHEMA in ${SCHEMAS}; do
  echo "Dropping schema: ${CATALOG_NAME}.${SCHEMA}"

  TABLES=$(curl -s "${DATABRICKS_HOST}/api/2.1/unity-catalog/tables?catalog_name=${CATALOG_NAME}&schema_name=${SCHEMA}" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    | python3 -c "
import json, sys
for t in json.load(sys.stdin).get('tables', []):
    print(t['full_name'])
" 2>/dev/null || echo "")

  for TABLE in ${TABLES}; do
    echo "  Deleting table: ${TABLE}"
    curl -s -X DELETE "${DATABRICKS_HOST}/api/2.1/unity-catalog/tables/${TABLE}" \
      -H "Authorization: Bearer ${DATABRICKS_TOKEN}" > /dev/null
  done

  curl -s -X DELETE "${DATABRICKS_HOST}/api/2.1/unity-catalog/schemas/${CATALOG_NAME}.${SCHEMA}" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" > /dev/null
done

echo "Catalog cleanup complete"
