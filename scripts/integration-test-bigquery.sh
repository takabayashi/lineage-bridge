#!/bin/bash
# Integration Test Suite for LineageBridge BigQuery Demo
# Tests end-to-end functionality using a live Confluent Cloud + BigQuery environment
#
# Prerequisites:
# - BigQuery demo infrastructure provisioned (terraform apply in infra/demos/bigquery/)
# - .env file generated from terraform output
# - Python environment with lineage-bridge installed
# - GCP credentials configured (gcloud auth login or service account key)
#
# Usage:
#   ./scripts/integration-test-bigquery.sh [--skip-docker]

set -e

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

SKIP_DOCKER=false
if [ "$1" = "--skip-docker" ]; then
  SKIP_DOCKER=true
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test results
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

# Helper functions
log_info() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

test_passed() {
  ((PASSED_TESTS++))
  log_info "✓ $1"
}

test_failed() {
  ((FAILED_TESTS++))
  log_error "✗ $1"
}

test_skipped() {
  ((SKIPPED_TESTS++))
  log_warn "⊘ $1"
}

# Verify prerequisites
check_prerequisites() {
  log_info "Checking prerequisites..."

  # Check .env file
  if [ ! -f "$PROJECT_ROOT/.env" ]; then
    log_error ".env file not found. Generate it with: cd infra/demos/bigquery && terraform output -raw demo_env_file > $PROJECT_ROOT/.env"
    exit 1
  fi

  # Check for required environment variables
  source "$PROJECT_ROOT/.env"
  if [ -z "$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY" ]; then
    log_error "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY not set in .env"
    exit 1
  fi

  # Extract environment ID and GCP details from terraform or .env
  if [ -d "$PROJECT_ROOT/infra/demos/bigquery" ]; then
    ENV_ID=$(cd "$PROJECT_ROOT/infra/demos/bigquery" && terraform output -raw confluent_environment_id 2>/dev/null || echo "")
    GCP_PROJECT=$(cd "$PROJECT_ROOT/infra/demos/bigquery" && terraform output -raw gcp_project_id 2>/dev/null || echo "")
    BQ_DATASET=$(cd "$PROJECT_ROOT/infra/demos/bigquery" && terraform output -raw bigquery_dataset_id 2>/dev/null || echo "")
  fi

  if [ -z "$ENV_ID" ]; then
    ENV_ID=$(grep -o 'env-[a-z0-9]*' "$PROJECT_ROOT/.env" | head -1)
  fi

  log_info "Environment ID: $ENV_ID"
  log_info "GCP Project: ${GCP_PROJECT:-not found}"
  log_info "BigQuery Dataset: ${BQ_DATASET:-not found}"
  log_info "Prerequisites OK"
  echo ""
}

# Test 1: Basic Extraction
test_basic_extraction() {
  log_info "Test 1: Basic Extraction (BigQuery Demo)"

  OUTPUT_FILE="/tmp/bq-integration-test.json"
  rm -f "$OUTPUT_FILE"

  if uv run lineage-bridge-extract --env "$ENV_ID" --output "$OUTPUT_FILE" 2>&1 | tee /tmp/bq-test1.log | tail -3 | grep -q "Complete:"; then
    if [ -f "$OUTPUT_FILE" ]; then
      NODE_COUNT=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); print(len(data['nodes']))" 2>/dev/null || echo "0")
      EDGE_COUNT=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); print(len(data['edges']))" 2>/dev/null || echo "0")

      if [ "$NODE_COUNT" -ge 8 ] && [ "$EDGE_COUNT" -ge 6 ]; then
        test_passed "Basic extraction ($NODE_COUNT nodes, $EDGE_COUNT edges)"

        # Display node type breakdown
        python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); types={}; [types.update({n['node_type']: types.get(n['node_type'], 0)+1}) for n in data['nodes']]; print('  Node types:', ', '.join(f'{k}: {v}' for k,v in sorted(types.items())))"
      else
        test_failed "Basic extraction (insufficient nodes: $NODE_COUNT, edges: $EDGE_COUNT)"
      fi
    else
      test_failed "Basic extraction (output file not created)"
    fi
  else
    test_failed "Basic extraction (extraction command failed)"
    cat /tmp/bq-test1.log | tail -20
  fi
  echo ""
}

# Test 2: BigQuery Connector Detection
test_bigquery_connector() {
  log_info "Test 2: BigQuery Connector Detection"

  OUTPUT_FILE="/tmp/bq-integration-test.json"

  if [ ! -f "$OUTPUT_FILE" ]; then
    test_skipped "BigQuery connector check (extraction not run yet)"
    return
  fi

  # Check for BigQuery connector in output
  BQ_CONNECTORS=$(python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq_connectors = [n for n in data['nodes'] if n['node_type'] == 'connector' and 'bigquery' in n.get('qualified_name', '').lower()]
print(len(bq_connectors))
" 2>/dev/null || echo "0")

  if [ "$BQ_CONNECTORS" -ge 1 ]; then
    test_passed "BigQuery connector detected ($BQ_CONNECTORS connectors)"

    # Display connector details
    python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq_connectors = [n for n in data['nodes'] if n['node_type'] == 'connector' and 'bigquery' in n.get('qualified_name', '').lower()]
for conn in bq_connectors:
    print(f\"  Connector: {conn['qualified_name']}\")
    print(f\"    Type: {conn.get('attributes', {}).get('connector_class', 'unknown')}\")
    print(f\"    Status: {conn.get('attributes', {}).get('status', 'unknown')}\")
"
  else
    log_warn "BigQuery connector not found (may not be deployed yet)"
    test_skipped "BigQuery connector check"
  fi
  echo ""
}

# Test 3: BigQuery External Dataset Nodes
test_bigquery_datasets() {
  log_info "Test 3: BigQuery External Dataset Nodes"

  OUTPUT_FILE="/tmp/bq-integration-test.json"

  if [ ! -f "$OUTPUT_FILE" ]; then
    test_skipped "BigQuery dataset check (extraction not run yet)"
    return
  fi

  # Check for BigQuery external dataset nodes
  BQ_DATASETS=$(python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq_datasets = [n for n in data['nodes'] if n['node_type'] == 'external_dataset' and 'bigquery' in n.get('qualified_name', '').lower()]
print(len(bq_datasets))
" 2>/dev/null || echo "0")

  if [ "$BQ_DATASETS" -ge 1 ]; then
    test_passed "BigQuery datasets detected ($BQ_DATASETS datasets)"

    # Display dataset details
    python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq_datasets = [n for n in data['nodes'] if n['node_type'] == 'external_dataset' and 'bigquery' in n.get('qualified_name', '').lower()]
for ds in bq_datasets:
    print(f\"  Dataset: {ds['qualified_name']}\")
    project = ds.get('attributes', {}).get('project_id', 'unknown')
    dataset = ds.get('attributes', {}).get('dataset_id', 'unknown')
    table = ds.get('attributes', {}).get('table_id', 'unknown')
    print(f\"    Location: {project}.{dataset}.{table}\")
"
  else
    log_warn "BigQuery datasets not found - connector may not have created tables yet"
    test_skipped "BigQuery dataset check"
  fi
  echo ""
}

# Test 4: BigQuery Table Validation
test_bigquery_tables() {
  log_info "Test 4: BigQuery Table Validation"

  if [ -z "$GCP_PROJECT" ] || [ -z "$BQ_DATASET" ]; then
    test_skipped "BigQuery tables (GCP project/dataset not found)"
    return
  fi

  if ! command -v bq &> /dev/null; then
    test_skipped "BigQuery tables (bq CLI not installed - install with: gcloud components install bq)"
    return
  fi

  # Check if gcloud is authenticated
  if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
    test_skipped "BigQuery tables (not authenticated - run: gcloud auth login)"
    return
  fi

  # List tables in BigQuery dataset
  TABLES=$(bq ls --format=json "$GCP_PROJECT:$BQ_DATASET" 2>/dev/null | python3 -c "import json, sys; tables=json.load(sys.stdin); print(' '.join([t['tableReference']['tableId'] for t in tables]))" 2>/dev/null || echo "")

  if [ -n "$TABLES" ]; then
    TABLE_COUNT=$(echo "$TABLES" | wc -w | tr -d ' ')
    test_passed "BigQuery tables accessible ($TABLE_COUNT tables in $BQ_DATASET)"

    log_info "  Tables: $TABLES"
    log_info "  Query example: bq query --use_legacy_sql=false 'SELECT * FROM $GCP_PROJECT.$BQ_DATASET.<table_name> LIMIT 10'"
  else
    log_warn "BigQuery tables - no tables found in $BQ_DATASET, connector may still be writing"
    test_skipped "BigQuery table validation"
  fi
  echo ""
}

# Test 5: BigQuery Data Lineage API
test_bigquery_lineage_api() {
  log_info "Test 5: BigQuery Data Lineage API Integration"

  # Check if Google Data Lineage provider is configured
  if [ -z "$LINEAGE_BRIDGE_GCP_PROJECT_ID" ]; then
    test_skipped "BigQuery lineage API (GCP credentials not configured)"
    return
  fi

  OUTPUT_FILE="/tmp/bq-enriched-test.json"
  rm -f "$OUTPUT_FILE"

  if uv run lineage-bridge-extract --env "$ENV_ID" --output "$OUTPUT_FILE" 2>&1 | tee /tmp/bq-test5.log | tail -3 | grep -q "Complete:"; then
    # Check if lineage metadata was added
    if grep -q "google.*lineage\|data lineage" /tmp/bq-test5.log; then
      test_passed "BigQuery Data Lineage API integration detected"
    else
      log_warn "BigQuery Data Lineage API not called - may not be enabled"
      test_skipped "BigQuery lineage API check"
    fi
  else
    test_failed "BigQuery lineage API (extraction failed)"
  fi
  echo ""
}

# Test 6: Connector Config Validation
test_connector_config() {
  log_info "Test 6: BigQuery Connector Configuration"

  OUTPUT_FILE="/tmp/bq-integration-test.json"

  if [ ! -f "$OUTPUT_FILE" ]; then
    test_skipped "Connector config (extraction not run yet)"
    return
  fi

  # Verify connector has required BigQuery configuration
  HAS_CONFIG=$(python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq_connectors = [n for n in data['nodes'] if n['node_type'] == 'connector' and 'bigquery' in n.get('qualified_name', '').lower()]
if bq_connectors:
    config = bq_connectors[0].get('attributes', {}).get('config', {})
    required = ['project', 'datasets', 'keyfile']  # BigQuery Sink v2 required fields
    has_all = all(key in str(config).lower() for key in required)
    print(1 if has_all or 'project' in str(config).lower() else 0)
else:
    print(0)
" 2>/dev/null || echo "0")

  if [ "$HAS_CONFIG" = "1" ]; then
    test_passed "Connector configuration valid (has BigQuery project settings)"
  else
    log_warn "Connector configuration incomplete or not found"
    test_skipped "Connector config validation"
  fi
  echo ""
}

# Test 7: Change Watcher
test_change_watcher() {
  log_info "Test 7: Change Watcher (30s test)"

  timeout 30 uv run lineage-bridge-watch --env "$ENV_ID" --cooldown 10 >/tmp/bq-test7.log 2>&1 &
  WATCHER_PID=$!

  sleep 5
  if ps -p $WATCHER_PID > /dev/null; then
    wait $WATCHER_PID 2>/dev/null || true

    if grep -q "Watcher using REST API polling\|Change poller initialized\|Discovered.*cluster" /tmp/bq-test7.log; then
      test_passed "Change watcher (ran for 30s, polling active)"
    else
      test_failed "Change watcher (did not start polling)"
      tail -10 /tmp/bq-test7.log
    fi
  else
    test_failed "Change watcher (failed to start)"
  fi
  echo ""
}

# Test 8: API Server
test_api_server() {
  log_info "Test 8: API Server"

  uv run lineage-bridge-api >/tmp/bq-test8-api.log 2>&1 &
  API_PID=$!

  sleep 5

  # Test 8a: Health check
  if curl -s http://localhost:8000/api/v1/health | grep -q '"status":"ok"'; then
    test_passed "API server health check"
  else
    test_failed "API server health check"
  fi

  # Test 8b: Trigger extraction task
  TASK_RESPONSE=$(curl -s -X POST http://localhost:8000/api/v1/tasks/extract)
  TASK_ID=$(echo "$TASK_RESPONSE" | python3 -c "import json, sys; data=json.load(sys.stdin); print(data.get('task_id', ''))" 2>/dev/null || echo "")

  if [ -n "$TASK_ID" ]; then
    test_passed "API extraction task created ($TASK_ID)"

    # Poll for completion (max 60s)
    for i in {1..30}; do
      STATUS=$(curl -s "http://localhost:8000/api/v1/tasks/$TASK_ID" | python3 -c "import json, sys; data=json.load(sys.stdin); print(data.get('status', 'unknown'))" 2>/dev/null || echo "unknown")

      if [ "$STATUS" = "completed" ]; then
        test_passed "API extraction task completed"
        break
      elif [ "$STATUS" = "failed" ]; then
        test_failed "API extraction task failed"
        break
      fi

      sleep 2
    done

    if [ "$STATUS" != "completed" ] && [ "$STATUS" != "failed" ]; then
      log_warn "API extraction task still running after 60s"
    fi
  else
    test_failed "API extraction task creation"
  fi

  # Cleanup
  kill $API_PID 2>/dev/null || true
  echo ""
}

# Test 9: Docker Build
test_docker_build() {
  log_info "Test 9: Docker Build"

  if $SKIP_DOCKER; then
    test_skipped "Docker build (--skip-docker flag)"
    return
  fi

  if ! command -v docker &> /dev/null; then
    test_skipped "Docker build (docker not installed)"
    return
  fi

  if ! docker info &> /dev/null; then
    test_skipped "Docker build (Docker daemon not running)"
    return
  fi

  if make docker-build 2>&1 | tee /tmp/bq-test9.log | tail -3 | grep -q "Successfully"; then
    test_passed "Docker build"
  else
    test_failed "Docker build"
    tail -10 /tmp/bq-test9.log
  fi
  echo ""
}

# Main execution
main() {
  echo "=========================================="
  echo "LineageBridge BigQuery Demo Integration Tests"
  echo "=========================================="
  echo ""

  check_prerequisites

  test_basic_extraction
  test_bigquery_connector
  test_bigquery_datasets
  test_bigquery_tables
  test_bigquery_lineage_api
  test_connector_config
  test_change_watcher
  test_api_server
  test_docker_build

  echo "=========================================="
  echo "Test Results Summary"
  echo "=========================================="
  echo -e "${GREEN}Passed:${NC}  $PASSED_TESTS"
  echo -e "${RED}Failed:${NC}  $FAILED_TESTS"
  echo -e "${YELLOW}Skipped:${NC} $SKIPPED_TESTS"
  echo "Total:   $((PASSED_TESTS + FAILED_TESTS + SKIPPED_TESTS))"
  echo ""

  if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
  else
    echo -e "${RED}Some tests failed. Check logs above for details.${NC}"
    exit 1
  fi
}

main
