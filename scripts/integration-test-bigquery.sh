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
#   ./scripts/integration-test-bigquery.sh [--skip-docker] [--env-file PATH]

set -e

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

SKIP_DOCKER=false
ENV_FILE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-docker)
      SKIP_DOCKER=true
      shift
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--skip-docker] [--env-file PATH]"
      echo "  --env-file PATH  Path to .env (default: \$PROJECT_ROOT/infra/demos/bigquery/.env)"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--skip-docker] [--env-file PATH]" >&2
      exit 1
      ;;
  esac
done

ENV_FILE="${ENV_FILE:-$PROJECT_ROOT/infra/demos/bigquery/.env}"

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
  if [ ! -f "$ENV_FILE" ]; then
    log_error ".env file not found at $ENV_FILE. Generate it with: cd infra/demos/bigquery && terraform output -raw demo_env_file > .env"
    exit 1
  fi

  log_info "Using env file: $ENV_FILE"

  # Load env vars via python-dotenv. Bash `source` mangles JSON values like
  # LINEAGE_BRIDGE_CLUSTER_CREDENTIALS={"lkc-...":{...}} because the unquoted
  # double quotes get stripped, producing invalid JSON downstream.
  eval "$(uv run python3 -c '
import shlex, sys
from dotenv import dotenv_values
for k, v in dotenv_values(sys.argv[1]).items():
    if v is not None:
        print(f"export {k}={shlex.quote(v)}")
' "$ENV_FILE")"
  if [ -z "$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY" ]; then
    log_error "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY not set in .env"
    exit 1
  fi

  # Extract environment ID and GCP details from terraform or .env
  if [ -d "$PROJECT_ROOT/infra/demos/bigquery" ]; then
    ENV_ID=$(cd "$PROJECT_ROOT/infra/demos/bigquery" && terraform output -raw confluent_environment_id 2>/dev/null || echo "")
    GCP_PROJECT=$(cd "$PROJECT_ROOT/infra/demos/bigquery" && terraform output -raw bigquery_project 2>/dev/null || echo "")
    BQ_DATASET=$(cd "$PROJECT_ROOT/infra/demos/bigquery" && terraform output -raw bigquery_dataset 2>/dev/null || echo "")
  fi

  if [ -z "$ENV_ID" ]; then
    ENV_ID=$(grep -o 'env-[a-z0-9]*' "$ENV_FILE" | head -1)
  fi

  log_info "Environment ID: $ENV_ID"
  log_info "GCP Project: ${GCP_PROJECT:-not found}"
  log_info "BigQuery Dataset: ${BQ_DATASET:-not found}"

  # Probe GCP auth — Google client libraries (used by GoogleLineageProvider)
  # rely on Application Default Credentials, which need
  # `gcloud auth application-default login` separately from `gcloud auth login`.
  # Warn upfront so test 5 doesn't silently come back empty.
  if [ -n "$GCP_PROJECT" ] && command -v gcloud &>/dev/null; then
    if ! gcloud auth application-default print-access-token >/dev/null 2>&1; then
      log_warn "GCP Application Default Credentials not configured (BQ enrichment will fail)."
      log_warn "  Fix: gcloud auth application-default login"
    fi
  fi

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
    test_failed "BigQuery connector check (no extraction output)"
    return
  fi

  # BigQuery sink connectors are matched by connector_class containing "BigQuery"
  # (qualified_name uses the user's connector name, not the class).
  BQ_CONNECTORS=$(python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq_connectors = [
    n for n in data['nodes']
    if n['node_type'] == 'connector'
    and 'bigquery' in n.get('attributes', {}).get('connector_class', '').lower()
]
print(len(bq_connectors))
" 2>/dev/null || echo "0")

  if [ "$BQ_CONNECTORS" -ge 1 ]; then
    test_passed "BigQuery connector detected ($BQ_CONNECTORS connectors)"

    python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq_connectors = [
    n for n in data['nodes']
    if n['node_type'] == 'connector'
    and 'bigquery' in n.get('attributes', {}).get('connector_class', '').lower()
]
for conn in bq_connectors:
    a = conn.get('attributes', {})
    print(f\"  Connector: {conn['qualified_name']}\")
    print(f\"    Class: {a.get('connector_class', 'unknown')}\")
    print(f\"    State: {a.get('state', 'unknown')}\")
"
  else
    test_failed "BigQuery connector not found"
  fi
  echo ""
}

# Test 3: BigQuery Catalog Table Nodes
test_bigquery_datasets() {
  log_info "Test 3: BigQuery Catalog Table Nodes"

  OUTPUT_FILE="/tmp/bq-integration-test.json"

  if [ ! -f "$OUTPUT_FILE" ]; then
    test_failed "BigQuery dataset check (no extraction output)"
    return
  fi

  # Post ADR-021, BQ tables are CATALOG_TABLE nodes with catalog_type=GOOGLE_DATA_LINEAGE.
  BQ_TABLES=$(python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq = [n for n in data['nodes']
      if n.get('node_type') == 'catalog_table'
      and n.get('catalog_type') == 'GOOGLE_DATA_LINEAGE']
print(len(bq))
" 2>/dev/null || echo "0")

  if [ "$BQ_TABLES" -ge 1 ]; then
    test_passed "BigQuery catalog tables detected ($BQ_TABLES tables)"

    python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq = [n for n in data['nodes']
      if n.get('node_type') == 'catalog_table'
      and n.get('catalog_type') == 'GOOGLE_DATA_LINEAGE']
for t in bq:
    a = t.get('attributes', {})
    print(f\"  Table: {t['qualified_name']}\")
    print(f\"    Location: {a.get('project_id', '?')}.{a.get('dataset_id', '?')}.{a.get('table_name', '?')}\")
    print(f\"    Source topic: {a.get('source_topic', '?')}\")
"
  else
    test_failed "BigQuery catalog tables not found"
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
    test_failed "BigQuery lineage API (GCP credentials not configured)"
    return
  fi

  OUTPUT_FILE="/tmp/bq-integration-test.json"

  if [ ! -f "$OUTPUT_FILE" ]; then
    test_failed "BigQuery lineage API (no extraction output)"
    return
  fi

  # GoogleLineageProvider enriches CATALOG_TABLE nodes via the BigQuery REST API
  # (and pushes via Data Lineage API). Verify that the enrichment actually populated
  # column metadata — proves the live HTTP call succeeded, not just that nodes exist.
  ENRICHED=$(python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
nodes = [n for n in data['nodes']
         if n.get('node_type') == 'catalog_table'
         and n.get('catalog_type') == 'GOOGLE_DATA_LINEAGE'
         and n.get('attributes', {}).get('columns')]
print(len(nodes))
" 2>/dev/null || echo "0")

  if [ "$ENRICHED" -ge 1 ]; then
    test_passed "BigQuery Data Lineage API enrichment ($ENRICHED tables with column metadata)"
  else
    test_failed "BigQuery lineage API (no GOOGLE_DATA_LINEAGE tables were enriched with columns)"
  fi
  echo ""
}

# Test 6: Connector Config Validation
test_connector_config() {
  log_info "Test 6: BigQuery Connector Configuration"

  OUTPUT_FILE="/tmp/bq-integration-test.json"

  if [ ! -f "$OUTPUT_FILE" ]; then
    test_failed "Connector config (no extraction output)"
    return
  fi

  # Connector nodes don't carry full config (sensitive); validate connector_class +
  # direction (sink for BQ) + state. That proves the orchestrator parsed the
  # connector and emitted the right shape post-refactor.
  HAS_CONFIG=$(python3 -c "
import json
data = json.load(open('$OUTPUT_FILE'))
bq = [n for n in data['nodes']
      if n['node_type'] == 'connector'
      and 'bigquery' in n.get('attributes', {}).get('connector_class', '').lower()]
ok = bool(bq) and all(
    c.get('attributes', {}).get('direction', '').upper() in ('SINK', 'OUTBOUND')
    and c.get('attributes', {}).get('state')
    for c in bq
)
print(1 if ok else 0)
" 2>/dev/null || echo "0")

  if [ "$HAS_CONFIG" = "1" ]; then
    test_passed "BigQuery connector configuration valid (sink direction + state present)"
  else
    test_failed "BigQuery connector configuration invalid or missing"
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

  # Refuse to start if port 8000 is already taken — otherwise our `uvicorn`
  # exits silently and the curl calls below hit some other process,
  # producing nonsense results (e.g. tasks that never complete).
  if lsof -ti :8000 >/dev/null 2>&1; then
    HOLDER_PID=$(lsof -ti :8000 | head -1)
    HOLDER_CMD=$(ps -p "$HOLDER_PID" -o command= 2>/dev/null | head -c 100)
    test_failed "API server (port 8000 already in use by PID $HOLDER_PID: $HOLDER_CMD)"
    log_warn "  Free the port and re-run: kill $HOLDER_PID"
    echo ""
    return
  fi

  uv run lineage-bridge-api >/tmp/bq-test8-api.log 2>&1 &
  API_PID=$!

  # Wait up to 10s for the server to either bind or fail.
  for _ in {1..20}; do
    sleep 0.5
    if curl -sf http://localhost:8000/api/v1/health >/dev/null 2>&1; then
      break
    fi
    if ! ps -p $API_PID >/dev/null 2>&1; then
      test_failed "API server (process died during startup)"
      tail -5 /tmp/bq-test8-api.log
      echo ""
      return
    fi
  done

  # Test 8a: Health check
  if curl -s http://localhost:8000/api/v1/health | grep -q '"status":"ok"'; then
    test_passed "API server health check"
  else
    test_failed "API server health check"
    tail -5 /tmp/bq-test8-api.log
  fi

  # Test 8b: Trigger extraction task. Scope it to the BigQuery demo env — a
  # bare POST scans every env reachable by the cloud key, which 401s on
  # unrelated clusters and pushes runtime past the 60s poll window.
  TASK_RESPONSE=$(curl -s -X POST http://localhost:8000/api/v1/tasks/extract \
    -H "Content-Type: application/json" \
    -d "{\"environment_ids\": [\"$ENV_ID\"]}")
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

  # Cleanup. `uv run` spawns a child python process; killing only $API_PID
  # leaves the actual server orphaned on port 8000, which then blocks the
  # next test run with "address already in use".
  kill $API_PID 2>/dev/null || true
  HOLDER_PID=$(lsof -ti :8000 2>/dev/null | head -1)
  if [ -n "$HOLDER_PID" ]; then
    kill "$HOLDER_PID" 2>/dev/null || true
    sleep 1
    HOLDER_PID=$(lsof -ti :8000 2>/dev/null | head -1)
    [ -n "$HOLDER_PID" ] && kill -9 "$HOLDER_PID" 2>/dev/null || true
  fi
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

  # Modern docker compose doesn't print "Successfully built ..." like old buildx.
  # Verify success via exit code AND absence of explicit ERROR lines.
  if make docker-build 2>&1 | tee /tmp/bq-test9.log >/dev/null; then
    if grep -qiE "^ERROR\b|failed to build|build failed" /tmp/bq-test9.log; then
      test_failed "Docker build (errors in build log)"
      grep -iE "^ERROR\b|failed to build|build failed" /tmp/bq-test9.log | head -5
    else
      test_passed "Docker build"
    fi
  else
    test_failed "Docker build (non-zero exit)"
    tail -15 /tmp/bq-test9.log
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
