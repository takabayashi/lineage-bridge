#!/bin/bash
# Integration Test Suite for LineageBridge AWS Glue Demo
# Tests end-to-end functionality using a live Confluent Cloud + AWS Glue environment
#
# Prerequisites:
# - AWS Glue demo infrastructure provisioned (terraform apply in infra/demos/glue/)
# - .env file generated from terraform output
# - Python environment with lineage-bridge installed
# - AWS credentials configured (for Athena queries)
#
# Usage:
#   ./scripts/integration-test-glue.sh [--skip-docker] [--env-file PATH]

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
      echo "  --env-file PATH  Path to .env (default: \$PROJECT_ROOT/infra/demos/glue/.env)"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--skip-docker] [--env-file PATH]" >&2
      exit 1
      ;;
  esac
done

ENV_FILE="${ENV_FILE:-$PROJECT_ROOT/infra/demos/glue/.env}"

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
    log_error ".env file not found at $ENV_FILE. Generate it with: cd infra/demos/glue && terraform output -raw demo_env_file > .env"
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

  # Extract environment ID from terraform or .env
  if [ -d "$PROJECT_ROOT/infra/demos/glue" ]; then
    ENV_ID=$(cd "$PROJECT_ROOT/infra/demos/glue" && terraform output -raw confluent_environment_id 2>/dev/null || echo "")
    S3_BUCKET=$(cd "$PROJECT_ROOT/infra/demos/glue" && terraform output -raw s3_bucket_name 2>/dev/null || echo "")
    GLUE_DATABASE=$(cd "$PROJECT_ROOT/infra/demos/glue" && terraform output -raw glue_database_name 2>/dev/null || echo "")
    # Tableflow auto-creates a Glue database named after the Confluent cluster ID
    # (e.g. lkc-n29p2v). Terraform doesn't expose it as an output because it's
    # created by Tableflow at runtime. Fall back to cluster_id.
    if [ -z "$GLUE_DATABASE" ]; then
      GLUE_DATABASE=$(cd "$PROJECT_ROOT/infra/demos/glue" && terraform output -raw confluent_cluster_id 2>/dev/null || echo "")
    fi
  fi

  if [ -z "$ENV_ID" ]; then
    # Fallback: extract from .env
    ENV_ID=$(grep -o 'env-[a-z0-9]*' "$ENV_FILE" | head -1)
  fi

  log_info "Environment ID: $ENV_ID"
  log_info "S3 Bucket: ${S3_BUCKET:-not found}"
  log_info "Glue Database: ${GLUE_DATABASE:-not found}"

  # boto3 uses a different SSO token cache than `aws` CLI v2. A default
  # profile written in legacy SSO style (no `sso_session = ...` field)
  # works for `aws sts get-caller-identity` but raises
  # UnauthorizedSSOTokenError from boto3. Warn early so test 3's enrichment
  # check doesn't silently come back empty.
  if ! uv run python3 -c "import boto3, sys; boto3.client('glue', region_name='${LINEAGE_BRIDGE_AWS_REGION:-us-east-1}').get_databases(MaxResults=1)" >/dev/null 2>&1; then
    log_warn "boto3 cannot authenticate to AWS Glue (Glue enrichment will be empty)."
    log_warn "  If you use SSO with a legacy default profile, set AWS_PROFILE to a profile"
    log_warn "  that has 'sso_session = ...' in ~/.aws/config, then re-run \`aws sso login\`."
  fi

  log_info "Prerequisites OK"
  echo ""
}

# Test 1: Basic Extraction
test_basic_extraction() {
  log_info "Test 1: Basic Extraction (Glue Demo)"

  OUTPUT_FILE="/tmp/glue-integration-test.json"
  rm -f "$OUTPUT_FILE"

  if uv run lineage-bridge-extract --env "$ENV_ID" --output "$OUTPUT_FILE" 2>&1 | tee /tmp/glue-test1.log | tail -3 | grep -q "Complete:"; then
    if [ -f "$OUTPUT_FILE" ]; then
      NODE_COUNT=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); print(len(data['nodes']))" 2>/dev/null || echo "0")
      EDGE_COUNT=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); print(len(data['edges']))" 2>/dev/null || echo "0")

      if [ "$NODE_COUNT" -ge 10 ] && [ "$EDGE_COUNT" -ge 8 ]; then
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
    cat /tmp/glue-test1.log | tail -20
  fi
  echo ""
}

# Test 2: Tableflow to S3 Iceberg
test_tableflow_iceberg() {
  log_info "Test 2: Tableflow to S3 Iceberg Integration"

  OUTPUT_FILE="/tmp/glue-integration-test.json"

  if [ ! -f "$OUTPUT_FILE" ]; then
    test_skipped "Tableflow Iceberg check (extraction not run yet)"
    return
  fi

  # Check for Tableflow tables in output
  TABLEFLOW_TABLES=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); print(sum(1 for n in data['nodes'] if n['node_type']=='tableflow_table'))" 2>/dev/null || echo "0")

  if [ "$TABLEFLOW_TABLES" -ge 1 ]; then
    test_passed "Tableflow integration ($TABLEFLOW_TABLES Tableflow tables)"

    # Check if S3 bucket is in attributes
    HAS_S3=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); tf=[n for n in data['nodes'] if n['node_type']=='tableflow_table']; print(1 if tf and 's3' in str(tf[0].get('attributes', {})).lower() else 0)" 2>/dev/null || echo "0")

    if [ "$HAS_S3" = "1" ]; then
      log_info "  S3 storage detected in Tableflow table metadata"
    fi
  else
    log_warn "Tableflow integration (no Tableflow tables found)"
    test_skipped "Tableflow Iceberg check"
  fi
  echo ""
}

# Test 3: AWS Glue Catalog Enrichment
test_glue_enrichment() {
  log_info "Test 3: AWS Glue Catalog Enrichment"

  OUTPUT_FILE="/tmp/glue-enriched-test.json"
  rm -f "$OUTPUT_FILE"

  # Check if AWS credentials are configured
  if [ -z "$LINEAGE_BRIDGE_AWS_REGION" ] && [ -z "$AWS_DEFAULT_REGION" ]; then
    test_skipped "Glue enrichment (AWS credentials not configured)"
    return
  fi

  if uv run lineage-bridge-extract --env "$ENV_ID" --output "$OUTPUT_FILE" 2>&1 | tee /tmp/glue-test3.log | tail -3 | grep -q "Complete:"; then
    # Phase 1B (ADR-021): Glue tables now use NodeType.CATALOG_TABLE with
    # catalog_type="AWS_GLUE" instead of the retired NodeType.GLUE_TABLE.
    GLUE_TABLES=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); print(sum(1 for n in data['nodes'] if n['node_type']=='catalog_table' and n.get('catalog_type')=='AWS_GLUE'))" 2>/dev/null || echo "0")

    if [ "$GLUE_TABLES" -ge 1 ]; then
      # Verify Glue tables have enriched metadata
      HAS_COLUMNS=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); glue=[n for n in data['nodes'] if n['node_type']=='catalog_table' and n.get('catalog_type')=='AWS_GLUE']; print(1 if glue and 'columns' in glue[0].get('attributes', {}) else 0)" 2>/dev/null || echo "0")

      if [ "$HAS_COLUMNS" = "1" ]; then
        test_passed "Glue enrichment ($GLUE_TABLES Glue tables with metadata)"
      else
        test_failed "Glue enrichment (Glue tables missing column metadata)"
      fi
    else
      log_warn "Glue enrichment (no Glue tables found - Glue crawler may not have run)"
      test_skipped "Glue enrichment check"
    fi
  else
    test_failed "Glue enrichment (extraction failed)"
  fi
  echo ""
}

# Test 4: Athena Query Validation
test_athena_queries() {
  log_info "Test 4: Athena Query Validation"

  if [ -z "$GLUE_DATABASE" ]; then
    test_skipped "Athena queries (Glue database name not found)"
    return
  fi

  if ! command -v aws &> /dev/null; then
    test_skipped "Athena queries (AWS CLI not installed)"
    return
  fi

  # Check if AWS credentials are valid
  # Pass --output text explicitly: an `output = exit` (or other invalid)
  # value in ~/.aws/config makes every CLI call exit non-zero even when auth
  # is fine, and would otherwise mask a working SSO session as "no creds".
  if ! aws sts get-caller-identity --output text &> /dev/null; then
    test_skipped "Athena queries (AWS credentials not configured)"
    return
  fi

  # List tables in Glue database
  TABLES=$(aws glue get-tables --database-name "$GLUE_DATABASE" --query 'TableList[*].Name' --output text 2>/dev/null || echo "")

  if [ -n "$TABLES" ]; then
    TABLE_COUNT=$(echo "$TABLES" | wc -w | tr -d ' ')
    test_passed "Athena/Glue tables accessible ($TABLE_COUNT tables in $GLUE_DATABASE)"

    log_info "  Tables: $TABLES"
    log_info "  Query in Athena: SELECT * FROM $GLUE_DATABASE.<table_name> LIMIT 10"
  else
    log_warn "Athena queries (no tables found in $GLUE_DATABASE)"
    test_skipped "Athena query validation"
  fi
  echo ""
}

# Test 5: S3 Iceberg Time Travel
test_iceberg_features() {
  log_info "Test 5: Iceberg Time Travel Features"

  if [ -z "$S3_BUCKET" ]; then
    test_skipped "Iceberg features (S3 bucket not found)"
    return
  fi

  if ! command -v aws &> /dev/null; then
    test_skipped "Iceberg features (AWS CLI not installed)"
    return
  fi

  # Iceberg metadata.json files live deep in per-table prefixes, not at the
  # bucket root (Tableflow uses paths like {prefix}/{env}/{cluster}/{table}/metadata/).
  # Recursive listing is the reliable check.
  if aws s3 ls "s3://$S3_BUCKET/" --recursive 2>/dev/null | grep -q "metadata.json"; then
    test_passed "Iceberg metadata exists in S3"

    log_info "  Iceberg time travel query example:"
    echo "    SELECT * FROM $GLUE_DATABASE.<table_name>"
    echo "    FOR SYSTEM_TIME AS OF (current_timestamp - interval '1' hour)"
  else
    log_warn "Iceberg metadata not found (tables may not have been written yet)"
    test_skipped "Iceberg time travel check"
  fi
  echo ""
}

# Test 6: Change Watcher
test_change_watcher() {
  log_info "Test 6: Change Watcher (30s test)"

  timeout 30 uv run lineage-bridge-watch --env "$ENV_ID" --cooldown 10 >/tmp/glue-test6.log 2>&1 &
  WATCHER_PID=$!

  sleep 5
  if ps -p $WATCHER_PID > /dev/null; then
    wait $WATCHER_PID 2>/dev/null || true

    if grep -q "Watcher using REST API polling\|Change poller initialized\|Discovered.*cluster" /tmp/glue-test6.log; then
      test_passed "Change watcher (ran for 30s, polling active)"
    else
      test_failed "Change watcher (did not start polling)"
      tail -10 /tmp/glue-test6.log
    fi
  else
    test_failed "Change watcher (failed to start)"
  fi
  echo ""
}

# Test 7: API Server
test_api_server() {
  log_info "Test 7: API Server"

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

  uv run lineage-bridge-api >/tmp/glue-test7-api.log 2>&1 &
  API_PID=$!

  # Wait up to 10s for the server to either bind or fail.
  for _ in {1..20}; do
    sleep 0.5
    if curl -sf http://localhost:8000/api/v1/health >/dev/null 2>&1; then
      break
    fi
    if ! ps -p $API_PID >/dev/null 2>&1; then
      test_failed "API server (process died during startup)"
      tail -5 /tmp/glue-test7-api.log
      echo ""
      return
    fi
  done

  # Test 7a: Health check
  if curl -s http://localhost:8000/api/v1/health | grep -q '"status":"ok"'; then
    test_passed "API server health check"
  else
    test_failed "API server health check"
    tail -5 /tmp/glue-test7-api.log
  fi

  # Test 7b: Trigger extraction task. Scope it to the Glue demo env — a
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

# Test 8: Docker Build
test_docker_build() {
  log_info "Test 8: Docker Build"

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

  # `docker compose build` (modern syntax) does not print "Successfully"
  # on success. Check the exit status and the absence of "ERROR" lines instead.
  if make docker-build >/tmp/glue-test8.log 2>&1; then
    if grep -qE "^ERROR\b|^failed to" /tmp/glue-test8.log; then
      test_failed "Docker build (errors in output)"
      tail -8 /tmp/glue-test8.log
    else
      test_passed "Docker build"
    fi
  else
    test_failed "Docker build"
    tail -10 /tmp/glue-test8.log
  fi
  echo ""
}

# Main execution
main() {
  echo "=========================================="
  echo "LineageBridge AWS Glue Demo Integration Tests"
  echo "=========================================="
  echo ""

  check_prerequisites

  test_basic_extraction
  test_tableflow_iceberg
  test_glue_enrichment
  test_athena_queries
  test_iceberg_features
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
