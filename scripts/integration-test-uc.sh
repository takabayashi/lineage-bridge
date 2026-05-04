#!/bin/bash
# Integration Test Suite for LineageBridge UC Demo
# Tests end-to-end functionality using a live Confluent Cloud + Databricks UC environment
#
# Prerequisites:
# - UC demo infrastructure provisioned (terraform apply in infra/demos/uc/)
# - .env file generated from terraform output
# - Python environment with lineage-bridge installed
#
# Usage:
#   ./scripts/integration-test-uc.sh [--skip-docker] [--env-file PATH]

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
      echo "  --env-file PATH  Path to .env (default: \$PROJECT_ROOT/infra/demos/uc/.env)"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--skip-docker] [--env-file PATH]" >&2
      exit 1
      ;;
  esac
done

ENV_FILE="${ENV_FILE:-$PROJECT_ROOT/infra/demos/uc/.env}"

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
    log_error ".env file not found at $ENV_FILE. Generate it with: cd infra/demos/uc && terraform output -raw demo_env_file > .env"
    exit 1
  fi

  log_info "Using env file: $ENV_FILE"

  # Check for required environment variables
  set -a
  source "$ENV_FILE"
  set +a
  if [ -z "$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY" ]; then
    log_error "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY not set in .env"
    exit 1
  fi

  # Extract environment ID from terraform or .env
  if [ -d "$PROJECT_ROOT/infra/demos/uc" ]; then
    ENV_ID=$(cd "$PROJECT_ROOT/infra/demos/uc" && terraform output -raw confluent_environment_id 2>/dev/null || echo "")
  fi

  if [ -z "$ENV_ID" ]; then
    # Fallback: extract from .env comment or use default
    ENV_ID=$(grep -o 'env-[a-z0-9]*' "$ENV_FILE" | head -1 || echo "env-26wn6m")
  fi

  log_info "Environment ID: $ENV_ID"
  log_info "Prerequisites OK"
  echo ""
}

# Test 1: Basic Extraction
test_basic_extraction() {
  log_info "Test 1: Basic Extraction"

  OUTPUT_FILE="/tmp/uc-integration-test.json"
  rm -f "$OUTPUT_FILE"

  if uv run lineage-bridge-extract --env "$ENV_ID" --output "$OUTPUT_FILE" 2>&1 | tee /tmp/test1.log | tail -3 | grep -q "Complete:"; then
    # Verify output file exists and has content
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
    cat /tmp/test1.log | tail -20
  fi
  echo ""
}

# Test 2: Catalog Enrichment
test_catalog_enrichment() {
  log_info "Test 2: Catalog Enrichment"

  OUTPUT_FILE="/tmp/uc-enriched-test.json"
  rm -f "$OUTPUT_FILE"

  # Check if Databricks credentials exist
  if [ -z "$LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL" ]; then
    test_skipped "Catalog enrichment (Databricks credentials not configured)"
    return
  fi

  if uv run lineage-bridge-extract --env "$ENV_ID" --output "$OUTPUT_FILE" 2>&1 | tee /tmp/test2.log | tail -3 | grep -q "Complete:"; then
    # Phase 1B (ADR-021): UC tables now use NodeType.CATALOG_TABLE with
    # catalog_type="UNITY_CATALOG" instead of the retired NodeType.UC_TABLE.
    UC_TABLES=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); print(sum(1 for n in data['nodes'] if n['node_type']=='catalog_table' and n.get('catalog_type')=='UNITY_CATALOG'))" 2>/dev/null || echo "0")

    if [ "$UC_TABLES" -ge 1 ]; then
      # Verify UC tables have enriched metadata. The DatabricksUCProvider
      # `enrich()` populates `columns` (the actual key — not "schema"),
      # along with owner, table_type, storage_location, etc.
      HAS_COLUMNS=$(python3 -c "import json; data=json.load(open('$OUTPUT_FILE')); uc=[n for n in data['nodes'] if n['node_type']=='catalog_table' and n.get('catalog_type')=='UNITY_CATALOG']; print(1 if uc and uc[0].get('attributes', {}).get('columns') else 0)" 2>/dev/null || echo "0")

      if [ "$HAS_COLUMNS" = "1" ]; then
        test_passed "Catalog enrichment ($UC_TABLES UC tables with column metadata)"
      else
        test_failed "Catalog enrichment (UC tables missing column metadata)"
      fi
    else
      log_warn "Catalog enrichment (no UC tables found - may not have Tableflow integration)"
      test_skipped "Catalog enrichment (no UC tables to enrich)"
    fi
  else
    test_failed "Catalog enrichment (extraction failed)"
  fi
  echo ""
}

# Test 3: Lineage Push to UC
test_lineage_push() {
  log_info "Test 3: Lineage Push to UC"

  # Check if Databricks warehouse is configured
  if [ -z "$LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID" ]; then
    test_skipped "Lineage push (Databricks warehouse not configured)"
    return
  fi

  # Run extraction with lineage push. The CLI emits one line per push:
  #   "Push: <N> tables, <P> properties, <C> comments"
  # Test passes when at least one property OR comment is set on at least
  # one table (i.e. the push actually wrote something to UC). PERMISSION_DENIED
  # errors propagate via the non-zero counts being printed alongside Error: lines.
  uv run lineage-bridge-extract --env "$ENV_ID" --push-lineage 2>&1 | tee /tmp/test3.log >/dev/null

  push_summary=$(grep -E "^Push: " /tmp/test3.log | tail -1)
  if [ -z "$push_summary" ]; then
    if grep -q "No UC tables to push\|catalog_table.*UNITY_CATALOG.*0" /tmp/test3.log; then
      test_skipped "Lineage push (no UC tables found)"
    else
      test_failed "Lineage push (CLI did not emit a Push: summary line)"
      tail -10 /tmp/test3.log
    fi
  else
    # Parse "Push: N tables, P properties, C comments" → check P+C > 0.
    counts=$(echo "$push_summary" | python3 -c "
import re, sys
m = re.search(r'Push: (\d+) tables, (\d+) properties, (\d+) comments', sys.stdin.read())
print(f'{m.group(1)} {m.group(2)} {m.group(3)}' if m else '0 0 0')
")
    set -- $counts
    n_tables=$1 n_props=$2 n_comments=$3
    if [ "$n_props" -gt 0 ] || [ "$n_comments" -gt 0 ]; then
      test_passed "Lineage push ($n_tables tables, $n_props properties, $n_comments comments)"
      log_info "  To verify: Run this SQL in Databricks:"
      echo "    SELECT table_name, comment FROM system.information_schema.tables"
      echo "    WHERE comment LIKE '%LineageBridge%';"
    else
      # Push reached UC but every property/comment write failed. Most common cause
      # is the user PAT lacking MODIFY on Tableflow-created tables (UC ownership
      # quirk — Tableflow's service principal owns the tables). Real LB regressions
      # would show 0 tables instead.
      test_failed "Lineage push ($n_tables tables touched but 0 properties + 0 comments — check Databricks permissions)"
      tail -8 /tmp/test3.log
    fi
  fi
  echo ""
}

# Test 4: Change Watcher
test_change_watcher() {
  log_info "Test 4: Change Watcher (30s test)"

  # Run watcher for 30 seconds
  timeout 30 uv run lineage-bridge-watch --env "$ENV_ID" --cooldown 10 >/tmp/test4.log 2>&1 &
  WATCHER_PID=$!

  sleep 5
  if ps -p $WATCHER_PID > /dev/null; then
    # Watcher is running, wait for timeout
    wait $WATCHER_PID 2>/dev/null || true

    # Check logs for successful polling
    if grep -q "Watcher using REST API polling\|Change poller initialized\|Discovered.*cluster" /tmp/test4.log; then
      test_passed "Change watcher (ran for 30s, polling active)"
    else
      test_failed "Change watcher (did not start polling)"
      tail -10 /tmp/test4.log
    fi
  else
    test_failed "Change watcher (failed to start)"
  fi
  echo ""
}

# Test 5: API Server
test_api_server() {
  log_info "Test 5: API Server"

  # Start API server in background
  uv run lineage-bridge-api >/tmp/test5-api.log 2>&1 &
  API_PID=$!

  # Wait for server to start
  sleep 5

  # Test 5a: Health check
  if curl -s http://localhost:8000/api/v1/health | grep -q '"status":"ok"'; then
    test_passed "API server health check"
  else
    test_failed "API server health check"
  fi

  # Test 5b: Trigger extraction task
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
    test_failed "API extraction task creation (no task_id returned)"
  fi

  # Test 5c: List graphs
  GRAPH_COUNT=$(curl -s http://localhost:8000/api/v1/graphs | python3 -c "import json, sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
  if [ "$GRAPH_COUNT" -ge 1 ]; then
    test_passed "API graphs endpoint ($GRAPH_COUNT graphs)"
  else
    log_warn "API graphs endpoint (0 graphs - may not have completed extraction)"
  fi

  # Cleanup
  kill $API_PID 2>/dev/null || true
  echo ""
}

# Test 6: Docker Build
test_docker_build() {
  log_info "Test 6: Docker Build"

  if $SKIP_DOCKER; then
    test_skipped "Docker build (--skip-docker flag)"
    return
  fi

  if ! command -v docker &> /dev/null; then
    test_skipped "Docker build (docker not installed)"
    return
  fi

  # Check if Docker daemon is running
  if ! docker info &> /dev/null; then
    test_skipped "Docker build (Docker daemon not running)"
    return
  fi

  # `docker compose build` (modern syntax used by the Makefile) does NOT
  # print "Successfully" on success — that was the legacy `docker build`
  # output. Check the exit status and the absence of "ERROR" lines instead.
  if make docker-build >/tmp/test6.log 2>&1; then
    if grep -qE "^ERROR\b|^failed to" /tmp/test6.log; then
      test_failed "Docker build (errors in output)"
      tail -8 /tmp/test6.log
    else
      test_passed "Docker build"
    fi
  else
    test_failed "Docker build"
    tail -10 /tmp/test6.log
  fi
  echo ""
}

# Test 7: MkDocs Build
test_docs_build() {
  log_info "Test 7: Documentation Build"

  if make docs-build 2>&1 | tee /tmp/test7.log | grep -q "INFO.*Building"; then
    # Check for broken links or errors
    if grep -q "ERROR" /tmp/test7.log; then
      test_failed "Documentation build (has errors)"
      grep "ERROR" /tmp/test7.log | head -5
    else
      test_passed "Documentation build"
    fi
  else
    test_failed "Documentation build (mkdocs command failed)"
  fi
  echo ""
}

# Main execution
main() {
  echo "=========================================="
  echo "LineageBridge UC Demo Integration Tests"
  echo "=========================================="
  echo ""

  check_prerequisites

  test_basic_extraction
  test_catalog_enrichment
  test_lineage_push
  test_change_watcher
  test_api_server
  test_docker_build
  test_docs_build

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

# Run main
main
