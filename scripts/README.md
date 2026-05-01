# LineageBridge Scripts

Utility scripts for development, testing, and operations.

## Integration Testing

LineageBridge includes comprehensive integration test suites for all three demo environments: Unity Catalog, AWS Glue, and BigQuery. These tests validate end-to-end functionality using live cloud infrastructure.

### Quick Start

```bash
# Run all integration tests (requires all demos provisioned)
make test-integration-all

# Run specific demo tests
make test-integration-uc              # Unity Catalog
make test-integration-glue            # AWS Glue
make test-integration-bigquery        # BigQuery

# Skip Docker build tests (faster)
make test-integration-uc-skip-docker
make test-integration-glue-skip-docker
make test-integration-bigquery-skip-docker
```

### Common Test Features

All integration test scripts share:
- **Colored output** (green=pass, red=fail, yellow=skip)
- **Detailed logging** to `/tmp/<demo>-test*.log`
- **Smart prerequisite checking** (credentials, CLI tools, cloud access)
- **Graceful skipping** when optional dependencies unavailable
- **Summary report** with pass/fail/skip counts

---

### `integration-test-uc.sh`

End-to-end integration tests using a live Unity Catalog demo environment.

**Prerequisites:**
- UC demo infrastructure provisioned: `make demo-uc-up`
- .env file generated from terraform: `cd infra/demos/uc && terraform output -raw demo_env_file > $PROJECT_ROOT/.env`

**Usage:**

```bash
# Run all tests
make test-integration-uc

# Skip Docker build test (faster)
make test-integration-uc-skip-docker

# Run directly
./scripts/integration-test-uc.sh
```

**Tests Included (7 total):**

1. **Basic Extraction** - Verifies lineage extraction with node/edge counts (target: 15+ nodes, 20+ edges)
2. **Catalog Enrichment** - Checks UC table metadata enrichment (schema, owner, storage)
3. **Lineage Push** - Tests pushing metadata back to Unity Catalog table properties
4. **Change Watcher** - Validates change detection polling (30s run)
5. **API Server** - Tests health check, task creation, and graph listing
6. **Docker Build** - Verifies Docker image builds successfully
7. **Documentation Build** - Ensures MkDocs builds without errors

**Logs:** `/tmp/test*.log`, `/tmp/uc-*.json`

---

### `integration-test-glue.sh`

End-to-end integration tests using a live AWS Glue demo environment.

**Prerequisites:**
- AWS Glue demo infrastructure provisioned: `make demo-glue-up`
- .env file generated from terraform: `cd infra/demos/glue && terraform output -raw demo_env_file > $PROJECT_ROOT/.env`
- AWS CLI installed and authenticated: `aws configure`

**Usage:**

```bash
# Run all tests
make test-integration-glue

# Skip Docker build test (faster)
make test-integration-glue-skip-docker

# Run directly
./scripts/integration-test-glue.sh
```

**Tests Included (8 total):**

1. **Basic Extraction** - Verifies lineage extraction (target: 10+ nodes, 8+ edges)
2. **Tableflow to S3 Iceberg** - Checks Tableflow table nodes with S3 storage
3. **AWS Glue Catalog Enrichment** - Verifies Glue table metadata (columns, partitions)
4. **Athena Query Validation** - Lists tables in Glue database via AWS CLI
5. **Iceberg Time Travel Features** - Checks for Iceberg metadata files in S3
6. **Change Watcher** - Validates change detection polling (30s run)
7. **API Server** - Tests health check, task creation, and graph listing
8. **Docker Build** - Verifies Docker image builds successfully

**Glue-Specific Checks:**
- Glue Data Catalog integration
- S3 Iceberg table format
- Athena query examples
- IAM role permissions

**Logs:** `/tmp/glue-test*.log`, `/tmp/glue-*.json`

---

### `integration-test-bigquery.sh`

End-to-end integration tests using a live BigQuery demo environment.

**Prerequisites:**
- BigQuery demo infrastructure provisioned: `make demo-bq-up`
- .env file generated from terraform: `cd infra/demos/bigquery && terraform output -raw demo_env_file > $PROJECT_ROOT/.env`
- gcloud CLI installed and authenticated: `gcloud auth login`
- bq CLI installed: `gcloud components install bq`

**Usage:**

```bash
# Run all tests
make test-integration-bigquery

# Skip Docker build test (faster)
make test-integration-bigquery-skip-docker

# Run directly
./scripts/integration-test-bigquery.sh
```

**Tests Included (9 total):**

1. **Basic Extraction** - Verifies lineage extraction (target: 8+ nodes, 6+ edges)
2. **BigQuery Connector Detection** - Finds BigQuery Sink v2 connectors
3. **BigQuery External Dataset Nodes** - Checks for BigQuery dataset nodes
4. **BigQuery Table Validation** - Lists tables via bq CLI
5. **BigQuery Data Lineage API** - Tests Google Data Lineage integration
6. **Connector Configuration** - Validates connector has required BigQuery settings
7. **Change Watcher** - Validates change detection polling (30s run)
8. **API Server** - Tests health check, task creation, and graph listing
9. **Docker Build** - Verifies Docker image builds successfully

**BigQuery-Specific Checks:**
- BigQuery Connector v2 configuration
- GCP project and dataset structure
- BigQuery Data Lineage API integration
- Service account authentication

**Logs:** `/tmp/bq-test*.log`, `/tmp/bq-*.json`

---

## Test Output Format

All integration test scripts provide consistent output:

```
==========================================
LineageBridge <Demo> Integration Tests
==========================================

[INFO] Checking prerequisites...
[INFO] Environment ID: env-abc123
[INFO] Prerequisites OK

[INFO] Test 1: Basic Extraction
[INFO] ✓ Basic extraction (27 nodes, 25 edges)
  Node types: connector: 3, kafka_topic: 12, schema: 4, uc_table: 3, ...

[INFO] Test 2: Catalog Enrichment
[INFO] ✓ Catalog enrichment (3 UC tables with metadata)

[WARN] Test 3: Lineage Push
[WARN] ⊘ Lineage push (Databricks warehouse not configured)

...

==========================================
Test Results Summary
==========================================
Passed:  5
Failed:  0
Skipped: 2
Total:   7

All tests passed!
```

**Exit Codes:**
- `0` - All tests passed
- `1` - One or more tests failed

---

## Comparison Matrix

| Feature | UC Tests | Glue Tests | BigQuery Tests |
|---------|----------|------------|----------------|
| **Total Tests** | 7 | 8 | 9 |
| **Catalog Integration** | Databricks UC | AWS Glue | BigQuery Data Catalog |
| **Lineage Push** | Yes (table properties) | No (read-only) | No (read-only) |
| **Query Validation** | Manual (Databricks SQL) | AWS CLI (Athena) | gcloud CLI (bq) |
| **Special Features** | Downstream table discovery | Iceberg time travel | BigQuery Lineage API |
| **Cloud Dependencies** | Databricks token | AWS credentials | GCP credentials |
| **Avg Runtime** | 2-3 min | 2-3 min | 2-3 min |

## Utility Scripts

### `ensure-cloud-key.sh`

Auto-provisions Confluent Cloud API key if missing. Used by `make ui`.

### `validate-docs.sh`

Validates documentation for correctness (CLI flags, environment variables, file paths).

**Usage:**

```bash
./scripts/validate-docs.sh
```

**Output:**

Runs 102 automated checks and reports confidence level (target: 98%+).

## Adding New Integration Tests

To add integration tests for additional catalog providers:

1. Copy an existing test script (e.g., `integration-test-uc.sh`) to `integration-test-<catalog>.sh`
2. Update terraform paths:
   ```bash
   ENV_ID=$(cd "$PROJECT_ROOT/infra/demos/<catalog>" && terraform output -raw confluent_environment_id)
   ```
3. Modify catalog-specific tests:
   - Update node type checks (e.g., `glue_table`, `bq_table`)
   - Add catalog-specific validation (APIs, CLI tools)
   - Adjust test counts based on demo topology
4. Add Makefile targets:
   ```makefile
   test-integration-<catalog>: ## Run <Catalog> demo integration tests
       bash scripts/integration-test-<catalog>.sh
   
   test-integration-<catalog>-skip-docker: ## Run <Catalog> tests (skip Docker)
       bash scripts/integration-test-<catalog>.sh --skip-docker
   ```
5. Update `scripts/README.md` with test documentation

---

## CI/CD Integration

### GitHub Actions

These scripts can be used in GitHub Actions workflows. Example for all three demos:

```yaml
# .github/workflows/integration-tests.yml
name: Integration Tests

on:
  push:
    branches: [main]
  pull_request:
  workflow_dispatch:

jobs:
  test-uc:
    name: Unity Catalog Integration
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      
      - name: Install uv
        run: pip install uv
      
      - name: Install dependencies
        run: make install
      
      - name: Run UC integration tests
        run: make test-integration-uc-skip-docker
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.CONFLUENT_CLOUD_API_SECRET }}
          LINEAGE_BRIDGE_CLUSTER_CREDENTIALS: ${{ secrets.UC_CLUSTER_CREDENTIALS }}
          LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT: ${{ secrets.UC_SR_ENDPOINT }}
          LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY: ${{ secrets.UC_SR_API_KEY }}
          LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET: ${{ secrets.UC_SR_API_SECRET }}
          LINEAGE_BRIDGE_TABLEFLOW_API_KEY: ${{ secrets.UC_TABLEFLOW_API_KEY }}
          LINEAGE_BRIDGE_TABLEFLOW_API_SECRET: ${{ secrets.UC_TABLEFLOW_API_SECRET }}
          LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL: ${{ secrets.DATABRICKS_WORKSPACE_URL }}
          LINEAGE_BRIDGE_DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID: ${{ secrets.DATABRICKS_WAREHOUSE_ID }}

  test-glue:
    name: AWS Glue Integration
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      
      - name: Install dependencies
        run: make install
      
      - name: Run Glue integration tests
        run: make test-integration-glue-skip-docker
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.CONFLUENT_CLOUD_API_SECRET }}
          LINEAGE_BRIDGE_AWS_REGION: us-east-1

  test-bigquery:
    name: BigQuery Integration
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      
      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SERVICE_ACCOUNT_KEY }}
      
      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2
      
      - name: Install dependencies
        run: make install
      
      - name: Run BigQuery integration tests
        run: make test-integration-bigquery-skip-docker
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.CONFLUENT_CLOUD_API_SECRET }}
          LINEAGE_BRIDGE_GCP_PROJECT_ID: ${{ secrets.GCP_PROJECT_ID }}
```

### GitLab CI

```yaml
# .gitlab-ci.yml
integration-tests:
  stage: test
  image: python:3.11
  
  before_script:
    - pip install uv
    - make install
  
  parallel:
    matrix:
      - DEMO: [uc, glue, bigquery]
  
  script:
    - make test-integration-${DEMO}-skip-docker
  
  variables:
    LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: $CONFLUENT_CLOUD_API_KEY
    LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: $CONFLUENT_CLOUD_API_SECRET
  
  artifacts:
    when: always
    paths:
      - /tmp/*-test*.log
      - /tmp/*-integration-test.json
    expire_in: 1 week
```

### Scheduled Testing

Run integration tests nightly to catch regressions:

```yaml
# .github/workflows/nightly-integration.yml
name: Nightly Integration Tests

on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM UTC daily
  workflow_dispatch:

jobs:
  test-all:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install dependencies
        run: make install
      - name: Run all integration tests
        run: make test-integration-all
      - name: Upload test logs
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: integration-test-logs
          path: /tmp/*-test*.log
```

---

## Troubleshooting

### Common Issues

**Issue:** Tests fail with authentication errors
- **Solution:** Verify credentials are set correctly in .env
- **Check:** Run `source .env && env | grep LINEAGE_BRIDGE` to verify

**Issue:** "Terraform output not found"
- **Solution:** Ensure demo is provisioned: `cd infra/demos/<demo> && terraform apply`
- **Alternative:** Manually create .env with required credentials

**Issue:** Tests skipped due to missing CLI tools
- **UC:** Install Databricks CLI (if testing push features)
- **Glue:** Install AWS CLI: `pip install awscli`
- **BigQuery:** Install gcloud SDK and bq: `gcloud components install bq`

**Issue:** Docker tests fail
- **Solution:** Start Docker daemon or use `--skip-docker` flag
- **Alternative:** Run `make test-integration-<demo>-skip-docker`

**Issue:** API server tests timeout
- **Solution:** Extraction may take longer on first run (60s+). Increase poll count in script or wait for completion.

### Debug Mode

Enable verbose logging:

```bash
# Add to script for debugging
set -x  # Print all commands
export LINEAGE_BRIDGE_LOG_LEVEL=DEBUG
```

### Manual Validation

If automated tests fail, validate manually:

```bash
# UC Demo
uv run lineage-bridge-extract --env <env-id> --output /tmp/manual-test.json
python3 -c "import json; data=json.load(open('/tmp/manual-test.json')); print(f'Nodes: {len(data[\"nodes\"])}, Edges: {len(data[\"edges\"])}')"

# Glue Demo
aws glue get-tables --database-name <database-name>

# BigQuery Demo
bq ls <project-id>:<dataset-id>
```
