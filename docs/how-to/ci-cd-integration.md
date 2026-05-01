# CI/CD Integration

Automate lineage extraction, run it on a schedule, publish artifacts, and integrate with existing validation pipelines.

## Overview

LineageBridge integrates seamlessly with CI/CD platforms to:

- Extract lineage on every push or pull request
- Run scheduled extractions (daily, hourly, on-demand)
- Publish lineage graphs as build artifacts
- Validate graph integrity before deployment
- Trigger extractions on topology changes

This guide provides examples for **GitHub Actions** and **GitLab CI**, plus general patterns for other platforms.

## GitHub Actions

### Example 1: Scheduled Extraction

Extract lineage every hour and publish the graph as an artifact.

**File:** `.github/workflows/lineage-extract.yml`

```yaml
name: Lineage Extraction

on:
  schedule:
    # Run every hour at :15 past the hour
    - cron: '15 * * * *'
  workflow_dispatch:
    # Allow manual triggers

jobs:
  extract:
    runs-on: ubuntu-latest
    timeout-minutes: 10

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install uv
        run: pip install uv

      - name: Install LineageBridge
        run: uv pip install --system -e .

      - name: Extract lineage
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET }}
          LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL: ${{ secrets.LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL }}
          LINEAGE_BRIDGE_DATABRICKS_TOKEN: ${{ secrets.LINEAGE_BRIDGE_DATABRICKS_TOKEN }}
        run: |
          uv run lineage-bridge-extract \
            --env env-prod-us \
            --env env-prod-eu \
            --output lineage_graph.json

      - name: Upload lineage graph
        uses: actions/upload-artifact@v4
        with:
          name: lineage-graph-${{ github.run_id }}
          path: lineage_graph.json
          retention-days: 30

      - name: Validate graph
        run: |
          uv run python -c "
          from lineage_bridge.models.graph import LineageGraph
          graph = LineageGraph.from_json('lineage_graph.json')
          warnings = graph.validate()
          if warnings:
              print('Graph validation warnings:')
              for w in warnings:
                  print(f'  - {w}')
          print(f'Graph: {len(graph.nodes)} nodes, {len(graph.edges)} edges')
          "
```

**Key features:**

- Runs hourly via `cron`
- Secrets loaded from repository secrets (Settings → Secrets and variables → Actions)
- Extracts from multiple environments (`env-prod-us`, `env-prod-eu`)
- Publishes graph as an artifact (downloadable from the Actions UI)
- Validates graph integrity before finishing

**Secrets to add:**

- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY`
- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET`
- `LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL` (optional)
- `LINEAGE_BRIDGE_DATABRICKS_TOKEN` (optional)

### Example 2: Pull Request Validation

Extract lineage on every pull request and post a summary comment.

**File:** `.github/workflows/lineage-pr.yml`

```yaml
name: Lineage PR Validation

on:
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    timeout-minutes: 10

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install uv
        run: pip install uv

      - name: Install LineageBridge
        run: uv pip install --system -e .

      - name: Extract lineage
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET }}
        run: |
          uv run lineage-bridge-extract --env env-staging --output lineage_graph.json

      - name: Validate and summarize
        id: validate
        run: |
          uv run python -c "
          import json
          from lineage_bridge.models.graph import LineageGraph
          graph = LineageGraph.from_json('lineage_graph.json')
          warnings = graph.validate()
          
          summary = f'**Lineage Extraction Summary**\n\n'
          summary += f'- **Nodes:** {len(graph.nodes)}\n'
          summary += f'- **Edges:** {len(graph.edges)}\n'
          summary += f'- **Warnings:** {len(warnings)}\n\n'
          
          if warnings:
              summary += '**Validation Warnings:**\n'
              for w in warnings[:5]:
                  summary += f'- {w}\n'
              if len(warnings) > 5:
                  summary += f'- ... and {len(warnings) - 5} more\n'
          
          with open('summary.txt', 'w') as f:
              f.write(summary)
          
          # Exit with error if critical warnings
          if any('missing endpoint' in w for w in warnings):
              exit(1)
          " > /dev/null 2>&1 || echo "validation_failed=true" >> $GITHUB_OUTPUT

      - name: Comment PR
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const summary = fs.readFileSync('summary.txt', 'utf8');
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: summary
            });

      - name: Fail if validation failed
        if: steps.validate.outputs.validation_failed == 'true'
        run: exit 1
```

**Key features:**

- Runs on every pull request to `main`
- Extracts lineage from staging environment
- Validates graph and posts a summary comment
- Fails the PR check if critical warnings are found

### Example 3: Docker Build and Push

Build and push a Docker image on every release.

**File:** `.github/workflows/docker.yml`

```yaml
name: Docker Build

on:
  push:
    tags:
      - 'v*'
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to Docker Hub
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKER_USERNAME }}
          password: ${{ secrets.DOCKER_PASSWORD }}

      - name: Extract version from tag
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: your-dockerhub-username/lineage-bridge
          tags: |
            type=semver,pattern={{version}}
            type=semver,pattern={{major}}.{{minor}}
            type=raw,value=latest,enable={{is_default_branch}}

      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: .
          file: infra/docker/Dockerfile
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=registry,ref=your-dockerhub-username/lineage-bridge:latest
          cache-to: type=inline

      - name: Test the image
        run: |
          docker run --rm \
            -e LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY }} \
            -e LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET }} \
            your-dockerhub-username/lineage-bridge:latest \
            lineage-bridge-extract --help
```

**Key features:**

- Triggers on version tags (`v1.0.0`, `v2.1.3`)
- Builds multi-platform Docker image (AMD64, ARM64)
- Pushes to Docker Hub with version tags
- Tests the image by running `--help`

**Secrets to add:**

- `DOCKER_USERNAME`
- `DOCKER_PASSWORD`

## GitLab CI

### Example 1: Scheduled Extraction

**File:** `.gitlab-ci.yml`

```yaml
stages:
  - extract
  - validate

variables:
  LINEAGE_OUTPUT: lineage_graph.json

extract-lineage:
  stage: extract
  image: python:3.11-slim
  before_script:
    - pip install uv
    - uv pip install --system -e .
  script:
    - |
      uv run lineage-bridge-extract \
        --env env-prod \
        --output $LINEAGE_OUTPUT
  artifacts:
    paths:
      - $LINEAGE_OUTPUT
    expire_in: 30 days
  only:
    - schedules
    - web

validate-lineage:
  stage: validate
  image: python:3.11-slim
  before_script:
    - pip install uv
    - uv pip install --system -e .
  script:
    - |
      uv run python -c "
      from lineage_bridge.models.graph import LineageGraph
      graph = LineageGraph.from_json('$LINEAGE_OUTPUT')
      warnings = graph.validate()
      if warnings:
          print('Validation warnings:')
          for w in warnings:
              print(f'  - {w}')
      print(f'Graph: {len(graph.nodes)} nodes, {len(graph.edges)} edges')
      "
  dependencies:
    - extract-lineage
  only:
    - schedules
    - web
```

**Set up a pipeline schedule:**

1. Go to your project → CI/CD → Schedules
2. Click "New schedule"
3. Set interval (e.g., "0 */1 * * *" for hourly)
4. Add variables:
   - `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY`
   - `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET`

### Example 2: Merge Request Validation

```yaml
lineage-mr-check:
  stage: test
  image: python:3.11-slim
  before_script:
    - pip install uv
    - uv pip install --system -e .
  script:
    - uv run lineage-bridge-extract --env env-staging --output lineage_graph.json
    - |
      uv run python -c "
      from lineage_bridge.models.graph import LineageGraph
      graph = LineageGraph.from_json('lineage_graph.json')
      warnings = graph.validate()
      if any('missing endpoint' in w for w in warnings):
          print('CRITICAL: Graph has missing endpoints')
          exit(1)
      print(f'✓ Graph is valid ({len(graph.nodes)} nodes, {len(graph.edges)} edges)')
      "
  only:
    - merge_requests
```

**Key features:**

- Runs on every merge request
- Fails the MR check if critical warnings are found

## Change Detection Triggers

### Example 1: GitHub Actions with Watcher

Use the watcher to trigger extractions on topology changes:

**File:** `.github/workflows/lineage-watcher.yml`

```yaml
name: Lineage Watcher

on:
  schedule:
    # Run every 10 minutes
    - cron: '*/10 * * * *'
  workflow_dispatch:

jobs:
  watch:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install uv
        run: pip install uv

      - name: Install LineageBridge
        run: uv pip install --system -e .

      - name: Run watcher (one iteration)
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET }}
        run: |
          # Run watcher for 5 minutes, then exit
          timeout 5m uv run lineage-bridge-watch \
            --env env-prod \
            --cooldown 30 \
            --poll-interval 10 \
            || true

      - name: Upload graph if changed
        if: hashFiles('lineage_graph.json') != ''
        uses: actions/upload-artifact@v4
        with:
          name: lineage-graph-${{ github.run_id }}
          path: lineage_graph.json
          retention-days: 7
```

**How it works:**

- Runs every 10 minutes (via `cron`)
- Starts the watcher for 5 minutes, then times out
- If lineage changes during that window, the watcher extracts and saves the graph
- The graph is uploaded as an artifact

**Why this pattern?**

GitHub Actions doesn't support long-running jobs. This pattern runs the watcher for a short window on every cron trigger, effectively polling for changes.

### Example 2: Webhook-Triggered Extraction

Use a webhook to trigger extraction when Confluent Cloud events occur.

**File:** `.github/workflows/lineage-webhook.yml`

```yaml
name: Lineage Webhook

on:
  repository_dispatch:
    types: [confluent-topology-change]

jobs:
  extract:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install uv
        run: pip install uv

      - name: Install LineageBridge
        run: uv pip install --system -e .

      - name: Extract lineage
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET }}
        run: |
          uv run lineage-bridge-extract \
            --env ${{ github.event.client_payload.env_id }} \
            --output lineage_graph.json

      - name: Upload graph
        uses: actions/upload-artifact@v4
        with:
          name: lineage-graph-webhook-${{ github.run_id }}
          path: lineage_graph.json
```

**Trigger the workflow:**

```bash
$ curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: token YOUR_GITHUB_TOKEN" \
  https://api.github.com/repos/your-username/lineage-bridge/dispatches \
  -d '{"event_type":"confluent-topology-change","client_payload":{"env_id":"env-abc123"}}'
```

**Use case:** External monitoring system detects a topology change and triggers the workflow via webhook.

## Artifact Publishing

### Publish to S3

**GitHub Actions:**

```yaml
- name: Upload to S3
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_REGION: us-east-1
  run: |
    aws s3 cp lineage_graph.json s3://my-bucket/lineage/$(date +%Y-%m-%d)/lineage_graph.json
```

**GitLab CI:**

```yaml
publish-s3:
  stage: publish
  image: amazon/aws-cli
  script:
    - aws s3 cp lineage_graph.json s3://my-bucket/lineage/$(date +%Y-%m-%d)/lineage_graph.json
  dependencies:
    - extract-lineage
```

### Publish to Google Cloud Storage

**GitHub Actions:**

```yaml
- name: Upload to GCS
  uses: google-github-actions/upload-cloud-storage@v1
  with:
    credentials: ${{ secrets.GCP_SERVICE_ACCOUNT_KEY }}
    path: lineage_graph.json
    destination: gs://my-bucket/lineage/
```

### Publish to Artifactory

**GitHub Actions:**

```yaml
- name: Upload to Artifactory
  run: |
    curl -u ${{ secrets.ARTIFACTORY_USER }}:${{ secrets.ARTIFACTORY_TOKEN }} \
      -T lineage_graph.json \
      https://artifactory.example.com/artifactory/lineage-graphs/$(date +%Y-%m-%d)/lineage_graph.json
```

## Integration with Existing Pipelines

### Example: Pre-Deployment Validation

Run lineage extraction and validation before deploying Kafka infrastructure changes.

**GitHub Actions:**

```yaml
name: Kafka Deployment

on:
  push:
    branches: [main]
    paths:
      - 'terraform/**'
      - 'kafka-config/**'

jobs:
  validate-lineage:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install uv
        run: pip install uv

      - name: Install LineageBridge
        run: uv pip install --system -e .

      - name: Extract current lineage
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET }}
        run: |
          uv run lineage-bridge-extract --env env-prod --output lineage_before.json

      - name: Apply Terraform changes
        run: terraform apply -auto-approve
        working-directory: terraform/

      - name: Extract new lineage
        env:
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY }}
          LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET }}
        run: |
          uv run lineage-bridge-extract --env env-prod --output lineage_after.json

      - name: Compare lineage
        run: |
          uv run python -c "
          from lineage_bridge.models.graph import LineageGraph
          before = LineageGraph.from_json('lineage_before.json')
          after = LineageGraph.from_json('lineage_after.json')
          
          added = set(after.nodes.keys()) - set(before.nodes.keys())
          removed = set(before.nodes.keys()) - set(after.nodes.keys())
          
          print(f'Nodes added: {len(added)}')
          print(f'Nodes removed: {len(removed)}')
          
          if removed:
              print('WARNING: The following nodes were removed:')
              for node_id in list(removed)[:10]:
                  print(f'  - {node_id}')
          "
```

**Key features:**

- Extracts lineage before and after Terraform apply
- Compares the two graphs to detect changes
- Warns if nodes were removed (potential data loss)

## Troubleshooting

### "Workflow is skipped"

**Cause:** GitHub Actions has a path filter that doesn't match any changed files.

**Solution:**

1. Remove the `paths` filter to run on every push:
   ```yaml
   on:
     push:
       branches: [main]
   ```

2. Or adjust the filter to match your file structure.

### "Secrets are not available in pull requests from forks"

**Cause:** GitHub Actions doesn't expose secrets to pull requests from forked repositories (security feature).

**Solution:**

1. Use `pull_request_target` instead of `pull_request`:
   ```yaml
   on:
     pull_request_target:
       branches: [main]
   ```

   **Warning:** This gives forked PRs access to secrets. Only use if you trust contributors.

2. Or skip extraction in forked PRs:
   ```yaml
   if: github.event.pull_request.head.repo.full_name == github.repository
   ```

### "Extraction times out"

**Cause:** Large environments take longer than the default timeout (10 minutes).

**Solution:**

1. Increase the timeout:
   ```yaml
   jobs:
     extract:
       timeout-minutes: 30
   ```

2. Or filter to specific clusters:
   ```yaml
   run: uv run lineage-bridge-extract --env env-prod --cluster lkc-primary
   ```

### "Artifact upload fails"

**Cause:** The artifact is too large (GitHub Actions limit: 2 GB per artifact).

**Solution:**

1. **Compress the graph:**
   ```yaml
   - name: Compress graph
     run: gzip lineage_graph.json

   - name: Upload artifact
     uses: actions/upload-artifact@v4
     with:
       name: lineage-graph
       path: lineage_graph.json.gz
   ```

2. **Or upload to S3/GCS instead.**

## Next Steps

- [Set up credential management](credential-management.md) to securely store secrets in CI/CD
- [Deploy with Docker](docker-deployment.md) to run extractions in containers
- [Configure multi-environment extraction](multi-environment-setup.md) for production pipelines
