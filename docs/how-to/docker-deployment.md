# Docker Deployment

Run LineageBridge in Docker containers for production deployments, reproducible extractions, and multi-stage workflows.

## Overview

LineageBridge provides a multi-stage Dockerfile and Docker Compose profiles for different use cases:

- **ui** — Run the Streamlit UI in a container
- **extract** — One-shot extraction, then exit
- **watch** — Continuous change-detection watcher
- **api** — REST API server (future)

This guide shows you how to build images, run containers, and deploy at scale.

## Quick Start

### Build the Image

```bash
$ make docker-build
```

This builds the image using the Dockerfile at `/infra/docker/Dockerfile`.

**Expected output:**
```
[+] Building 45.2s (12/12) FINISHED
 => [builder 1/4] FROM python:3.11-slim
 => [builder 2/4] COPY pyproject.toml .
 => [builder 3/4] COPY lineage_bridge/ lineage_bridge/
 => [builder 4/4] RUN pip install build && python -m build
 => [stage-1 1/5] FROM python:3.11-slim
 => [stage-1 2/5] COPY --from=builder /app/dist/*.whl /tmp/
 => [stage-1 3/5] RUN pip install /tmp/*.whl && rm /tmp/*.whl
 => [stage-1 4/5] COPY lineage_bridge/ lineage_bridge/
 => [stage-1 5/5] RUN mkdir -p /app/data
 => naming to docker.io/library/lineage-bridge:latest
```

### Run the UI

```bash
$ make docker-ui
```

This starts the Streamlit UI at http://localhost:8501.

**Expected output:**
```
[+] Running 1/1
 ✔ Container infra-ui-1  Started
```

### Run Extraction

```bash
$ make docker-extract
```

This runs a one-shot extraction and saves the graph to `./data/lineage_graph.json`.

**Expected output:**
```
[+] Running 1/1
 ✔ Container infra-extract-1  Started
[INFO] Discovering environments...
[INFO] Found 2 environments
[INFO] Extracting from env-abc123...
[INFO] Extraction complete (87 nodes, 134 edges)
[INFO] Graph saved to /app/data/lineage_graph.json
```

### Run the Watcher

```bash
$ make docker-watch
```

This starts the change-detection watcher in the foreground. Press Ctrl+C to stop.

**Expected output:**
```
[+] Running 1/1
 ✔ Container infra-watch-1  Started
Polling Confluent Cloud every 10s (cooldown: 30s)
Press Ctrl+C to stop
```

## Docker Compose Profiles

The `docker-compose.yml` file defines three profiles:

### Profile: `ui`

**Purpose:** Run the Streamlit UI.

**Service definition:**
```yaml
ui:
  build:
    context: ../..
    dockerfile: infra/docker/Dockerfile
  ports:
    - "8501:8501"
  env_file: ../../.env
  environment:
    - LINEAGE_BRIDGE_EXTRACT_OUTPUT_PATH=/app/data/lineage_graph.json
  volumes:
    - ../../data:/app/data
  profiles:
    - ui
```

**Key features:**

- Exposes port 8501 (Streamlit default)
- Mounts `./data` for graph persistence
- Loads credentials from `.env`

**Run it:**
```bash
$ docker compose -f infra/docker/docker-compose.yml --profile ui up
```

### Profile: `extract`

**Purpose:** One-shot extraction, then exit.

**Service definition:**
```yaml
extract:
  build:
    context: ../..
    dockerfile: infra/docker/Dockerfile
  command: ["lineage-bridge-extract"]
  env_file: ../../.env
  environment:
    - LINEAGE_BRIDGE_EXTRACT_OUTPUT_PATH=/app/data/lineage_graph.json
  volumes:
    - ../../data:/app/data
  profiles:
    - extract
```

**Key features:**

- Runs `lineage-bridge-extract` and exits
- Saves output to `./data/lineage_graph.json`
- Use `run --rm` to remove the container after completion

**Run it:**
```bash
$ docker compose -f infra/docker/docker-compose.yml --profile extract run --rm extract
```

### Profile: `watch`

**Purpose:** Continuous change-detection watcher.

**Service definition:**
```yaml
watch:
  build:
    context: ../..
    dockerfile: infra/docker/Dockerfile
  command:
    - lineage-bridge-watch
    - --env
    - ${LINEAGE_BRIDGE_WATCH_ENV:-env-change-me}
    - --cooldown
    - "${LINEAGE_BRIDGE_WATCH_COOLDOWN:-30}"
  env_file: ../../.env
  volumes:
    - ../../data:/app/data
  profiles:
    - watch
```

**Key features:**

- Polls Confluent Cloud every 10 seconds (default)
- Re-extracts lineage 30 seconds after the last detected change (cooldown)
- Runs in the foreground (use `docker compose up -d` to daemonize)

**Run it:**
```bash
$ LINEAGE_BRIDGE_WATCH_ENV=env-abc123 docker compose -f infra/docker/docker-compose.yml --profile watch up
```

## Environment Variables

### Required

All services need a Confluent Cloud API key:

```bash
# In .env file
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your-cloud-api-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your-cloud-api-secret
```

### Optional

Override extraction settings:

```bash
# Output path (default: /app/data/lineage_graph.json)
LINEAGE_BRIDGE_EXTRACT_OUTPUT_PATH=/app/data/custom.json

# Watcher settings
LINEAGE_BRIDGE_WATCH_ENV=env-abc123
LINEAGE_BRIDGE_WATCH_COOLDOWN=60

# Catalog credentials (for enrichment)
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi...
LINEAGE_BRIDGE_AWS_REGION=us-east-1
```

### Passing Environment Variables

=== "Via .env File"

    Create a `.env` file in your project root:

    ```bash
    LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=cloud-key
    LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=cloud-secret
    ```

    Docker Compose loads it automatically via `env_file: ../../.env`.

=== "Via Command Line"

    Pass variables directly:

    ```bash
    $ docker compose -f infra/docker/docker-compose.yml --profile extract run --rm \
        -e LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=cloud-key \
        -e LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=cloud-secret \
        extract
    ```

=== "Via Shell Export"

    Export variables in your shell:

    ```bash
    $ export LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=cloud-key
    $ export LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=cloud-secret
    $ docker compose -f infra/docker/docker-compose.yml --profile extract run --rm extract
    ```

## Volume Mounts

### Data Directory

The `./data` directory is mounted to `/app/data` in the container:

```yaml
volumes:
  - ../../data:/app/data
```

**What it contains:**

- `lineage_graph.json` — Extracted lineage graph
- `lineage_graph_uc.json` — UC-specific lineage export (if using Databricks)
- Temporary files (auto-cleaned)

**Persistence:** Data survives container restarts and removals.

### .env File

The `.env` file is loaded via `env_file`:

```yaml
env_file: ../../.env
```

**Security note:** Do NOT mount `.env` as a volume in production. Use Docker secrets or environment variables instead.

## Dockerfile Deep Dive

The Dockerfile uses a **multi-stage build** to minimize image size.

### Stage 1: Builder

```dockerfile
FROM python:3.11-slim AS builder

WORKDIR /app

COPY pyproject.toml .
COPY lineage_bridge/ lineage_bridge/

RUN pip install --no-cache-dir build && \
    python -m build --wheel --outdir /app/dist
```

**What it does:**

- Copies source code and `pyproject.toml`
- Builds a wheel package
- Stores it in `/app/dist`

### Stage 2: Runtime

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /app/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

COPY lineage_bridge/ lineage_bridge/

RUN mkdir -p /app/data

RUN mkdir -p /root/.streamlit && \
    printf '[server]\nheadless = true\nport = 8501\naddress = "0.0.0.0"\n\n[browser]\ngatherUsageStats = false\n' \
    > /root/.streamlit/config.toml

EXPOSE 8501

CMD ["streamlit", "run", "lineage_bridge/ui/app.py"]
```

**What it does:**

- Installs the wheel from the builder stage
- Copies source code (needed for Streamlit to import modules)
- Creates the `/app/data` directory
- Configures Streamlit for headless mode
- Sets the default command to run the UI

**Image size:** ~500 MB (Python 3.11 base + dependencies)

## Multi-Stage Deployment

### Development: Local Extraction + Containerized UI

Extract locally, view in Docker:

```bash
$ uv run lineage-bridge-extract
$ make docker-ui
```

**Why:** Faster iteration during development (no image rebuild).

### Staging: Scheduled Extraction + Persistent UI

Run extraction on a cron schedule, keep UI running:

```bash
# Cron job (runs every hour)
0 * * * * cd /path/to/lineage-bridge && make docker-extract

# Long-running UI
$ docker compose -f infra/docker/docker-compose.yml --profile ui up -d
```

**Why:** Fresh lineage data every hour, always-on UI.

### Production: Watcher + UI

Run both watcher and UI:

```bash
$ docker compose -f infra/docker/docker-compose.yml --profile watch --profile ui up -d
```

**Why:** Automatic re-extraction on changes, always-on UI.

## Resource Limits

### Memory

LineageBridge is memory-efficient, but large graphs can use significant memory during rendering.

**Recommended limits:**

- **Extract:** 512 MB (sufficient for most clusters)
- **UI:** 1 GB (supports large graphs with 1000+ nodes)
- **Watch:** 512 MB (minimal overhead)

**Set limits in `docker-compose.yml`:**
```yaml
ui:
  deploy:
    resources:
      limits:
        memory: 1G
      reservations:
        memory: 512M
```

### CPU

LineageBridge is I/O-bound (network calls to Confluent Cloud). CPU usage is low.

**Recommended limits:**

- **Extract:** 0.5 CPU
- **UI:** 1 CPU (for responsive UI)
- **Watch:** 0.5 CPU

**Set limits in `docker-compose.yml`:**
```yaml
ui:
  deploy:
    resources:
      limits:
        cpus: '1.0'
      reservations:
        cpus: '0.5'
```

## Optimization

### Reduce Image Size

1. **Use Alpine-based Python:**
   Replace `python:3.11-slim` with `python:3.11-alpine` in the Dockerfile.

   **Trade-off:** Smaller image (~200 MB) but longer build time (needs to compile some dependencies).

2. **Remove dev dependencies:**
   The wheel includes only runtime dependencies. Dev dependencies (pytest, ruff) are excluded.

3. **Multi-arch builds:**
   Build for ARM64 and AMD64:
   ```bash
   $ docker buildx build --platform linux/amd64,linux/arm64 -t lineage-bridge:latest .
   ```

### Cache Docker Layers

Build with BuildKit caching:

```bash
$ DOCKER_BUILDKIT=1 docker build --cache-from lineage-bridge:latest -t lineage-bridge:latest -f infra/docker/Dockerfile .
```

Or use a cache mount:

```dockerfile
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir /tmp/*.whl
```

## Troubleshooting

### "Could not find .env file"

**Cause:** `.env` doesn't exist or is not in the expected location.

**Solution:**

1. **Create `.env` from the example:**
   ```bash
   $ cp .env.example .env
   ```

2. **Verify the path in `docker-compose.yml`:**
   ```yaml
   env_file: ../../.env
   ```

   This path is relative to the `docker-compose.yml` file, not your current directory.

### "Permission denied: /app/data/lineage_graph.json"

**Cause:** The `./data` directory doesn't exist or has incorrect permissions.

**Solution:**

1. **Create the directory:**
   ```bash
   $ mkdir -p data
   ```

2. **Set permissions:**
   ```bash
   $ chmod 755 data
   ```

3. **Verify the mount:**
   ```bash
   $ docker compose -f infra/docker/docker-compose.yml --profile extract run --rm extract ls -la /app/data
   ```

### "Streamlit is not accessible at localhost:8501"

**Cause:** The UI service didn't start, or the port is already in use.

**Solution:**

1. **Check container logs:**
   ```bash
   $ docker compose -f infra/docker/docker-compose.yml --profile ui logs ui
   ```

2. **Check if port 8501 is in use:**
   ```bash
   $ lsof -i :8501
   ```

3. **Change the port in `docker-compose.yml`:**
   ```yaml
   ports:
     - "8502:8501"
   ```

### "Watcher exits immediately"

**Cause:** Missing `LINEAGE_BRIDGE_WATCH_ENV` environment variable.

**Solution:**

1. **Set the environment variable:**
   ```bash
   $ export LINEAGE_BRIDGE_WATCH_ENV=env-abc123
   $ make docker-watch
   ```

2. **Or pass it directly:**
   ```bash
   $ LINEAGE_BRIDGE_WATCH_ENV=env-abc123 make docker-watch
   ```

### "Image build fails: no such file or directory"

**Cause:** Docker build context is incorrect.

**Solution:**

1. **Verify the build context in `docker-compose.yml`:**
   ```yaml
   build:
     context: ../..
     dockerfile: infra/docker/Dockerfile
   ```

   The context is `../..` (project root), not `infra/docker`.

2. **Build manually to debug:**
   ```bash
   $ docker build -f infra/docker/Dockerfile .
   ```

## Next Steps

- [Integrate with CI/CD](ci-cd-integration.md) to automate Docker builds and deployments
- [Manage credentials securely](credential-management.md) using Docker secrets
- [Set up multi-environment extraction](multi-environment-setup.md) in Docker containers
