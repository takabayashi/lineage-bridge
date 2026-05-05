# Docker Publishing Guide

This guide explains how to build and publish the LineageBridge Docker image to GitHub Container Registry (ghcr.io).

## Quick Start

**Option 1: Use GitHub Actions (Recommended)**

Push a git tag to automatically build and publish:

```bash
git tag v0.6.0
git push origin v0.6.0
```

GitHub Actions will automatically build and push to `ghcr.io/takabayashi/lineage-bridge:0.6.0` and `:latest`.

**Option 2: Manual Build & Push**

Run the publish script:

```bash
./scripts/docker-publish.sh
```

## Prerequisites

### For GitHub Actions (Option 1)

- Push access to the repository
- No additional setup needed (uses `GITHUB_TOKEN` automatically)

### For Manual Publishing (Option 2)

1. **Docker installed and running**
   ```bash
   docker --version
   ```

2. **GitHub personal access token with `write:packages` permission**
   - Create one at: https://github.com/settings/tokens/new?scopes=write:packages
   - Select the `write:packages` scope
   - Copy the token (you won't see it again)

3. **Login to GitHub Container Registry**
   ```bash
   export GITHUB_TOKEN=<your-token>
   echo $GITHUB_TOKEN | docker login ghcr.io -u takabayashi --password-stdin
   ```

## Publishing Methods

### Method 1: GitHub Actions (Automated)

The repository has a `.github/workflows/docker.yml` workflow that automatically builds and pushes images when you push a git tag.

**Workflow triggers:**
- Any git tag starting with `v` (e.g., `v0.6.0`, `v1.0.0`)

**What it does:**
- Builds the Docker image using `infra/docker/Dockerfile`
- Tags with the version (e.g., `0.6.0`) and `latest`
- Pushes to `ghcr.io/takabayashi/lineage-bridge`

**Example:**

```bash
# Tag the current commit
git tag v0.6.0

# Push the tag (triggers the workflow)
git push origin v0.6.0

# Watch the build at:
# https://github.com/takabayashi/lineage-bridge/actions
```

**Wait 2-3 minutes**, then the image will be available at:
- `ghcr.io/takabayashi/lineage-bridge:0.6.0`
- `ghcr.io/takabayashi/lineage-bridge:latest`

### Method 2: Manual Build & Push

Use the helper script for manual builds:

```bash
# Use version from pyproject.toml (0.6.0)
./scripts/docker-publish.sh

# Or specify a custom version
./scripts/docker-publish.sh 0.6.1-rc1
```

**What it does:**
1. Reads version from `pyproject.toml` (or uses provided version)
2. Checks Docker login to ghcr.io
3. Builds image with two tags: `${VERSION}` and `latest`
4. Pushes both tags to GitHub Container Registry
5. Prints pull/run commands

**Example output:**

```
→ Building LineageBridge Docker image v0.6.0

✓ Image built successfully

→ Image details:
REPOSITORY                              TAG       SIZE      CREATED AT
ghcr.io/takabayashi/lineage-bridge     0.6.0     287MB     2026-05-05 10:55:00
ghcr.io/takabayashi/lineage-bridge     latest    287MB     2026-05-05 10:55:00

→ Pushing to ghcr.io...
✓ Images pushed successfully

  ╔═══════════════════════════════════════════════════════════════╗
  ║                                                               ║
  ║   Images published to GitHub Container Registry              ║
  ║                                                               ║
  ╚═══════════════════════════════════════════════════════════════╝

  Pull commands:
    docker pull ghcr.io/takabayashi/lineage-bridge:0.6.0
    docker pull ghcr.io/takabayashi/lineage-bridge:latest

  Run commands:
    docker run -p 8501:8501 ghcr.io/takabayashi/lineage-bridge:latest
    docker run -p 8501:8501 ghcr.io/takabayashi/lineage-bridge:0.6.0
```

### Method 3: Manual Docker Commands

If you prefer manual control:

```bash
# Build
docker build \
  -t ghcr.io/takabayashi/lineage-bridge:0.6.0 \
  -t ghcr.io/takabayashi/lineage-bridge:latest \
  -f infra/docker/Dockerfile \
  .

# Push
docker push ghcr.io/takabayashi/lineage-bridge:0.6.0
docker push ghcr.io/takabayashi/lineage-bridge:latest
```

## Verifying the Published Image

### 1. Check on GitHub

Visit the package page:
https://github.com/takabayashi/lineage-bridge/pkgs/container/lineage-bridge

You should see:
- ✓ Package visibility: Public
- ✓ Tags: `latest`, `0.6.0`
- ✓ Size: ~287MB
- ✓ Last updated: recent timestamp

### 2. Pull and Run

```bash
# Pull the image
docker pull ghcr.io/takabayashi/lineage-bridge:latest

# Run the UI
docker run -p 8501:8501 ghcr.io/takabayashi/lineage-bridge:latest

# Open http://localhost:8501
# Click "Load Demo Graph"
```

### 3. Test the One-Liner

The README one-liner should work:

```bash
docker run -p 8501:8501 ghcr.io/takabayashi/lineage-bridge:latest
```

## Making the Package Public

By default, GitHub packages are private. To make it public:

1. Go to: https://github.com/takabayashi/lineage-bridge/pkgs/container/lineage-bridge
2. Click **Package settings** (gear icon)
3. Scroll to **Danger Zone**
4. Click **Change visibility**
5. Select **Public**
6. Confirm

Now anyone can pull the image without authentication:
```bash
docker pull ghcr.io/takabayashi/lineage-bridge:latest
```

## Troubleshooting

### "denied: permission_denied" when pushing

**Cause:** Not logged in or token lacks `write:packages` permission.

**Fix:**
```bash
# Create a new token with write:packages at:
# https://github.com/settings/tokens/new?scopes=write:packages

export GITHUB_TOKEN=<your-token>
echo $GITHUB_TOKEN | docker login ghcr.io -u takabayashi --password-stdin
```

### "repository does not exist or may require docker login"

**Cause:** Package doesn't exist yet or is private.

**Fix:**
1. Push the image once (creates the package)
2. Make the package public (see "Making the Package Public" above)

### Build fails with "cannot find module"

**Cause:** Missing files or incorrect build context.

**Fix:**
```bash
# Ensure you're in the repo root
cd /path/to/lineage-bridge

# Check Dockerfile path
ls -la infra/docker/Dockerfile

# Build with explicit context
docker build -f infra/docker/Dockerfile .
```

## Multi-Architecture Builds (Optional)

To build for both AMD64 and ARM64:

```bash
# Create buildx builder
docker buildx create --name multiarch --use

# Build and push for both architectures
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t ghcr.io/takabayashi/lineage-bridge:0.6.0 \
  -t ghcr.io/takabayashi/lineage-bridge:latest \
  -f infra/docker/Dockerfile \
  --push \
  .
```

This ensures the image works on both Intel/AMD machines and Apple Silicon.

## Related Files

- `.github/workflows/docker.yml` — GitHub Actions workflow for automated builds
- `infra/docker/Dockerfile` — Multi-stage Dockerfile
- `infra/docker/docker-compose.yml` — Docker Compose profiles
- `scripts/docker-publish.sh` — Helper script for manual publishing
- `Makefile` — Docker build targets (`make docker-build`, `make docker-ui`)
