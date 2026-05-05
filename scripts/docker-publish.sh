#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Build and push LineageBridge Docker image to GitHub Container Registry
#
# Prerequisites:
#   - Docker installed and running
#   - GitHub personal access token with write:packages permission
#   - Logged in to ghcr.io: echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin
#
# Usage:
#   ./scripts/docker-publish.sh [VERSION]
#
# Examples:
#   ./scripts/docker-publish.sh           # Uses version from pyproject.toml
#   ./scripts/docker-publish.sh 0.6.0     # Explicit version
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────────────

REPO_OWNER="takabayashi"
REPO_NAME="lineage-bridge"
REGISTRY="ghcr.io"
IMAGE_NAME="${REGISTRY}/${REPO_OWNER}/${REPO_NAME}"

# ── Functions ───────────────────────────────────────────────────────────────

log() {
    echo "→ $*"
}

success() {
    echo "✓ $*"
}

error() {
    echo "✗ ERROR: $*" >&2
    exit 1
}

get_version_from_pyproject() {
    python3 -c "import tomllib; print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])" 2>/dev/null || \
    python3 -c "import tomli; print(tomli.load(open('pyproject.toml', 'rb'))['project']['version'])" 2>/dev/null || \
    grep '^version = ' pyproject.toml | cut -d'"' -f2
}

check_docker_login() {
    if ! docker info &>/dev/null; then
        error "Docker is not running. Please start Docker Desktop."
    fi

    if ! docker pull "${REGISTRY}/hello-world:latest" &>/dev/null 2>&1; then
        log "Not logged in to ${REGISTRY}"
        echo ""
        echo "  Please log in to GitHub Container Registry:"
        echo "  1. Create a GitHub personal access token with 'write:packages' permission"
        echo "     https://github.com/settings/tokens/new?scopes=write:packages"
        echo "  2. Run:"
        echo "     export GITHUB_TOKEN=<your-token>"
        echo "     echo \$GITHUB_TOKEN | docker login ghcr.io -u ${REPO_OWNER} --password-stdin"
        echo ""
        exit 1
    fi
}

# ── Main ────────────────────────────────────────────────────────────────────

main() {
    cd "$(dirname "$0")/.."

    # Get version
    VERSION="${1:-$(get_version_from_pyproject)}"
    if [ -z "$VERSION" ]; then
        error "Could not determine version. Please provide it as an argument."
    fi

    log "Building LineageBridge Docker image v${VERSION}"
    echo ""

    # Check Docker login
    check_docker_login

    # Build image
    log "Building image..."
    docker build \
        -t "${IMAGE_NAME}:${VERSION}" \
        -t "${IMAGE_NAME}:latest" \
        -f infra/docker/Dockerfile \
        .

    success "Image built successfully"
    echo ""

    # Show image info
    log "Image details:"
    docker images "${IMAGE_NAME}" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"
    echo ""

    # Push to registry
    log "Pushing to ${REGISTRY}..."
    docker push "${IMAGE_NAME}:${VERSION}"
    docker push "${IMAGE_NAME}:latest"

    success "Images pushed successfully"
    echo ""

    # Print pull commands
    cat <<EOF
  ╔═══════════════════════════════════════════════════════════════╗
  ║                                                               ║
  ║   Images published to GitHub Container Registry              ║
  ║                                                               ║
  ╚═══════════════════════════════════════════════════════════════╝

  Pull commands:
    docker pull ${IMAGE_NAME}:${VERSION}
    docker pull ${IMAGE_NAME}:latest

  Run commands:
    docker run -p 8501:8501 ${IMAGE_NAME}:latest
    docker run -p 8501:8501 ${IMAGE_NAME}:${VERSION}

  One-liner for README:
    docker run -p 8501:8501 ${IMAGE_NAME}:latest

  View on GitHub:
    https://github.com/${REPO_OWNER}/${REPO_NAME}/pkgs/container/${REPO_NAME}

EOF
}

main "$@"
