#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge One-Line Quickstart
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/takabayashi/lineage-bridge/main/scripts/quickstart.sh | bash
#
# What it does:
#   1. Installs uv (if needed)
#   2. Clones/updates LineageBridge
#   3. Installs dependencies
#   4. Launches UI in demo mode (sample data, no credentials)
#   5. Opens browser automatically
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────────────

REPO_URL="https://github.com/takabayashi/lineage-bridge.git"
INSTALL_DIR="$HOME/.lineage-bridge"
VENV_DIR="$INSTALL_DIR/.venv"
UI_PORT=8501

# ── Banner ──────────────────────────────────────────────────────────────────

cat <<'EOF'

  ╔═══════════════════════════════════════════════════════════════╗
  ║                                                               ║
  ║   LineageBridge Quickstart                                    ║
  ║   Extract stream lineage from Confluent Cloud                 ║
  ║                                                               ║
  ╚═══════════════════════════════════════════════════════════════╝

EOF

# ── Functions ───────────────────────────────────────────────────────────────

log() {
    echo "  → $*"
}

success() {
    echo "  ✓ $*"
}

error() {
    echo "  ✗ ERROR: $*" >&2
    exit 1
}

check_command() {
    command -v "$1" >/dev/null 2>&1
}

install_uv() {
    if check_command uv; then
        success "uv already installed ($(uv --version))"
        return
    fi

    log "Installing uv package manager..."

    if check_command curl; then
        curl -LsSf https://astral.sh/uv/install.sh | sh
    elif check_command wget; then
        wget -qO- https://astral.sh/uv/install.sh | sh
    else
        error "Neither curl nor wget found. Please install one of them."
    fi

    # Add to PATH for this session
    export PATH="$HOME/.cargo/bin:$PATH"

    if check_command uv; then
        success "uv installed successfully"
    else
        error "uv installation failed. Please install manually: https://docs.astral.sh/uv/"
    fi
}

clone_or_update_repo() {
    if [ -d "$INSTALL_DIR/.git" ]; then
        log "Updating LineageBridge..."
        cd "$INSTALL_DIR"
        git fetch origin main >/dev/null 2>&1 || true
        git reset --hard origin/main >/dev/null 2>&1 || git pull origin main >/dev/null 2>&1 || true
        success "LineageBridge updated"
    else
        log "Downloading LineageBridge..."
        rm -rf "$INSTALL_DIR"
        git clone --depth 1 "$REPO_URL" "$INSTALL_DIR" >/dev/null 2>&1 || \
            error "Failed to clone repository. Check your internet connection."
        success "LineageBridge downloaded to $INSTALL_DIR"
    fi
}

install_dependencies() {
    log "Installing dependencies..."
    cd "$INSTALL_DIR"

    # Create/update venv
    if [ ! -d "$VENV_DIR" ]; then
        uv venv "$VENV_DIR" >/dev/null 2>&1
    fi

    # Install package
    uv pip install -e . >/dev/null 2>&1 || error "Dependency installation failed"

    success "Dependencies installed"
}

wait_for_ui() {
    local max_wait=30
    local waited=0

    log "Waiting for UI to start..."

    while [ $waited -lt $max_wait ]; do
        if curl -s "http://localhost:$UI_PORT/_stcore/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done

    return 1
}

open_browser() {
    local url="http://localhost:$UI_PORT"

    # Wait for UI to be ready
    if wait_for_ui; then
        success "UI is ready!"

        # Open browser (platform-specific)
        if check_command open; then
            open "$url" 2>/dev/null
        elif check_command xdg-open; then
            xdg-open "$url" 2>/dev/null
        elif check_command wslview; then
            wslview "$url" 2>/dev/null
        fi
    else
        log "UI taking longer than expected to start"
    fi
}

# ── Main ────────────────────────────────────────────────────────────────────

main() {
    # Check prerequisites
    if ! check_command git; then
        error "git not found. Please install git first."
    fi

    # Install uv
    install_uv

    # Clone or update repo
    clone_or_update_repo

    # Install dependencies
    install_dependencies

    # Create .env if needed (empty is fine for demo mode)
    if [ ! -f "$INSTALL_DIR/.env" ]; then
        touch "$INSTALL_DIR/.env"
    fi

    # Launch UI
    cat <<EOF

  ╔═══════════════════════════════════════════════════════════════╗
  ║                                                               ║
  ║   Starting LineageBridge UI in DEMO MODE                      ║
  ║                                                               ║
  ║   → No credentials needed                                     ║
  ║   → Sample lineage graph pre-loaded                           ║
  ║   → Click "Load Demo Graph" to explore                        ║
  ║                                                               ║
  ║   To connect to Confluent Cloud:                              ║
  ║   1. Stop the UI (Ctrl+C)                                     ║
  ║   2. Run: cd $INSTALL_DIR && make ui                          ║
  ║   3. Follow the credential setup wizard                       ║
  ║                                                               ║
  ╚═══════════════════════════════════════════════════════════════╝

EOF

    log "Launching UI at http://localhost:$UI_PORT"
    echo ""

    # Open browser in background after delay
    (sleep 3 && open_browser) &

    # Run UI
    cd "$INSTALL_DIR"
    export PATH="$VENV_DIR/bin:$PATH"
    exec uv run streamlit run lineage_bridge/ui/app.py --server.port "$UI_PORT" --server.headless true
}

# ── Entry Point ─────────────────────────────────────────────────────────────

main "$@"
