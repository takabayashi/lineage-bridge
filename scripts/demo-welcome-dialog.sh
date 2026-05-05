#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Demo the welcome dialog by temporarily removing credentials
#
# Usage:
#   ./scripts/demo-welcome-dialog.sh
#
# What it does:
#   1. Backs up .env (if it exists)
#   2. Launches the UI without credentials (triggers welcome dialog)
#   3. Waits for you to press Ctrl+C
#   4. Restores .env
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ENV_FILE=".env"
BACKUP_FILE=".env.demo-backup"

# ── Cleanup function ─────────────────────────────────────────────────────────

cleanup() {
    echo ""
    echo "  → Cleaning up..."

    # Kill streamlit if still running
    pkill -f "streamlit run lineage_bridge/ui/app.py" 2>/dev/null || true

    # Restore .env
    if [ -f "$BACKUP_FILE" ]; then
        mv "$BACKUP_FILE" "$ENV_FILE"
        echo "  ✓ Restored $ENV_FILE"
    fi

    echo "  ✓ Demo cleanup complete"
}

trap cleanup EXIT INT TERM

# ── Main ─────────────────────────────────────────────────────────────────────

cat <<'EOF'

  ╔═══════════════════════════════════════════════════════════════╗
  ║                                                               ║
  ║   Welcome Dialog Demo                                         ║
  ║                                                               ║
  ╚═══════════════════════════════════════════════════════════════╝

  This script will:
  1. Temporarily remove your .env file
  2. Launch the UI (triggering the welcome dialog)
  3. Open http://localhost:8501 in your browser

  You'll see a dialog with 3 options:

  • 💾 Save & Connect    — Enter cloud credentials
  • ⏭️  Skip for Now      — Dismiss dialog, use sidebar later
  • 🎨 Load Demo Graph   — Instant demo mode

  Press Ctrl+C when done to restore your .env file.

EOF

read -p "  Press Enter to start the demo..."

# Backup .env
if [ -f "$ENV_FILE" ]; then
    cp "$ENV_FILE" "$BACKUP_FILE"
    rm "$ENV_FILE"
    echo "  → .env backed up to $BACKUP_FILE"
fi

# Launch UI
echo ""
echo "  → Launching UI without credentials..."
echo "  → The welcome dialog should appear automatically"
echo ""

uv run streamlit run lineage_bridge/ui/app.py

# Cleanup runs automatically via trap
