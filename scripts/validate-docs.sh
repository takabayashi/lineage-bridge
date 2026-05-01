#!/bin/bash
# Automated documentation validation for LineageBridge
# Tests commands, examples, and configuration against actual implementation

set -e

PROJECT_ROOT="/Users/taka/projects/petprojects/lineage-bridge"
cd "$PROJECT_ROOT"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() {
    echo -e "${GREEN}✓${NC} $1"
    ((PASS_COUNT++))
}

fail() {
    echo -e "${RED}✗${NC} $1"
    ((FAIL_COUNT++))
}

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
    ((WARN_COUNT++))
}

section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "$1"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# ── Test CLI Commands ──────────────────────────────────────────────

section "1. CLI Commands Validation"

# Test each CLI command exists and has --help
for cmd in lineage-bridge-extract lineage-bridge-watch lineage-bridge-ui lineage-bridge-api; do
    if uv run $cmd --help >/dev/null 2>&1; then
        pass "Command exists: $cmd"
    else
        # Special case for UI - it doesn't support --help
        if [ "$cmd" = "lineage-bridge-ui" ]; then
            if uv run python -c "from lineage_bridge.ui.app import run; print('ok')" 2>/dev/null | grep -q "ok"; then
                pass "Command exists: $cmd (entry point verified)"
            else
                fail "Command exists: $cmd"
            fi
        else
            fail "Command exists: $cmd"
        fi
    fi
done

# ── Test Makefile Targets ──────────────────────────────────────────

section "2. Makefile Targets Validation"

# Test that make help works
if make help >/dev/null 2>&1; then
    pass "make help"
else
    fail "make help"
fi

# Test key targets exist (dry-run)
for target in install test lint format clean docs-build; do
    if make -n $target >/dev/null 2>&1; then
        pass "Makefile target: $target"
    else
        fail "Makefile target: $target"
    fi
done

# ── Test Environment Variables ──────────────────────────────────────

section "3. Environment Variables Validation"

# Extract all LINEAGE_BRIDGE_ variables from .env.example
ENV_VARS=$(grep "^LINEAGE_BRIDGE_" .env.example | cut -d= -f1 | grep -v "^#" || true)

# Check each variable is documented in settings.py
for var in $ENV_VARS; do
    # Remove LINEAGE_BRIDGE_ prefix and convert to lowercase, then to snake_case field name
    field_name=$(echo "$var" | sed 's/LINEAGE_BRIDGE_//' | tr '[:upper:]' '[:lower:]')

    if grep -q "$field_name:" lineage_bridge/config/settings.py; then
        pass "Environment variable: $var"
    else
        fail "Environment variable: $var (not found as $field_name in settings.py)"
    fi
done

# ── Test Installation Commands ─────────────────────────────────────

section "4. Installation Commands Validation"

# Test that the package is installed
if uv run python -c "import lineage_bridge; print('ok')" 2>/dev/null | grep -q "ok"; then
    pass "Package import: lineage_bridge"
else
    fail "Package import: lineage_bridge"
fi

# Test key modules exist
for module in config clients extractors models ui api; do
    if uv run python -c "import lineage_bridge.$module; print('ok')" 2>/dev/null | grep -q "ok"; then
        pass "Module import: lineage_bridge.$module"
    else
        fail "Module import: lineage_bridge.$module"
    fi
done

# ── Test pyproject.toml Scripts ────────────────────────────────────

section "5. pyproject.toml Scripts Validation"

# Extract script names from pyproject.toml
SCRIPTS=$(grep -A 10 "\[project.scripts\]" pyproject.toml | grep "lineage-bridge-" | cut -d= -f1 | tr -d ' ')

for script in $SCRIPTS; do
    if uv run which $script >/dev/null 2>&1; then
        pass "Script registered: $script"
    else
        fail "Script registered: $script"
    fi
done

# ── Test Documentation Files ───────────────────────────────────────

section "6. Documentation Files Validation"

# Check key documentation files exist
for doc in docs/index.md README.md CONTRIBUTING.md; do
    if [ -f "$doc" ]; then
        pass "Documentation file: $doc"
    else
        fail "Documentation file: $doc"
    fi
done

# Check documentation structure
for dir in docs/getting-started docs/user-guide docs/api-reference docs/catalog-integration; do
    if [ -d "$dir" ]; then
        pass "Documentation directory: $dir"
    else
        fail "Documentation directory: $dir"
    fi
done

# ── Test CLI Flag Consistency ──────────────────────────────────────

section "7. CLI Flags Validation"

# Test lineage-bridge-extract flags match docs
EXTRACT_HELP=$(uv run lineage-bridge-extract --help 2>&1)
for flag in --env --cluster --output --no-enrich --enrich-only --push-lineage; do
    if echo "$EXTRACT_HELP" | grep -- "$flag" >/dev/null 2>&1; then
        pass "lineage-bridge-extract flag: $flag"
    else
        fail "lineage-bridge-extract flag: $flag (not found in --help)"
    fi
done

# Test lineage-bridge-watch flags match docs
WATCH_HELP=$(uv run lineage-bridge-watch --help 2>&1)
for flag in --env --cluster --cooldown --poll-interval --push-uc --push-glue; do
    if echo "$WATCH_HELP" | grep -- "$flag" >/dev/null 2>&1; then
        pass "lineage-bridge-watch flag: $flag"
    else
        fail "lineage-bridge-watch flag: $flag (not found in --help)"
    fi
done

# ── Test File Paths in Documentation ───────────────────────────────

section "8. File Paths in Documentation"

# Check that common file paths mentioned in docs exist
for path in pyproject.toml Makefile .env.example lineage_bridge/ui/app.py; do
    if [ -e "$path" ]; then
        pass "File path: $path"
    else
        fail "File path: $path"
    fi
done

# ── Summary ─────────────────────────────────────────────────────────

section "Validation Summary"

TOTAL=$((PASS_COUNT + FAIL_COUNT + WARN_COUNT))
PASS_PCT=$(( (PASS_COUNT * 100) / TOTAL ))

echo ""
echo "Total tests:    $TOTAL"
echo -e "${GREEN}Passed:${NC}         $PASS_COUNT ($PASS_PCT%)"
if [ $FAIL_COUNT -gt 0 ]; then
    echo -e "${RED}Failed:${NC}         $FAIL_COUNT"
fi
if [ $WARN_COUNT -gt 0 ]; then
    echo -e "${YELLOW}Warnings:${NC}       $WARN_COUNT"
fi
echo ""

if [ $FAIL_COUNT -eq 0 ]; then
    echo -e "${GREEN}✓ All validation checks passed!${NC}"
    echo ""
    echo "Confidence level: $PASS_PCT%"
    exit 0
else
    echo -e "${RED}✗ $FAIL_COUNT validation checks failed${NC}"
    echo ""
    echo "Please review the failed checks above."
    exit 1
fi
