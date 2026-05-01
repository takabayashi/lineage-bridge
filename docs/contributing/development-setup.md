# Development Setup

This guide walks you through setting up a local development environment for LineageBridge.

## Prerequisites

### Required

- **Python 3.11 or later** - Check your version:
  ```bash
  python --version
  ```
  
  If you need to install Python 3.11+, we recommend:
  - macOS: `brew install python@3.11`
  - Linux: `pyenv install 3.11.7`
  - Windows: Download from [python.org](https://www.python.org/downloads/)

- **uv** (recommended) - Fast Python package installer:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
  
  Alternatively, you can use pip, but uv is significantly faster for dependency installation.

### Optional but Recommended

- **Git** for version control
- **Make** for using Makefile targets
- **Confluent Cloud account** for integration testing

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/takabayashi/lineage-bridge.git
cd lineage-bridge
```

If you're contributing, fork the repository first and clone your fork:

```bash
git clone https://github.com/YOUR-USERNAME/lineage-bridge.git
cd lineage-bridge
git remote add upstream https://github.com/takabayashi/lineage-bridge.git
```

### 2. Create a Virtual Environment

We recommend using uv to create and manage your virtual environment:

```bash
# uv will automatically create a .venv if it doesn't exist
uv venv
```

Alternatively, with standard Python:

```bash
python -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate     # On Windows
```

### 3. Install Dependencies

Install LineageBridge in editable mode with development dependencies:

```bash
# Using uv (recommended)
uv pip install -e ".[dev]"

# Or using pip
pip install -e ".[dev]"
```

This installs:
- **Core dependencies**: httpx, networkx, pydantic, streamlit, etc.
- **Dev dependencies**: pytest, pytest-asyncio, pytest-cov, respx, ruff

### 4. Verify Installation

Check that everything is installed correctly:

```bash
# Check Python package
python -c "import lineage_bridge; print(lineage_bridge.__version__)"

# Run tests to verify setup
make test
# or
uv run pytest tests/ -v
```

## Configuration

### Environment Variables

Copy the example `.env` file and configure your credentials:

```bash
cp .env.example .env
```

Edit `.env` and add your Confluent Cloud credentials:

```bash
# Confluent Cloud credentials
LINEAGE_BRIDGE_CLOUD_API_KEY=your-cloud-api-key
LINEAGE_BRIDGE_CLOUD_API_SECRET=your-cloud-api-secret

# Optional: Databricks Unity Catalog
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=your-databricks-token

# Optional: AWS Glue
LINEAGE_BRIDGE_AWS_REGION=us-east-1
LINEAGE_BRIDGE_AWS_GLUE_DATABASE=your-glue-database
```

**Note**: Only credentials go in `.env`. Environment and cluster selection happens in the UI or CLI.

### Auto-Provisioning API Keys

LineageBridge can auto-provision Kafka cluster API keys using the Confluent CLI:

1. Install Confluent CLI:
   ```bash
   # macOS
   brew install confluentinc/tap/cli
   
   # Other platforms: https://docs.confluent.io/confluent-cli/current/install.html
   ```

2. Log in to Confluent Cloud:
   ```bash
   confluent login
   ```

3. Start the UI - it will auto-provision keys as needed:
   ```bash
   make ui
   ```

## Makefile Targets

The project includes a comprehensive Makefile for common tasks:

### Development

```bash
make install      # Install project with dev dependencies
make test         # Run all tests
make lint         # Run ruff linter
make format       # Auto-format code and fix lint issues
make clean        # Remove build artifacts and caches
```

### Running

```bash
make ui           # Start Streamlit UI (auto-provisions API keys)
make extract      # Run CLI extraction
make watch        # Run change-detection watcher
make api          # Start REST API server
```

### Documentation

```bash
make docs-serve   # Serve docs locally at localhost:8001
make docs-build   # Build static docs site
make docs-install # Install documentation dependencies
```

### Docker

```bash
make docker-build    # Build Docker images
make docker-ui       # Start UI via Docker
make docker-extract  # Run extraction via Docker
make docker-watch    # Run watcher via Docker
make docker-down     # Stop all Docker services
```

### Demo Infrastructure

```bash
make demo-uc-up      # Provision Unity Catalog demo
make demo-glue-up    # Provision AWS Glue demo
make demo-bq-up      # Provision BigQuery demo
make demo-uc-down    # Tear down demo
```

Run `make help` to see all available targets.

## IDE Setup

### VS Code (Recommended)

Create `.vscode/settings.json`:

```json
{
  "python.defaultInterpreterPath": ".venv/bin/python",
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": ["tests/"],
  "[python]": {
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
      "source.organizeImports": "explicit"
    },
    "editor.defaultFormatter": "charliermarsh.ruff"
  },
  "ruff.lint.enable": true,
  "ruff.format.enable": true
}
```

Install the Ruff extension:
- Open VS Code
- Go to Extensions (Cmd+Shift+X)
- Search for "Ruff" by Astral Software
- Install

### PyCharm

1. Set Python interpreter:
   - File > Settings > Project > Python Interpreter
   - Add Interpreter > Existing Environment
   - Select `.venv/bin/python`

2. Configure pytest:
   - File > Settings > Tools > Python Integrated Tools
   - Default test runner: pytest

3. Configure Ruff:
   - File > Settings > Tools > External Tools
   - Add new tool for "Ruff Format" and "Ruff Check"

## Pre-commit Hooks (Optional)

Set up automatic linting and formatting on commit:

```bash
# Create .git/hooks/pre-commit
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
set -e

echo "Running ruff format..."
uv run ruff format .

echo "Running ruff check..."
uv run ruff check --fix .

echo "Running tests..."
uv run pytest tests/ -v
EOF

chmod +x .git/hooks/pre-commit
```

## Virtual Environment Best Practices

### Activating the Environment

Always activate your virtual environment before working:

```bash
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

With uv, you can also run commands without activating:

```bash
uv run pytest tests/ -v
uv run streamlit run lineage_bridge/ui/app.py
```

### Keeping Dependencies Updated

Update dependencies periodically:

```bash
# Using uv
uv pip install --upgrade -e ".[dev]"

# Using pip
pip install --upgrade -e ".[dev]"
```

### Deactivating

When you're done:

```bash
deactivate
```

## Troubleshooting

### "Command not found: uv"

Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then restart your shell or run:
```bash
source ~/.bashrc  # or ~/.zshrc
```

### "No module named 'lineage_bridge'"

Make sure you installed in editable mode:
```bash
uv pip install -e ".[dev]"
```

### Tests fail with import errors

Reinstall dependencies:
```bash
make clean
uv pip install -e ".[dev]"
```

### Ruff errors on save

Make sure the Ruff extension is installed and enabled in your IDE.

### "Permission denied" on Makefile

Ensure you have execution permissions:
```bash
chmod +x Makefile
```

## Next Steps

- [Run the test suite](testing.md) to verify everything works
- [Add a new extractor](adding-extractors.md) to integrate a new data source
- Check out the [main contributing guide](index.md) for PR guidelines

---

**Questions?** Open a GitHub Discussion or file an issue.
