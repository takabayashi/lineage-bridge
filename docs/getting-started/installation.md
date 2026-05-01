# Installation

This guide walks you through installing LineageBridge on your local machine or server.

## Prerequisites

Before installing LineageBridge, ensure you have the following:

### System Requirements

- **Python 3.11 or higher** - Check your version with `python --version`
- **Git** - For cloning the repository
- **4GB RAM minimum** - For running the Streamlit UI and extraction processes
- **Internet access** - To communicate with Confluent Cloud and catalog APIs

### Confluent Cloud Account

You'll need:

- A Confluent Cloud account (sign up at [confluent.cloud](https://confluent.cloud))
- At least one Kafka cluster in an environment
- Cloud-level API credentials with read permissions (OrgAdmin or EnvironmentAdmin)

### Optional: Data Catalog Access

For catalog integration features, you may also need:

- **Databricks Unity Catalog**: Workspace URL and personal access token
- **AWS Glue**: AWS credentials with Glue read permissions
- **Google Data Lineage**: GCP project with Data Lineage API enabled

!!! tip "Getting API Credentials"
    See the [Configuration Guide](configuration.md#confluent-cloud-credentials) for detailed instructions on creating API keys in Confluent Cloud.

## Installation Methods

LineageBridge supports two installation methods: **uv** (recommended) and **pip**.

### Option 1: Install with uv (Recommended)

[uv](https://docs.astral.sh/uv/) is a fast Python package installer and resolver. It's the recommended way to install LineageBridge.

#### Step 1: Install uv

If you don't have uv installed:

=== "macOS/Linux"
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

=== "Windows"
    ```powershell
    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

=== "With pip"
    ```bash
    pip install uv
    ```

#### Step 2: Clone the Repository

```bash
git clone https://github.com/takabayashi/lineage-bridge.git
cd lineage-bridge
```

#### Step 3: Install LineageBridge

For basic usage:

```bash
uv pip install -e .
```

For development (includes testing and linting tools):

```bash
uv pip install -e ".[dev]"
```

For documentation building:

```bash
uv pip install -e ".[docs]"
```

Or install everything:

```bash
uv pip install -e ".[dev,docs]"
```

!!! note "Editable Install"
    The `-e` flag installs in editable mode, meaning changes to the source code are immediately reflected without reinstalling.

### Option 2: Install with pip

If you prefer using standard pip:

```bash
git clone https://github.com/takabayashi/lineage-bridge.git
cd lineage-bridge
pip install -e .
```

For development dependencies:

```bash
pip install -e ".[dev]"
```

## Using the Makefile (Recommended)

LineageBridge includes a Makefile with convenient shortcuts. To install with all development dependencies:

```bash
make install
```

This automatically:

1. Creates a virtual environment if one doesn't exist
2. Installs the package with dev dependencies
3. Sets up all CLI entry points

## Verifying Installation

After installation, verify that LineageBridge is properly installed:

```bash
# Check CLI tools are available
lineage-bridge-extract --help
lineage-bridge-watch --help
lineage-bridge-api --help

# Check Python import
python -c "from lineage_bridge import __version__; print(__version__)"
```

You should see version `0.4.0` (or higher) printed.

## Docker Installation

For containerized deployments, LineageBridge provides pre-configured Docker images.

### Prerequisites

- Docker 20.10+ and Docker Compose 2.0+

### Build Images

```bash
make docker-build
```

Or manually:

```bash
docker compose -f infra/docker/docker-compose.yml build
```

### Run with Docker

Run the Streamlit UI:

```bash
make docker-ui
```

Run lineage extraction:

```bash
make docker-extract
```

Run the change-detection watcher:

```bash
make docker-watch
```

!!! tip "Environment Variables"
    Docker deployments read credentials from your `.env` file in the project root. See the [Configuration Guide](configuration.md) for details.

## Next Steps

Now that LineageBridge is installed, continue to:

- **[Configuration →](configuration.md)** - Set up your credentials
- **[Quickstart →](quickstart.md)** - Extract your first lineage graph

## Troubleshooting

### Python Version Issues

If you encounter Python version errors:

```bash
# Check your Python version
python --version

# Use a specific Python version with uv
uv pip install --python=3.11 -e .
```

### Virtual Environment Issues

If you prefer managing your own virtual environment:

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Then install
pip install -e ".[dev]"
```

### Permission Errors

On macOS/Linux, if you encounter permission errors:

```bash
# Don't use sudo - use a virtual environment instead
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Import Errors

If you see import errors after installation:

```bash
# Ensure you're in the correct directory
cd lineage-bridge

# Reinstall in editable mode
pip install -e .
```

For more help, see the [Troubleshooting section](../troubleshooting/index.md).
