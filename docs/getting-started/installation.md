# Installation

Let's get LineageBridge running on your machine. We'll keep this simple and get you up and running in just a few minutes.

## What You Need

Here's what you'll need before starting:

### System Requirements

- **Python 3.11 or newer** - We develop on 3.11, so that's what we recommend. Check your version with `python --version`
- **Git** - You'll use this to clone the repo
- **4GB RAM minimum** - Enough to run the Streamlit UI comfortably
- **Internet access** - To talk to Confluent Cloud and data catalog APIs

### Confluent Cloud Account

You'll need:

- A Confluent Cloud account (grab a free trial at [confluent.cloud](https://confluent.cloud))
- At least one Kafka cluster running somewhere
- Cloud API credentials with read permissions (OrgAdmin or EnvironmentAdmin role)

### Optional: Data Catalog Access

If you want to connect lineage to your data catalog, you'll need:

- **Databricks Unity Catalog**: Workspace URL + personal access token
- **AWS Glue**: AWS credentials with Glue read permissions
- **Google Data Lineage**: GCP project with Data Lineage API enabled

!!! tip "Getting API Credentials"
    Head to [Configuration Guide](configuration.md#confluent-cloud-credentials) for step-by-step instructions on creating API keys in Confluent Cloud.

## Installation Methods

We support three ways to install LineageBridge. Pick whichever fits your workflow best.

### Choose Your Method

=== "uv (Recommended)"

    We use [uv](https://docs.astral.sh/uv/) for development—it's blazing fast and handles dependencies beautifully.

    **First, install uv if you don't have it:**

    ```bash
    # macOS/Linux
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Windows
    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

    # Or use pip
    pip install uv
    ```

    **Then install LineageBridge:**

    ```bash
    # Clone the repo
    git clone https://github.com/takabayashi/lineage-bridge.git
    cd lineage-bridge

    # Basic installation
    uv pip install -e .

    # Or with dev tools (recommended if you'll be contributing)
    uv pip install -e ".[dev]"

    # Or with everything (dev + docs)
    uv pip install -e ".[dev,docs]"
    ```

    !!! tip "Why the `-e` flag?"
        Editable mode means changes to source code take effect immediately—no reinstalling needed. Handy for tinkering!

=== "pip"

    Good old pip works perfectly fine if you don't want to install uv.

    ```bash
    # Clone the repo
    git clone https://github.com/takabayashi/lineage-bridge.git
    cd lineage-bridge

    # Basic installation
    pip install -e .

    # With dev dependencies
    pip install -e ".[dev]"
    ```

=== "Make"

    For the fastest setup, use our Makefile (requires `make` installed):

    ```bash
    # Clone the repo
    git clone https://github.com/takabayashi/lineage-bridge.git
    cd lineage-bridge

    # One command to rule them all
    make install
    ```

    This automatically:
    
    1. Creates a virtual environment if needed
    2. Installs the package with dev dependencies
    3. Sets up all CLI entry points

=== "Docker"

    Prefer containers? We've got you covered.

    ```bash
    # Clone the repo
    git clone https://github.com/takabayashi/lineage-bridge.git
    cd lineage-bridge

    # Build all images
    make docker-build

    # Or build manually
    docker compose -f infra/docker/docker-compose.yml build
    ```

    See [Docker Installation](#docker-installation) below for running instructions.

## Verify Everything Works

Let's make sure the installation succeeded.

```bash
# Check CLI tools are available
lineage-bridge-extract --help
lineage-bridge-watch --help
lineage-bridge-api --help

# Check the package version
python -c "from lineage_bridge import __version__; print(__version__)"
```

You should see version `0.4.0` (or higher). If all commands work, you're good to go!

## Docker Installation

If you prefer containers, we provide pre-built Docker images for all components.

### Prerequisites

You'll need Docker 20.10+ and Docker Compose 2.0+.

### Run with Docker

Here's how to run each component:

=== "Streamlit UI"

    ```bash
    # Using Makefile
    make docker-ui

    # Or manually
    docker compose -f infra/docker/docker-compose.yml up ui
    ```

    Open [http://localhost:8501](http://localhost:8501) to access the UI.

=== "CLI Extraction"

    ```bash
    # Using Makefile
    make docker-extract

    # Or manually
    docker compose -f infra/docker/docker-compose.yml run extract
    ```

=== "Watcher Service"

    ```bash
    # Using Makefile
    make docker-watch

    # Or manually
    docker compose -f infra/docker/docker-compose.yml up watch
    ```

!!! warning "Don't Forget Credentials"
    Docker reads from your `.env` file in the project root. Make sure it's set up before running containers. See [Configuration](configuration.md) for details.

## Next Steps

Installation done! Now let's get your credentials configured and extract some lineage.

- **[Configuration →](configuration.md)** - Set up your API credentials
- **[Quickstart →](quickstart.md)** - Extract your first lineage graph

## Troubleshooting

### Python Version Issues

If you see Python version errors:

```bash
# Check what you're running
python --version

# Tell uv to use a specific Python version
uv pip install --python=3.11 -e .
```

### Virtual Environment Issues

Prefer to manage your own virtual environment? No problem:

```bash
# Create and activate a venv
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Then install normally
pip install -e ".[dev]"
```

### Permission Errors

Getting permission denied errors on macOS/Linux? **Don't use sudo.** Use a virtual environment instead:

```bash
# Create a venv first
python -m venv .venv
source .venv/bin/activate

# Now install
pip install -e ".[dev]"
```

### Import Errors

If Python can't find the package after installation:

```bash
# Make sure you're in the right directory
cd lineage-bridge

# Reinstall in editable mode
pip install -e .
```

### Still Stuck?

Check out the full [Troubleshooting Guide](../troubleshooting/index.md) or open an issue on GitHub.
