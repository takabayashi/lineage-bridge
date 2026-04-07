# Contributing to LineageBridge

Thank you for your interest in contributing to LineageBridge! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- A Confluent Cloud account (for integration testing)

### Install

```bash
# Clone the repository
git clone https://github.com/takabayashi/lineage-bridge.git
cd lineage-bridge

# Install with dev dependencies (uv recommended)
uv pip install -e ".[dev]"
```

### Configuration

Copy `.env.example` to `.env` and fill in your Confluent Cloud credentials:

```bash
cp .env.example .env
```

Only credentials go in `.env` — environment and cluster selection happens in the UI.

## Running

```bash
# Start the Streamlit UI
uv run streamlit run lineage_bridge/ui/app.py

# CLI extraction
uv run lineage-bridge-extract

# Change-detection watcher
uv run lineage-bridge-watch
```

## Testing

```bash
# Run all tests
uv run pytest tests/ -v

# Run a specific test file
uv run pytest tests/unit/test_flink_client.py -v

# Run with coverage
uv run pytest --cov=lineage_bridge --cov-report=term-missing
```

All tests must pass before submitting a PR.

## Code Style

We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
# Check for lint issues
uv run ruff check .

# Auto-fix lint issues
uv run ruff check . --fix

# Check formatting
uv run ruff format --check .

# Auto-format
uv run ruff format .
```

Configuration is in `pyproject.toml`:
- Target: Python 3.11+
- Line length: 100 characters

## Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting (`pytest tests/ -v && ruff check .`)
5. Commit with a descriptive message
6. Push to your fork and open a Pull Request

### PR Guidelines

- Keep PRs focused — one feature or fix per PR
- Include tests for new functionality
- Update documentation if adding new features or changing behavior
- Describe what changed and why in the PR description

## Project Structure

```
lineage_bridge/
  clients/          # Confluent Cloud + Databricks API clients (async httpx)
  catalogs/         # Data catalog providers (UC, Glue) — extensible
  config/           # Settings, credentials, caching, key provisioning
  extractors/       # Orchestration pipeline (5-phase extraction)
  models/           # Pydantic data models (nodes, edges, graph, audit events)
  ui/               # Streamlit app + vis.js graph component
  watcher/          # Change-detection engine + CLI
tests/
  unit/             # Unit tests (mocked HTTP via respx)
  integration/      # Integration tests (live APIs, manual trigger)
```

## Reporting Issues

Please open an issue on GitHub with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Python version and OS

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
