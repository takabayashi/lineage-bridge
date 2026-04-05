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

# Install with dev dependencies
uv pip install -e ".[dev]"

# Or with pip
pip install -e ".[dev]"
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
streamlit run lineage_bridge/ui/app.py

# Or via the CLI entry point
lineage-bridge-ui
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run a specific test file
pytest tests/unit/test_flink_client.py -v

# Run with coverage
pytest tests/ --cov=lineage_bridge --cov-report=term-missing
```

All tests must pass before submitting a PR.

## Code Style

We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
# Check for lint issues
ruff check .

# Auto-fix lint issues
ruff check . --fix

# Check formatting
ruff format --check .

# Auto-format
ruff format .
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
  clients/          # Confluent Cloud API clients (one per service)
  config/           # Settings, credentials, caching
  extractors/       # Orchestration layer
  models/           # Pydantic data models (nodes, edges, graph)
  ui/               # Streamlit app + vis.js graph component
tests/
  unit/             # Unit tests (mocked HTTP via respx)
```

## Reporting Issues

Please open an issue on GitHub with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Python version and OS

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
