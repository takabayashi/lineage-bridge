# Contributing to LineageBridge

Thank you for your interest in contributing to LineageBridge! We welcome contributions of all kinds - bug reports, feature requests, documentation improvements, and code contributions.

## Quick Start

1. Read the [Development Setup](development-setup.md) guide
2. Check the [Testing](testing.md) guide for running tests
3. If adding new extractors, see [Adding Extractors](adding-extractors.md)
4. Submit your PR following the guidelines below

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please be respectful and constructive in all interactions.

## How to Contribute

### Reporting Bugs

If you find a bug, please open a GitHub issue with:

- **Clear title**: Describe the issue concisely
- **Expected behavior**: What you expected to happen
- **Actual behavior**: What actually happened
- **Reproduction steps**: Step-by-step instructions to reproduce the issue
- **Environment details**: Python version, OS, LineageBridge version
- **Logs**: Any relevant error messages or stack traces

**Example:**

```
Title: FlinkClient fails to parse TUMBLE windowing functions

Expected: Flink SQL with TUMBLE(TABLE ...) should extract table as source
Actual: Parser raises IndexError on TUMBLE clause
Steps: Run extraction on environment env-xyz with Flink statement flink-abc123
Environment: Python 3.11.5, macOS 14.2, LineageBridge 0.4.0
Logs: [paste stack trace]
```

### Requesting Features

Feature requests are welcome! Please open an issue describing:

- **Use case**: What problem does this solve?
- **Proposed solution**: How should it work?
- **Alternatives considered**: Other approaches you've thought about
- **Impact**: Who would benefit from this feature?

### Contributing Code

We follow a standard fork-and-pull-request workflow:

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/lineage-bridge.git
   cd lineage-bridge
   ```
3. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/my-feature
   ```
4. **Make your changes** and commit with clear messages:
   ```bash
   git add -A
   git commit -m "Add support for Flink HOP windowing"
   ```
5. **Run tests and linting** before pushing:
   ```bash
   make test
   make lint
   ```
6. **Push to your fork**:
   ```bash
   git push origin feature/my-feature
   ```
7. **Open a Pull Request** on GitHub

## Pull Request Guidelines

### Before Submitting

- Run tests: `make test` or `uv run pytest tests/ -v`
- Check linting: `make lint` or `uv run ruff check .`
- Format code: `make format` or `uv run ruff format .`
- Update documentation if adding features or changing behavior
- Add tests for new functionality

### PR Description

Your PR description should include:

- **What changed**: Brief summary of the changes
- **Why**: Rationale and motivation
- **How**: Implementation approach (for complex changes)
- **Testing**: How you tested the changes
- **Breaking changes**: Note any API changes or migrations needed

**Example:**

```markdown
## What
Add support for Google BigQuery as a catalog provider

## Why
Users want to materialize Kafka topics to BigQuery via Dataflow
and track lineage in BigQuery's native metadata system.

## How
- Implemented BigQueryProvider following the CatalogProvider protocol
- Added BigQuery client using google-cloud-bigquery SDK
- Set table descriptions and labels via BigQuery API

## Testing
- Unit tests with mocked BigQuery client
- Integration test against real BigQuery project (manual)
- Added fixture data in tests/fixtures/bigquery_tables.json

## Breaking Changes
None - purely additive.
```

### Review Process

- All PRs require review before merging
- Reviewers may request changes - please respond constructively
- Once approved, a maintainer will merge your PR
- Keep PRs focused - one feature or fix per PR

## Areas Needing Contribution

Here are some areas where we'd especially welcome contributions:

### New Catalog Providers

- **Google BigQuery**: Materialize to BigQuery via Dataflow
- **Snowflake**: Support Snowpipe streaming
- **Azure Data Explorer**: Kafka Connect integration
- **Iceberg REST Catalog**: Direct lineage to Iceberg tables

See [Adding Extractors](adding-extractors.md) for guidance.

### Enhanced Extractors

- **Schema Registry**: Subject references, schema evolution tracking
- **ksqlDB**: Stream-table joins, windowing functions
- **Flink**: Complex window patterns, catalog integrations
- **Tableflow**: Multi-topic joins, transformation tracking

### UI Improvements

- Graph layout algorithms (force-directed, hierarchical)
- Advanced filtering (by tags, metadata, date ranges)
- Export formats (SVG, PNG, Mermaid diagram)
- Diff view for lineage changes over time

### Testing & Infrastructure

- Integration test coverage for all extractors
- Performance benchmarks for large graphs
- Docker Compose local development setup
- CI/CD pipeline improvements

## Development Workflow

### Setting Up Your Environment

See [Development Setup](development-setup.md) for detailed instructions.

Quick version:

```bash
# Install dependencies
uv pip install -e ".[dev]"

# Run tests
make test

# Run UI locally
make ui

# Run extraction
make extract
```

### Coding Standards

- **Python 3.11+** required
- **Type hints** on all public functions
- **Docstrings** for public modules, classes, functions
- **Ruff** for linting and formatting (line length: 100 chars)
- **pytest** for tests (asyncio_mode = "auto")

### Commit Messages

Write clear, descriptive commit messages:

- Use present tense: "Add feature" not "Added feature"
- Keep first line under 72 characters
- Add details in the body if needed

**Good:**
```
Add HOP windowing support to FlinkClient

Extends the Flink SQL parser to recognize HOP window functions
and extract the source table from HOP(TABLE <name>, ...).
```

**Avoid:**
```
Fix stuff
Updated flink
wip
```

## Community & Support

- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: Questions and general discussion
- **Pull Requests**: Code contributions

## License

By contributing to LineageBridge, you agree that your contributions will be licensed under the Apache License 2.0.

---

**Ready to contribute?** Start with the [Development Setup](development-setup.md) guide, or browse open issues tagged with `good-first-issue` on GitHub.
