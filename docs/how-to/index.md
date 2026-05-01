# How-To Guides

Practical, task-oriented guides for common LineageBridge operations.

## What You'll Find Here

How-to guides are **focused on getting things done**. Each guide walks you through a specific task with step-by-step instructions, expected outputs, and troubleshooting tips. Unlike tutorials (which teach) or references (which explain), these guides assume you understand the basics and need help with a particular workflow.

## Guide Categories

### Setup & Configuration

These guides help you configure LineageBridge for different deployment scenarios:

| Guide | Use When |
|-------|----------|
| [Multi-Environment Setup](multi-environment-setup.md) | You need to extract lineage from multiple Confluent Cloud environments simultaneously |
| [Credential Management](credential-management.md) | You're setting up credentials, rotating API keys, or managing secrets in CI/CD |
| [Docker Deployment](docker-deployment.md) | You want to run LineageBridge in containers for production or reproducible extractions |

### Automation & Integration

These guides help you integrate LineageBridge into your existing workflows:

| Guide | Use When |
|-------|----------|
| [CI/CD Integration](ci-cd-integration.md) | You want to automate lineage extraction, run it on a schedule, or publish artifacts |

## Common Scenarios

**"I have multiple Confluent Cloud environments and want to see lineage across all of them."**
→ Start with [Multi-Environment Setup](multi-environment-setup.md). You'll learn how auto-discovery works and how to configure per-cluster credentials.

**"I need to rotate my Confluent Cloud API keys without breaking my extraction pipelines."**
→ Check [Credential Management](credential-management.md) for the credential resolution order and rotation procedures.

**"I want to run lineage extraction in a Docker container on a schedule."**
→ Combine [Docker Deployment](docker-deployment.md) and [CI/CD Integration](ci-cd-integration.md) for a complete solution.

**"I want to set up LineageBridge to automatically re-extract lineage when my Kafka topology changes."**
→ See the watcher section in [Docker Deployment](docker-deployment.md) and the change detection triggers in [CI/CD Integration](ci-cd-integration.md).

**"I'm getting credential errors and don't know which API key is being used."**
→ The credential resolution order is documented in [Credential Management](credential-management.md) under "Understanding Credential Resolution".

**"I want to run LineageBridge in GitHub Actions and publish the lineage graph as an artifact."**
→ Follow the GitHub Actions example in [CI/CD Integration](ci-cd-integration.md).

## Getting Started

If you're new to LineageBridge:

1. Read the [Quickstart Tutorial](../tutorials/quickstart.md) to understand the basics
2. Review the [Architecture Overview](../reference/architecture.md) to understand how components fit together
3. Pick a guide from the table above based on your current task

## Style Conventions

Throughout these guides:

- **Commands** are shown in code blocks with `$` prompts
- **Environment variables** use `LINEAGE_BRIDGE_` prefix
- **Expected output** appears after each step to help you verify progress
- **Troubleshooting** sections address common issues
- **Tabs** show alternative approaches (UI vs CLI, different platforms)

## Need Help?

- If a guide doesn't cover your use case, check the [FAQ](../reference/faq.md)
- For API details, see the [API Reference](../reference/api.md)
- For conceptual explanations, see [Concepts](../reference/concepts.md)
- To report issues or suggest improvements, [open an issue](https://github.com/takabayashi/lineage-bridge/issues)
