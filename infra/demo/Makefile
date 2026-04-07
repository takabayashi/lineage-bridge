# LineageBridge Demo Infrastructure
# ──────────────────────────────────────────────────────────────────────────────

SHELL := /bin/bash

# Extract variables from terraform.tfvars for cleanup scripts
DATABRICKS_HOST  := $(shell grep databricks_workspace_url terraform.tfvars 2>/dev/null | sed 's/.*= *"\(.*\)"/\1/')
DATABRICKS_TOKEN := $(shell grep databricks_token terraform.tfvars 2>/dev/null | sed 's/.*= *"\(.*\)"/\1/')
CATALOG_NAME     := $(shell terraform state show 'databricks_catalog.demo' >/dev/null 2>&1 && terraform output -raw databricks_catalog_name 2>/dev/null || echo "")

.DEFAULT_GOAL := help

.PHONY: demo-up demo-down clean-catalog clean status env help

demo-up: ## Provision everything from scratch (init + apply + Tableflow key bootstrap)
	bash scripts/provision-demo.sh

demo-down: ## Tear down everything (catalog cleanup + terraform destroy)
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  LineageBridge Demo — Destroying"
	@echo "══════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "▸ Cleaning up auto-created Databricks schemas..."
	@if [ -n "$(CATALOG_NAME)" ] && [ -n "$(DATABRICKS_HOST)" ]; then \
		DATABRICKS_HOST="$(DATABRICKS_HOST)" \
		DATABRICKS_TOKEN="$(DATABRICKS_TOKEN)" \
		CATALOG_NAME="$(CATALOG_NAME)" \
		bash scripts/cleanup-catalog.sh || true; \
	else \
		echo "  Skipped (catalog already destroyed)"; \
	fi
	@echo ""
	@echo "▸ Destroying Terraform resources..."
	terraform destroy -auto-approve
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Demo destroyed."
	@echo "══════════════════════════════════════════════════════════════════"

clean: demo-down ## Tear down + remove local Terraform state files
	rm -f terraform.tfstate terraform.tfstate.backup
	rm -rf .terraform .terraform.lock.hcl

status: ## Show current Terraform resources
	@terraform state list 2>/dev/null || echo "No state file found"

env: ## Print .env contents for LineageBridge
	@terraform output -raw demo_env_file

help: ## Show this help
	@echo ""
	@echo "  LineageBridge Demo Infrastructure"
	@echo "  ─────────────────────────────────"
	@echo ""
	@echo "  Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{ printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }'
	@echo ""
	@echo "  Quick start:"
	@echo "    cp terraform.tfvars.example terraform.tfvars"
	@echo "    # edit terraform.tfvars with your credentials"
	@echo "    make demo-up"
	@echo ""
