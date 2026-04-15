# LineageBridge Demo: Connector → BigQuery
# ──────────────────────────────────────────────────────────────────────────────

SHELL := /bin/bash

GCP_PROJECT_ID := $(shell grep gcp_project_id terraform.tfvars 2>/dev/null | sed 's/.*= *"\(.*\)"/\1/')

.DEFAULT_GOAL := help

.PHONY: setup demo-up demo-down clean status env help

setup: ## Interactive credential setup (creates terraform.tfvars)
	bash scripts/setup-tfvars.sh

demo-up: ## Provision everything (auto-runs setup if terraform.tfvars missing)
	@if [ ! -f terraform.tfvars ] || grep -q 'YOUR_' terraform.tfvars 2>/dev/null; then \
		echo "  terraform.tfvars missing or has placeholders — running setup..."; \
		echo ""; \
		bash scripts/setup-tfvars.sh; \
	fi
	bash scripts/provision-demo.sh

demo-down: ## Tear down everything (Terraform + GCP service account)
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  LineageBridge BigQuery Demo — Destroying"
	@echo "══════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "▸ Destroying Terraform resources..."
	@if [ -f gcp-sa-key.json ]; then \
		export TF_VAR_gcp_sa_key_json=$$(cat gcp-sa-key.json); \
		terraform destroy -auto-approve; \
	else \
		terraform destroy -auto-approve; \
	fi
	@echo ""
	@echo "▸ Cleaning up GCP service account..."
	@if [ -n "$(GCP_PROJECT_ID)" ]; then \
		GCP_PROJECT_ID="$(GCP_PROJECT_ID)" bash scripts/cleanup-gcp-sa.sh || true; \
	else \
		echo "  Skipped (no GCP project ID found)"; \
	fi
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  BigQuery Demo destroyed."
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
	@echo "  LineageBridge BigQuery Demo (Connector -> BigQuery)"
	@echo "  ──────────────────────────────────────────────────"
	@echo ""
	@echo "  Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{ printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }'
	@echo ""
