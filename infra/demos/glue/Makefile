# LineageBridge Demo: Tableflow → AWS Glue
# ──────────────────────────────────────────────────────────────────────────────

SHELL := /bin/bash

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

demo-down: ## Tear down everything
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  LineageBridge Glue Demo — Destroying"
	@echo "══════════════════════════════════════════════════════════════════"
	@echo ""
	terraform destroy -auto-approve
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Glue Demo destroyed."
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
	@echo "  LineageBridge Glue Demo (Tableflow -> AWS Glue)"
	@echo "  ───────────────────────────────────────────────"
	@echo ""
	@echo "  Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{ printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }'
	@echo ""
