# LineageBridge Demo Infrastructure
# ──────────────────────────────────────────────────────────────────────────────

SHELL := /bin/bash

# Extract variables from terraform.tfvars for cleanup scripts
DATABRICKS_HOST  := $(shell grep databricks_workspace_url terraform.tfvars 2>/dev/null | sed 's/.*= *"\(.*\)"/\1/')
DATABRICKS_TOKEN := $(shell grep databricks_token terraform.tfvars 2>/dev/null | sed 's/.*= *"\(.*\)"/\1/')
CATALOG_NAME     := $(shell terraform output -raw databricks_catalog_name 2>/dev/null || echo "lb_demo")

.PHONY: demo-up demo-down clean-catalog clean status env help

## Provision everything from scratch (init + apply + Tableflow key bootstrap)
demo-up:
	bash scripts/provision-demo.sh

## Tear down everything (catalog cleanup + terraform destroy)
demo-down:
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  LineageBridge Demo — Destroying"
	@echo "══════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "▸ Cleaning up auto-created Databricks schemas..."
	@DATABRICKS_HOST="$(DATABRICKS_HOST)" \
	DATABRICKS_TOKEN="$(DATABRICKS_TOKEN)" \
	CATALOG_NAME="$(CATALOG_NAME)" \
	bash scripts/cleanup-catalog.sh || true
	@echo ""
	@echo "▸ Destroying Terraform resources..."
	terraform destroy -auto-approve
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Demo destroyed."
	@echo "══════════════════════════════════════════════════════════════════"

## Full cleanup: destroy + remove local Terraform state
clean: demo-down
	rm -f terraform.tfstate terraform.tfstate.backup
	rm -rf .terraform .terraform.lock.hcl

## Show current resource status
status:
	@terraform state list 2>/dev/null || echo "No state file found"

## Generate .env file for LineageBridge
env:
	@terraform output -raw demo_env_file

## Help (default target)
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  demo-up    Provision everything (single command)"
	@echo "  demo-down  Tear down everything (single command)"
	@echo "  clean      Tear down + remove local state files"
	@echo "  status     Show current Terraform resources"
	@echo "  env        Print .env contents for LineageBridge"
	@echo "  help       Show this help"
