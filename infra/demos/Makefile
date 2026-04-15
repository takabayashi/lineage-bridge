# LineageBridge Demos — Top-Level Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: demo-uc-up demo-uc-down demo-glue-up demo-glue-down demo-bq-up demo-bq-down help

demo-uc-up: ## Provision UC demo (Tableflow -> Databricks Unity Catalog)
	$(MAKE) -C uc demo-up

demo-uc-down: ## Tear down UC demo
	$(MAKE) -C uc demo-down

demo-glue-up: ## Provision Glue demo (Tableflow -> AWS Glue)
	$(MAKE) -C glue demo-up

demo-glue-down: ## Tear down Glue demo
	$(MAKE) -C glue demo-down

demo-bq-up: ## Provision BigQuery demo (Connector -> BigQuery)
	$(MAKE) -C bigquery demo-up

demo-bq-down: ## Tear down BigQuery demo
	$(MAKE) -C bigquery demo-down

help: ## Show this help
	@echo ""
	@echo "  LineageBridge Demos"
	@echo "  ──────────────────"
	@echo ""
	@echo "  Choose a demo by use case:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{ printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }'
	@echo ""
	@echo "  Or run directly in a demo directory:"
	@echo "    cd uc && make demo-up"
	@echo "    cd glue && make demo-up"
	@echo "    cd bigquery && make demo-up"
	@echo ""
