.DEFAULT_GOAL := help

.PHONY: install extract watch api openapi ui test lint format clean docker-build docker-extract docker-watch docker-ui docker-down demo-setup demo-up demo-down demo-uc-up demo-uc-down demo-glue-up demo-glue-down demo-bq-up demo-bq-down docs-serve docs-build docs-deploy docs-install help

install: ## Install project with dev dependencies
	@test -x .venv/bin/python || uv venv
	uv pip install -e ".[dev]"

extract: ## Run lineage extraction CLI
	uv run lineage-bridge-extract

watch: ## Run change-detection watcher CLI
	uv run lineage-bridge-watch

api: ## Start the REST API server
	uv run lineage-bridge-api

openapi: ## Export OpenAPI spec to docs/openapi.yaml
	uv run python -c "import yaml; from lineage_bridge.api.app import create_app; \
		app = create_app(); spec = app.openapi(); \
		open('docs/openapi.yaml', 'w').write(yaml.dump(spec, sort_keys=False, allow_unicode=True))"
	@echo "OpenAPI spec written to docs/openapi.yaml"

ui: ## Start the Streamlit UI (auto-provisions Cloud API key if missing)
	@bash scripts/ensure-cloud-key.sh
	uv run streamlit run lineage_bridge/ui/app.py

test: ## Run tests
	uv run pytest tests/ -v

test-integration-uc: ## Run UC demo integration tests (requires provisioned UC demo)
	bash scripts/integration-test-uc.sh

test-integration-uc-skip-docker: ## Run UC integration tests (skip Docker build)
	bash scripts/integration-test-uc.sh --skip-docker

test-integration-glue: ## Run AWS Glue demo integration tests (requires provisioned Glue demo)
	bash scripts/integration-test-glue.sh

test-integration-glue-skip-docker: ## Run Glue integration tests (skip Docker build)
	bash scripts/integration-test-glue.sh --skip-docker

test-integration-bigquery: ## Run BigQuery demo integration tests (requires provisioned BigQuery demo)
	bash scripts/integration-test-bigquery.sh

test-integration-bigquery-skip-docker: ## Run BigQuery integration tests (skip Docker build)
	bash scripts/integration-test-bigquery.sh --skip-docker

test-integration-dataplex: ## Run Dataplex Catalog live-API tests (needs LINEAGE_BRIDGE_GCP_PROJECT_ID + ADC)
	LINEAGE_BRIDGE_GCP_INTEGRATION=1 uv run pytest tests/integration/test_gcp_dataplex_integration.py -v

test-integration-datazone: ## Run AWS DataZone live-API tests (needs domain/project IDs + AWS creds)
	LINEAGE_BRIDGE_AWS_DATAZONE_INTEGRATION=1 uv run pytest tests/integration/test_aws_datazone_integration.py -v

test-integration-all: ## Run all integration tests (UC, Glue, BigQuery)
	@echo "Running all integration tests..."
	@bash scripts/integration-test-uc.sh --skip-docker || true
	@bash scripts/integration-test-glue.sh --skip-docker || true
	@bash scripts/integration-test-bigquery.sh --skip-docker || true

lint: ## Run linter
	uv run ruff check .

format: ## Format code and auto-fix lint issues
	uv run ruff format .
	uv run ruff check --fix .

clean: ## Remove build artifacts and caches
	rm -rf dist build *.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -f lineage_graph.json

# ── Docker ──────────────────────────────────────────────────────────────────

COMPOSE := docker compose -f infra/docker/docker-compose.yml

docker-build: ## Build Docker images
	$(COMPOSE) build

docker-extract: ## Run extraction via Docker
	$(COMPOSE) --profile extract run --rm extract

docker-watch: ## Run change-detection watcher via Docker
	$(COMPOSE) --profile watch up watch

docker-ui: ## Start UI via Docker
	$(COMPOSE) --profile ui up ui

docker-down: ## Stop all Docker services
	$(COMPOSE) --profile extract --profile ui --profile watch down

# ── Demo Infrastructure (per catalog) ─────────────────────────────────────

demo-uc-up: ## Provision UC demo (Tableflow -> Unity Catalog) and start UI
	$(MAKE) -C infra/demos/uc demo-up
	$(MAKE) ui

demo-uc-down: ## Tear down UC demo
	$(MAKE) -C infra/demos/uc demo-down

demo-glue-up: ## Provision Glue demo (Tableflow -> AWS Glue) and start UI
	$(MAKE) -C infra/demos/glue demo-up
	$(MAKE) ui

demo-glue-down: ## Tear down Glue demo
	$(MAKE) -C infra/demos/glue demo-down

demo-bq-up: ## Provision BigQuery demo (Connector -> BigQuery) and start UI
	$(MAKE) -C infra/demos/bigquery demo-up
	$(MAKE) ui

demo-bq-down: ## Tear down BigQuery demo
	$(MAKE) -C infra/demos/bigquery demo-down

# Legacy aliases (point to UC demo as default)
demo-setup: ## (legacy) Interactive credential setup — same as UC demo
	$(MAKE) -C infra/demos/uc setup

demo-up: ## (legacy) Provision demo — same as demo-uc-up
	$(MAKE) demo-uc-up

demo-down: ## (legacy) Tear down demo — same as demo-uc-down
	$(MAKE) demo-uc-down

# ── Documentation ──────────────────────────────────────────────────────────

docs-serve: ## Serve docs locally with live reload
	uv run mkdocs serve --dev-addr localhost:8001

docs-build: ## Build static docs site
	uv run mkdocs build --strict

docs-deploy: ## Deploy docs to GitHub Pages
	uv run mkdocs gh-deploy --force

docs-install: ## Install documentation dependencies
	uv pip install -e ".[docs]"

# ── Help ───────────────────────────────────────────────────────────────────

help: ## Show this help
	@echo ""
	@echo "  LineageBridge"
	@echo "  ────────────"
	@echo ""
	@echo "  Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{ printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 }'
	@echo ""
