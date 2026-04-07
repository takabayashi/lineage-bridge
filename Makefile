.DEFAULT_GOAL := help

.PHONY: install extract watch ui test lint format clean docker-build docker-extract docker-watch docker-ui docker-down demo-setup demo-up demo-down help

install: ## Install project with dev dependencies
	@test -x .venv/bin/python || uv venv
	uv pip install -e ".[dev]"

extract: ## Run lineage extraction CLI
	uv run lineage-bridge-extract

watch: ## Run change-detection watcher CLI
	uv run lineage-bridge-watch

ui: ## Start the Streamlit UI (auto-provisions Cloud API key if missing)
	@bash scripts/ensure-cloud-key.sh
	uv run streamlit run lineage_bridge/ui/app.py

test: ## Run tests
	uv run pytest tests/ -v

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

# ── Demo Infrastructure ────────────────────────────────────────────────────

demo-setup: ## Interactive credential setup for demo infrastructure
	$(MAKE) -C infra/demo setup

demo-up: ## Provision demo infrastructure (Confluent + AWS + Databricks)
	$(MAKE) -C infra/demo demo-up

demo-down: ## Tear down demo infrastructure
	$(MAKE) -C infra/demo demo-down

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
