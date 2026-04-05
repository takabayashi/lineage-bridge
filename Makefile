.PHONY: extract ui test lint format clean install docker-build docker-extract docker-ui docker-down demo infra-init infra-plan infra-apply infra-destroy infra-env

install:
	pip install -e ".[dev]"

extract:
	lineage-bridge-extract

ui:
	lineage-bridge-ui

test:
	pytest tests/ -v

lint:
	ruff check .

format:
	ruff format .
	ruff check --fix .

clean:
	rm -rf dist build *.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -f lineage_graph.json

# ── Docker ──────────────────────────────────────────────────────────────────

docker-build:
	docker compose build

docker-extract:
	docker compose --profile extract run --rm extract

docker-ui:
	docker compose --profile ui up ui

docker-down:
	docker compose --profile extract --profile ui down

# ── Demo ────────────────────────────────────────────────────────────────────

demo:
	@echo "Generating sample lineage graph..."
	python -c "from lineage_bridge.models.graph import LineageGraph; LineageGraph.sample().save('lineage_graph.json')" 2>/dev/null \
		|| echo '{"nodes":[],"edges":[]}' > lineage_graph.json
	@echo "Launching Streamlit UI..."
	streamlit run lineage_bridge/ui/app.py

# ── Infrastructure ─────────────────────────────────────────────────────────

infra-init:
	cd infra && terraform init

infra-plan:
	cd infra && terraform plan

infra-apply:
	cd infra && terraform apply

infra-destroy:
	cd infra && terraform destroy

infra-env:
	./scripts/env-from-terraform.sh
