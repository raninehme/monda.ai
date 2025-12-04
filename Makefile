SHELL := /bin/bash
.DEFAULT_GOAL := help

# =============================================================================
# Infrastructure
# =============================================================================

up: down ## Build and start MinIO & Prefect (detached), then serve all flows
	docker compose up -d --build --remove-orphans
	-docker image prune -f
	@echo "Waiting for Prefect server to start..."

down: ## Stop and remove all containers
	-docker compose down --remove-orphans

logs: ## View live container logs
	docker compose logs -f

ps: ## Show container status
	docker compose ps

clean: ## Remove containers, networks, and volumes created by docker-compose
	@echo "Cleaning Docker Compose environment..."
	-docker compose down --volumes --remove-orphans
	@echo "Docker environment cleaned."

# =============================================================================
# Prefect
# =============================================================================

serve: ## Serve all Prefect flows (create + trigger) inside the Prefect container
	@echo "Serving Prefect flows (create_pipeline + trigger_pipeline)..."
	docker compose exec prefect bash -c "PYTHONPATH=/app python src/flows/serve_all.py"

# =============================================================================
# Utilities
# =============================================================================

help: ## Display this help message
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?##' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
	@echo ""
