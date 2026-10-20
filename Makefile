.DEFAULT_GOAL := help
VENV ?= .venv
PY := $(VENV)/bin/python

.PHONY: help venv install dev worker test cov lint fmt docker-up docker-down clean

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv: ## Create virtualenv
	test -d $(VENV) || python3 -m venv $(VENV)

install: venv ## Install with dev extras
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e ".[dev]"

dev: ## API + in-process worker with reload on :8000
	$(VENV)/bin/uvicorn webhook_relay.main:app --reload --port 8000

worker: ## Standalone delivery worker
	$(PY) -m webhook_relay.worker

test: ## Run tests
	$(PY) -m pytest

cov: ## Tests with coverage
	$(PY) -m pytest --cov=webhook_relay --cov-report=term-missing

lint: ## Ruff check + format check
	$(VENV)/bin/ruff check webhook_relay tests
	$(VENV)/bin/ruff format --check webhook_relay tests

fmt: ## Format + autofix
	$(VENV)/bin/ruff format webhook_relay tests
	$(VENV)/bin/ruff check --fix webhook_relay tests

docker-up: ## api + worker + postgres
	docker compose up --build -d

docker-down: ## Stop stack
	docker compose down

clean: ## Remove caches and local DB
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage relay.db
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
