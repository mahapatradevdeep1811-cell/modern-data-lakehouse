.PHONY: help up down restart logs test test-unit test-int lint format typecheck clean scaffold

PYTHON := python
PIP    := pip

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Docker / local stack
# ---------------------------------------------------------------------------

up:  ## Start Airflow + Spark stack
	docker compose up -d --build
	@echo "\n✅  Airflow UI → http://localhost:8080  (admin / admin)"
	@echo "    Spark UI  → http://localhost:8081"

down:  ## Stop the stack
	docker compose down

restart:  ## Restart all services
	docker compose restart

logs:  ## Follow all container logs
	docker compose logs -f

logs-scheduler:  ## Follow Airflow scheduler logs
	docker compose logs -f airflow-scheduler

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

install:  ## Install Python dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

env:  ## Copy .env.example → .env
	cp .env.example .env
	@echo "✅  .env created — fill in your credentials"

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------

test:  ## Run all tests with coverage
	pytest tests/ -v --cov=. --cov-report=term-missing --cov-report=html

test-unit:  ## Run unit tests only
	pytest tests/unit/ -v

test-int:  ## Run integration tests only
	pytest tests/integration/ -v -m integration

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

lint:  ## Lint with ruff
	ruff check .

format:  ## Format with black
	black .

typecheck:  ## Type-check with mypy
	mypy etl/ dq/ utils/ orchestration/

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

run:  ## Run pipeline manually (set SOURCE and TARGET env vars)
	$(PYTHON) -m etl.pipeline \
		--source $(SOURCE) \
		--target $(TARGET) \
		--date $(shell date +%Y-%m-%d)

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

clean:  ## Remove compiled Python files and test artefacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache htmlcov .coverage
