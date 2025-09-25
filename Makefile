# ANAC Sync Makefile

.PHONY: help install install-dev format lint test clean run

help: ## Show this help message
	@echo "ANAC Sync - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install the package
	pip install -e .

install-dev: ## Install development dependencies
	pip install -e ".[dev]"

format: ## Format code with black and ruff
	black anacsync/ tests/
	ruff check anacsync/ tests/ --fix

lint: ## Run linting checks
	ruff check anacsync/ tests/
	mypy anacsync/

test: ## Run tests
	pytest tests/ -v

test-coverage: ## Run tests with coverage
	pytest tests/ -v --cov=anacsync --cov-report=html --cov-report=term

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf .pytest_cache/

run: ## Run the interactive CLI
	anacsync

run-crawl: ## Run crawl command
	anacsync crawl

run-scan: ## Run scan command
	anacsync scan

run-plan: ## Run plan command
	anacsync plan

run-download: ## Run download command
	anacsync download

run-sort: ## Run sort command
	anacsync sort

run-report: ## Run report command
	anacsync report

check: format lint test ## Run all checks (format, lint, test)

build: ## Build the package
	python -m build

dist: build ## Create distribution packages
	@echo "Distribution packages created in dist/"

all: clean install-dev check ## Clean, install dev deps, and run all checks

