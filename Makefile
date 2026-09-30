.PHONY: help install test qa format build clean

.DEFAULT_GOAL := help

help:  ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

install:  ## Create the venv and install all dependencies
	poetry install

test:  ## Run the unit tests with coverage
	poetry run pytest

qa:  ## Format, lint, and check YAML
	poetry run black .
	poetry run flake8
	poetry run yamllint .

format:  ## Sort imports and format only
	poetry run ruff check --select I --fix .
	poetry run black .

build:  ## Build the wheel into dist/
	poetry build

clean:  ## Remove build artifacts and caches
	rm -rf dist build .pytest_cache .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
